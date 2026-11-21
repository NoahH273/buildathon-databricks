use crate::{
    parquet_io::{self, BATCH_BYTES, BATCH_ROWS},
    parser::{DAY, WINDOW, decimal, scan, timestamp},
};
use anyhow::{Context, Result, ensure};
use arrow_array::{
    ArrayRef, Int32Array, RecordBatch, StringArray,
    builder::{BinaryBuilder, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema};
use rayon::prelude::*;
use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
};

type Codes = HashMap<Vec<u8>, i32>;

fn csv_files(path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = vec![];
    if path.exists() {
        for entry in std::fs::read_dir(path)? {
            let path = entry?.path();
            if path.is_file() && path.extension().is_some_and(|s| s == "csv") {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}
fn text(bytes: &[u8]) -> Result<&str> {
    Ok(std::str::from_utf8(bytes)?)
}

#[derive(Default)]
struct UnionFind {
    ids: HashMap<Vec<u8>, usize>,
    values: Vec<Vec<u8>>,
    parent: Vec<usize>,
    size: Vec<usize>,
    minimum: Vec<usize>,
}
impl UnionFind {
    fn intern(&mut self, id: &[u8]) -> usize {
        if let Some(&node) = self.ids.get(id) {
            return node;
        }
        let node = self.parent.len();
        self.ids.insert(id.to_vec(), node);
        self.values.push(id.to_vec());
        self.parent.push(node);
        self.size.push(1);
        self.minimum.push(node);
        node
    }
    fn find(&mut self, mut node: usize) -> usize {
        while self.parent[node] != node {
            self.parent[node] = self.parent[self.parent[node]];
            node = self.parent[node];
        }
        node
    }
    fn join(&mut self, a: &[u8], b: &[u8]) {
        let a = self.intern(a);
        let b = self.intern(b);
        let mut a = self.find(a);
        let mut b = self.find(b);
        if a == b {
            return;
        }
        if self.size[a] < self.size[b] {
            std::mem::swap(&mut a, &mut b);
        }
        self.parent[b] = a;
        self.size[a] += self.size[b];
        if self.values[self.minimum[b]] < self.values[self.minimum[a]] {
            self.minimum[a] = self.minimum[b];
        }
    }
    fn freeze(mut self) -> Result<HashMap<Vec<u8>, Arc<str>>> {
        let mut names = HashMap::<usize, Arc<str>>::new();
        let mut result = HashMap::with_capacity(self.parent.len());
        for index in 0..self.parent.len() {
            let root = self.find(index);
            let name = names
                .entry(root)
                .or_insert(Arc::from(text(&self.values[self.minimum[root]])?));
            result.insert(self.values[index].clone(), name.clone());
        }
        Ok(result)
    }
}

struct Lookups {
    people: HashMap<Vec<u8>, Arc<str>>,
    products: Codes,
    campaigns: Codes,
    unknown: i32,
    start: i64,
    end: i64,
}
impl Lookups {
    fn person<'a>(&'a self, value: &'a [u8]) -> Result<&'a str> {
        match self.people.get(value) {
            Some(name) => Ok(name),
            None => text(value),
        }
    }
}

fn event_schema() -> Schema {
    Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("ingest_ms", DataType::Int64, false),
        Field::new("is_valid", DataType::Boolean, false),
        Field::new("person_id", DataType::Utf8, true),
        Field::new("event_ms", DataType::Int64, true),
        Field::new("purchase_cents", DataType::Int64, true),
        Field::new("category_code", DataType::Int32, true),
    ])
}
fn click_schema() -> Schema {
    Schema::new(vec![
        Field::new("person_id", DataType::Utf8, false),
        Field::new("click_ms", DataType::Int64, false),
        Field::new("campaign_code", DataType::Int32, false),
        Field::new("click_id", DataType::Binary, false),
    ])
}

fn events(path: &Path, out: &Path, lookup: &Lookups) -> Result<u64> {
    let schema = Arc::new(event_schema());
    let mut writer = parquet_io::writer(out, schema.clone())?;
    let (mut ids, mut ingests, mut valid, mut people, mut times, mut cents, mut categories) = (
        StringBuilder::new(),
        Int64Builder::new(),
        BooleanBuilder::new(),
        StringBuilder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
        Int32Builder::new(),
    );
    let (mut count, mut bytes, mut rows) = (0u64, 0usize, 0usize);
    macro_rules! flush {
        () => {{
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(ids.finish()),
                Arc::new(ingests.finish()),
                Arc::new(valid.finish()),
                Arc::new(people.finish()),
                Arc::new(times.finish()),
                Arc::new(cents.finish()),
                Arc::new(categories.finish()),
            ];
            writer.write(&RecordBatch::try_new(schema.clone(), arrays)?)?;
            // A byte-limited batch can have fewer than the row-group limit.
            // Flush it now so the writer cannot accumulate many such batches.
            writer.flush()?;
        }};
    }
    scan::<7>(path, |f| {
        if f[0].is_empty() {
            return Ok(());
        }
        let ingest = timestamp(f[3]).context("ingest_ts must be valid F1 per input contract")?;
        let event = timestamp(f[2]);
        let kind_ok = matches!(f[4], b"view" | b"cart" | b"purchase");
        let price = if f[4] == b"purchase" {
            decimal(f[6]).filter(|&v| (1..=10_000_000).contains(&v))
        } else {
            Some(0)
        };
        let is_valid = !f[1].is_empty()
            && !f[5].is_empty()
            && kind_ok
            && price.is_some()
            && event.is_some_and(|t| t >= lookup.start && t < lookup.end && ingest - t <= DAY);
        ids.append_value(text(f[0])?);
        ingests.append_value(ingest);
        valid.append_value(is_valid);
        if is_valid {
            let person = lookup.person(f[1])?;
            people.append_value(person);
            times.append_value(event.unwrap());
            cents.append_value(price.unwrap());
            if f[4] == b"purchase" {
                categories.append_value(*lookup.products.get(f[5]).unwrap_or(&lookup.unknown));
            } else {
                categories.append_null();
            }
            bytes += person.len();
        } else {
            people.append_null();
            times.append_null();
            cents.append_null();
            categories.append_null();
        }
        bytes += f[0].len() + 40;
        count += 1;
        rows += 1;
        if rows >= BATCH_ROWS || bytes >= BATCH_BYTES {
            flush!();
            bytes = 0;
            rows = 0;
        }
        Ok(())
    })?;
    if rows > 0 {
        flush!();
    }
    writer.close()?;
    Ok(count)
}

fn clicks(path: &Path, out: &Path, lookup: &Lookups) -> Result<u64> {
    let schema = Arc::new(click_schema());
    let mut writer = parquet_io::writer(out, schema.clone())?;
    let (mut people, mut times, mut campaigns, mut ids) = (
        StringBuilder::new(),
        Int64Builder::new(),
        Int32Builder::new(),
        BinaryBuilder::new(),
    );
    let (mut count, mut bytes, mut rows) = (0u64, 0usize, 0usize);
    macro_rules! flush {
        () => {{
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(people.finish()),
                Arc::new(times.finish()),
                Arc::new(campaigns.finish()),
                Arc::new(ids.finish()),
            ];
            writer.write(&RecordBatch::try_new(schema.clone(), arrays)?)?;
            writer.flush()?;
        }};
    }
    scan::<4>(path, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return Ok(());
        }
        let Some(&campaign) = lookup.campaigns.get(f[2]) else {
            return Ok(());
        };
        let Some(time) = timestamp(f[3]).filter(|&t| t >= lookup.start - WINDOW && t < lookup.end)
        else {
            return Ok(());
        };
        let person = lookup.person(f[1])?;
        people.append_value(person);
        times.append_value(time);
        campaigns.append_value(campaign);
        ids.append_value(f[0]);
        bytes += person.len() + f[0].len() + 24;
        rows += 1;
        count += 1;
        if rows >= BATCH_ROWS || bytes >= BATCH_BYTES {
            flush!();
            bytes = 0;
            rows = 0;
        }
        Ok(())
    })?;
    if rows > 0 {
        flush!();
    }
    writer.close()?;
    Ok(count)
}

pub fn run(data: &Path, out: &Path, threads: usize) -> Result<()> {
    let profiling = std::env::var_os("CHECKPOINTS_FILE").is_some()
        && std::env::var("CHECKPOINTS_ENABLED").as_deref() != Ok("0");
    let started = profiling.then(std::time::Instant::now);
    std::fs::create_dir_all(out)?;
    let manifest: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(data.join("manifest.json"))?)?;
    let start = timestamp(
        manifest["window_start"]
            .as_str()
            .context("window_start")?
            .as_bytes(),
    )
    .context("bad window_start")?;
    let end = timestamp(
        manifest["window_end"]
            .as_str()
            .context("window_end")?
            .as_bytes(),
    )
    .context("bad window_end")?;
    ensure!(
        end > start && start.checked_sub(WINDOW).is_some(),
        "invalid manifest window"
    );
    let mut raw_products = HashMap::new();
    let mut labels = BTreeSet::from(["unknown".to_owned()]);
    scan::<2>(&data.join("products.csv"), |f| {
        let label = text(f[1])?.to_owned();
        labels.insert(label.clone());
        raw_products.insert(f[0].to_vec(), label);
        Ok(())
    })?;
    let labels = labels.into_iter().collect::<Vec<_>>();
    let category_codes = labels
        .iter()
        .enumerate()
        .map(|(i, v)| (v.clone(), i as i32))
        .collect::<HashMap<_, _>>();
    let products = raw_products
        .into_iter()
        .map(|(key, label)| (key, category_codes[&label]))
        .collect();
    parquet_io::write(
        &out.join("categories.parquet"),
        Schema::new(vec![
            Field::new("category_code", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]),
        vec![
            Arc::new(Int32Array::from_iter_values(0..labels.len() as i32)),
            Arc::new(StringArray::from(labels.clone())),
        ],
    )?;
    let mut raw_campaigns = std::collections::BTreeMap::new();
    scan::<2>(&data.join("campaigns.csv"), |f| {
        raw_campaigns.insert(text(f[0])?.to_owned(), text(f[1])?.to_owned());
        Ok(())
    })?;
    let mut ids = vec!["organic".to_owned()];
    let mut channels = vec!["organic".to_owned()];
    let mut campaigns = HashMap::new();
    for (id, channel) in raw_campaigns {
        campaigns.insert(id.as_bytes().to_vec(), ids.len() as i32);
        ids.push(id);
        channels.push(channel);
    }
    parquet_io::write(
        &out.join("campaigns.parquet"),
        Schema::new(vec![
            Field::new("campaign_code", DataType::Int32, false),
            Field::new("campaign_id", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
        ]),
        vec![
            Arc::new(Int32Array::from_iter_values(0..ids.len() as i32)),
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(channels)),
        ],
    )?;
    let dimensions_seconds = started.map(|t| t.elapsed().as_secs_f64());
    let mut union = UnionFind::default();
    for path in csv_files(&data.join("links"))? {
        scan::<2>(&path, |f| {
            if !f[0].is_empty() && !f[1].is_empty() {
                union.join(f[0], f[1]);
            }
            Ok(())
        })?;
    }
    let lookup = Lookups {
        people: union.freeze()?,
        products,
        campaigns,
        unknown: category_codes["unknown"],
        start,
        end,
    };
    let linked_seconds = started.map(|t| t.elapsed().as_secs_f64() - dimensions_seconds.unwrap());
    let mut jobs = vec![];
    for (folder, events) in [("events", true), ("clicks", false)] {
        let mut paths = csv_files(&data.join(folder))?;
        if paths.is_empty() {
            let schema = Arc::new(if events {
                event_schema()
            } else {
                click_schema()
            });
            parquet_io::writer(
                &out.join(if events {
                    "event_candidates/empty.parquet"
                } else {
                    "click_candidates/empty.parquet"
                }),
                schema,
            )?
            .close()?;
        }
        for (i, path) in paths.drain(..).enumerate() {
            let size = path.metadata()?.len();
            jobs.push((size, path, events, i));
        }
    }
    jobs.sort_by_key(|job| std::cmp::Reverse(job.0));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()?;
    // All workers take the next largest file from the same queue, rather than
    // starting at independently stolen slices of a parallel iterator.
    let next = std::sync::atomic::AtomicUsize::new(0);
    let counts = pool
        .install(|| {
            (0..threads)
                .into_par_iter()
                .map(|_| {
                    let mut counts = vec![];
                    loop {
                        let job = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some((_, path, is_event, index)) = jobs.get(job) else {
                            break;
                        };
                        let folder = if *is_event {
                            "event_candidates"
                        } else {
                            "click_candidates"
                        };
                        let output = out.join(folder).join(format!("part-{index}.parquet"));
                        let count = if *is_event {
                            events(path, &output, &lookup)
                        } else {
                            clicks(path, &output, &lookup)
                        }?;
                        counts.push((*is_event, count));
                    }
                    Ok::<_, anyhow::Error>(counts)
                })
                .collect::<Result<Vec<_>>>()
        })?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if profiling {
        let events: u64 = counts
            .iter()
            .filter(|(event, _)| *event)
            .map(|(_, n)| n)
            .sum();
        let clicks: u64 = counts
            .iter()
            .filter(|(event, _)| !*event)
            .map(|(_, n)| n)
            .sum();
        std::fs::write(
            out.join("diagnostics.json"),
            serde_json::to_vec_pretty(
                &serde_json::json!({"event_candidates":events,"click_candidates":clicks,"linked_ids":lookup.people.len(),
                    "dimensions_seconds":dimensions_seconds,"linked_ids_seconds":linked_seconds,
                    "parse_write_seconds":started.unwrap().elapsed().as_secs_f64()-dimensions_seconds.unwrap()-linked_seconds.unwrap()}),
            )?,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arbitrary_chain_order_preserves_unused_minimum() {
        let mut dsu = UnionFind::default();
        for n in (1..1000).rev() {
            dsu.join(
                format!("u{n:04}").as_bytes(),
                format!("u{:04}", n - 1).as_bytes(),
            );
        }
        dsu.join(b" a", b"u0500");
        dsu.join(b"u0999", b"u0999");
        let people = dsu.freeze().unwrap();
        assert_eq!(&*people[b"u0999".as_slice()], " a");
        assert_eq!(people.len(), 1001);
    }
}
