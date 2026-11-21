//! The sweep is independent of Parquet. I/O and budget scheduling follow below.
use crate::{
    parquet_io,
    parser::{DAY, WINDOW},
};
use anyhow::{Context, Result, ensure};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int32Array, Int64Array, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use rayon::prelude::*;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Debug)]
pub struct Session {
    pub person: i64,
    pub time: i64,
    pub revenue: i64,
    pub purchase: bool,
}
#[derive(Clone, Debug)]
struct Click {
    person: i64,
    time: i64,
    campaign: i32,
    offset: usize,
    length: usize,
}
#[derive(Clone, Debug)]
struct OwnedClick {
    person: i64,
    time: i64,
    campaign: i32,
    id: Vec<u8>,
}
pub type Metrics = BTreeMap<(i32, i32), [i64; 4]>;
#[derive(Default)]
struct Stats {
    sessions: u64,
    clicks: u64,
    intervals: u64,
    largest_person_sessions: usize,
    largest_bucket_rows: u64,
    deferred: usize,
}
impl Stats {
    fn merge(&mut self, other: Self) {
        self.sessions += other.sessions;
        self.clicks += other.clicks;
        self.intervals += other.intervals;
        self.largest_person_sessions = self
            .largest_person_sessions
            .max(other.largest_person_sessions);
        self.largest_bucket_rows = self.largest_bucket_rows.max(other.largest_bucket_rows);
    }
}
fn add(metrics: &mut Metrics, campaign: i32, day: i32, values: [i64; 4]) -> Result<()> {
    let entry = metrics.entry((campaign, day)).or_default();
    for (old, value) in entry.iter_mut().zip(values) {
        *old = old.checked_add(value).context("campaign metric overflow")?;
    }
    Ok(())
}

struct DayRange {
    day: i32,
    left: usize,
    right: usize,
}
struct Sweep<'a> {
    sessions: &'a [Session],
    index: usize,
    latest: Option<(i64, i32)>,
    winner: Vec<u8>,
    pending: &'a mut [Option<(i64, i64)>],
    touched: Vec<usize>,
    days: Vec<DayRange>,
    day_indices: Vec<usize>,
    metrics: &'a mut Metrics,
    stats: &'a mut Stats,
}
impl<'a> Sweep<'a> {
    fn new(
        sessions: &'a [Session],
        pending: &'a mut [Option<(i64, i64)>],
        metrics: &'a mut Metrics,
        stats: &'a mut Stats,
    ) -> Self {
        let mut days: Vec<DayRange> = vec![];
        let mut day_indices = Vec::with_capacity(sessions.len());
        for (index, session) in sessions.iter().enumerate() {
            let day = session.time.div_euclid(DAY) as i32;
            if days.last().is_none_or(|range| range.day != day) {
                days.push(DayRange {
                    day,
                    left: index,
                    right: index + 1,
                });
            } else {
                days.last_mut().unwrap().right = index + 1;
            }
            day_indices.push(days.len() - 1);
        }
        stats.largest_person_sessions = stats.largest_person_sessions.max(sessions.len());
        Self {
            sessions,
            index: 0,
            latest: None,
            winner: vec![],
            pending,
            touched: vec![],
            days,
            day_indices,
            metrics,
            stats,
        }
    }
    fn assist(&mut self, campaign: i32, lo: i64, hi: i64) -> Result<()> {
        self.stats.intervals += 1;
        let left = self.sessions.partition_point(|s| s.time < lo);
        let right = self.sessions.partition_point(|s| s.time <= hi);
        if left == right {
            return Ok(());
        }
        for range in &self.days[self.day_indices[left]..=self.day_indices[right - 1]] {
            let count = range.right.min(right) - range.left.max(left);
            if count > 0 {
                add(self.metrics, campaign, range.day, [0, 0, 0, count as i64])?;
            }
        }
        Ok(())
    }
    fn credit(&mut self) -> Result<()> {
        let session = &self.sessions[self.index];
        let campaign = self
            .latest
            .filter(|(time, _)| *time >= session.time - WINDOW)
            .map_or(0, |(_, code)| code);
        add(
            self.metrics,
            campaign,
            session.time.div_euclid(DAY) as i32,
            [1, i64::from(session.purchase), session.revenue, 0],
        )?;
        self.stats.sessions += 1;
        self.index += 1;
        Ok(())
    }
    fn consume(&mut self, time: i64, campaign: i32, id: &[u8]) -> Result<bool> {
        while self.index < self.sessions.len() && self.sessions[self.index].time < time {
            self.credit()?;
        }
        if self.index == self.sessions.len() {
            return Ok(false);
        }
        if time < self.sessions[0].time - WINDOW {
            return Ok(true);
        }
        ensure!(
            campaign > 0 && (campaign as usize) < self.pending.len(),
            "invalid campaign code"
        );
        self.stats.clicks += 1;
        if self.latest.is_none_or(|(latest, _)| {
            time > latest || (time == latest && id < self.winner.as_slice())
        }) {
            self.latest = Some((time, campaign));
            self.winner.clear();
            self.winner.extend_from_slice(id);
        }
        let code = campaign as usize;
        match self.pending[code] {
            None => {
                self.touched.push(code);
                self.pending[code] = Some((time, time + WINDOW));
            }
            Some((lo, hi)) if time <= hi => {
                self.pending[code] = Some((lo, hi.max(time + WINDOW)));
            }
            Some((lo, hi)) => {
                self.assist(campaign, lo, hi)?;
                self.pending[code] = Some((time, time + WINDOW));
            }
        }
        Ok(true)
    }
    fn finish(mut self) -> Result<()> {
        while self.index < self.sessions.len() {
            self.credit()?;
        }
        for index in 0..self.touched.len() {
            let code = self.touched[index];
            let (lo, hi) = self.pending[code].take().unwrap();
            self.assist(code as i32, lo, hi)?;
        }
        Ok(())
    }
}

fn memory_used(
    sessions: &Vec<Session>,
    clicks: &Vec<Click>,
    arena: &Vec<u8>,
    decoded: usize,
    campaigns: usize,
) -> u64 {
    (sessions.capacity() * std::mem::size_of::<Session>()
        + clicks.capacity() * std::mem::size_of::<Click>()
        + arena.capacity()
        + decoded
        + campaigns * 128
        + 1024 * 1024) as u64
}
fn next_capacity(current: usize, needed: usize) -> usize {
    if needed <= current {
        current
    } else {
        needed.max(current.saturating_mul(2)).max(4)
    }
}
fn reserve_batch(
    sessions: &mut Vec<Session>,
    clicks: &mut Vec<Click>,
    arena: &mut Vec<u8>,
    extra_sessions: usize,
    extra_clicks: usize,
    extra_ids: usize,
    decoded: usize,
    campaigns: usize,
    budget: u64,
) -> Result<bool> {
    let sc = next_capacity(sessions.capacity(), sessions.len() + extra_sessions);
    let cc = next_capacity(clicks.capacity(), clicks.len() + extra_clicks);
    let ac = next_capacity(arena.capacity(), arena.len() + extra_ids);
    let projected = sc * std::mem::size_of::<Session>()
        + cc * std::mem::size_of::<Click>()
        + ac
        + decoded
        + campaigns * 128
        + 1024 * 1024;
    if projected as u64 > budget {
        return Ok(false);
    }
    sessions.try_reserve_exact(sc - sessions.len())?;
    clicks.try_reserve_exact(cc - clicks.len())?;
    arena.try_reserve_exact(ac - arena.len())?;
    Ok(true)
}
fn batch_col<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .context("missing Parquet column")?
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| format!("unexpected Parquet type for {name}"))
}

fn load_bucket(
    sessions_files: &[PathBuf],
    clicks_files: &[PathBuf],
    budget: u64,
    campaigns: usize,
) -> Result<Option<(Vec<Session>, Vec<Click>, Vec<u8>)>> {
    let (mut sessions, mut clicks, mut arena) = (vec![], vec![], vec![]);
    for path in sessions_files {
        for batch in parquet_io::reader(path, 4096)? {
            let batch = batch?;
            if !reserve_batch(
                &mut sessions,
                &mut clicks,
                &mut arena,
                batch.num_rows(),
                0,
                0,
                batch.get_array_memory_size(),
                campaigns,
                budget,
            )? {
                return Ok(None);
            }
            let keys = batch_col::<Int64Array>(&batch, "person_key")?;
            let times = batch_col::<Int64Array>(&batch, "start_ms")?;
            let revenue = batch_col::<Int64Array>(&batch, "revenue_cents")?;
            let purchase = batch_col::<BooleanArray>(&batch, "has_purchase")?;
            for i in 0..batch.num_rows() {
                sessions.push(Session {
                    person: keys.value(i),
                    time: times.value(i),
                    revenue: revenue.value(i),
                    purchase: purchase.value(i),
                });
            }
            if memory_used(
                &sessions,
                &clicks,
                &arena,
                batch.get_array_memory_size(),
                campaigns,
            ) > budget
            {
                return Ok(None);
            }
        }
    }
    for path in clicks_files {
        for batch in parquet_io::reader(path, 4096)? {
            let batch = batch?;
            let keys = batch_col::<Int64Array>(&batch, "person_key")?;
            let times = batch_col::<Int64Array>(&batch, "click_ms")?;
            let codes = batch_col::<Int32Array>(&batch, "campaign_code")?;
            let ids = batch_col::<BinaryArray>(&batch, "click_id")?;
            if !reserve_batch(
                &mut sessions,
                &mut clicks,
                &mut arena,
                0,
                batch.num_rows(),
                ids.value_data().len(),
                batch.get_array_memory_size(),
                campaigns,
                budget,
            )? {
                return Ok(None);
            }
            for i in 0..batch.num_rows() {
                let id = ids.value(i);
                let offset = arena.len();
                arena.extend_from_slice(id);
                clicks.push(Click {
                    person: keys.value(i),
                    time: times.value(i),
                    campaign: codes.value(i),
                    offset,
                    length: id.len(),
                });
            }
            if memory_used(
                &sessions,
                &clicks,
                &arena,
                batch.get_array_memory_size(),
                campaigns,
            ) > budget
            {
                return Ok(None);
            }
        }
    }
    Ok(Some((sessions, clicks, arena)))
}
fn in_memory(
    mut sessions: Vec<Session>,
    mut clicks: Vec<Click>,
    arena: Vec<u8>,
    campaigns: usize,
    budget: u64,
) -> Result<Option<(Metrics, Stats)>> {
    sessions.sort_unstable_by_key(|s| (s.person, s.time));
    clicks.sort_unstable_by_key(|c| (c.person, c.time));
    let mut metrics = Metrics::new();
    let mut stats = Stats::default();
    stats.largest_bucket_rows = (sessions.len() + clicks.len()) as u64;
    let mut pending = vec![None; campaigns];
    let mut start = 0;
    let mut click_index = 0;
    let base = memory_used(&sessions, &clicks, &arena, 0, campaigns);
    let largest_id = clicks.iter().map(|c| c.length).max().unwrap_or(0) as u64;
    while start < sessions.len() {
        let person = sessions[start].person;
        let end = start + sessions[start..].partition_point(|s| s.person == person);
        let days = 1 + sessions[start..end]
            .windows(2)
            .filter(|s| s[0].time.div_euclid(DAY) != s[1].time.div_euclid(DAY))
            .count();
        // Reserve space for this person's day indexes, pending intervals, tie
        // ID, and worst-case new campaign/day counters before sweeping it.
        let state = (end - start) as u64 * 40
            + largest_id
            + (metrics.len() as u64 + days as u64 * campaigns as u64) * 128;
        if base.saturating_add(state) > budget {
            return Ok(None);
        }
        while click_index < clicks.len() && clicks[click_index].person < person {
            click_index += 1;
        }
        let mut sweep = Sweep::new(
            &sessions[start..end],
            &mut pending,
            &mut metrics,
            &mut stats,
        );
        while click_index < clicks.len() && clicks[click_index].person == person {
            let click = &clicks[click_index];
            sweep.consume(
                click.time,
                click.campaign,
                &arena[click.offset..click.offset + click.length],
            )?;
            click_index += 1;
        }
        sweep.finish()?;
        start = end;
    }
    Ok(Some((metrics, stats)))
}

// Sorted fallback decodes bounded batches and retains only the current person's
// sessions plus a single click cursor. No whole-bucket vectors or ID arena.
struct Rows {
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    index: usize,
}
impl Rows {
    fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: parquet_io::reader(path, 512)?,
            batch: None,
            index: 0,
        })
    }
    fn row(&mut self) -> Result<Option<(&RecordBatch, usize)>> {
        if self
            .batch
            .as_ref()
            .is_none_or(|batch| self.index == batch.num_rows())
        {
            self.batch = self.reader.next().transpose()?;
            self.index = 0;
        }
        match &self.batch {
            Some(batch) => {
                let index = self.index;
                self.index += 1;
                Ok(Some((batch, index)))
            }
            None => Ok(None),
        }
    }
    fn session(&mut self) -> Result<Option<Session>> {
        let Some((batch, i)) = self.row()? else {
            return Ok(None);
        };
        Ok(Some(Session {
            person: batch_col::<Int64Array>(batch, "person_key")?.value(i),
            time: batch_col::<Int64Array>(batch, "start_ms")?.value(i),
            revenue: batch_col::<Int64Array>(batch, "revenue_cents")?.value(i),
            purchase: batch_col::<BooleanArray>(batch, "has_purchase")?.value(i),
        }))
    }
    fn click(&mut self) -> Result<Option<OwnedClick>> {
        let Some((batch, i)) = self.row()? else {
            return Ok(None);
        };
        Ok(Some(OwnedClick {
            person: batch_col::<Int64Array>(batch, "person_key")?.value(i),
            time: batch_col::<Int64Array>(batch, "click_ms")?.value(i),
            campaign: batch_col::<Int32Array>(batch, "campaign_code")?.value(i),
            id: batch_col::<BinaryArray>(batch, "click_id")?
                .value(i)
                .to_vec(),
        }))
    }
}
fn streaming(input: &Path, bucket: usize, campaigns: usize) -> Result<(Metrics, Stats)> {
    let directory = input.join(format!("bucket={bucket}"));
    let mut sr = Rows::new(&directory.join("sessions.parquet"))?;
    let mut cr = Rows::new(&directory.join("clicks.parquet"))?;
    let (mut next_session, mut next_click) = (sr.session()?, cr.click()?);
    let mut metrics = Metrics::new();
    let mut stats = Stats::default();
    let mut pending = vec![None; campaigns];
    while let Some(first) = next_session.take() {
        let person = first.person;
        let mut sessions = vec![first];
        loop {
            next_session = sr.session()?;
            match next_session.as_ref() {
                Some(s) if s.person == person => sessions.push(next_session.take().unwrap()),
                _ => break,
            }
        }
        while next_click.as_ref().is_some_and(|c| c.person < person) {
            next_click = cr.click()?;
        }
        let mut sweep = Sweep::new(&sessions, &mut pending, &mut metrics, &mut stats);
        while let Some(click) = next_click.as_ref().filter(|c| c.person == person) {
            sweep.consume(click.time, click.campaign, &click.id)?;
            next_click = cr.click()?;
        }
        sweep.finish()?;
    }
    Ok((metrics, stats))
}
fn write_metrics(out: &Path, bucket: usize, metrics: Metrics) -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("campaign_code", DataType::Int32, false),
        Field::new("epoch_day", DataType::Int32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]);
    let mut codes = vec![];
    let mut days = vec![];
    let mut values = [vec![], vec![], vec![], vec![]];
    for ((code, day), counts) in metrics {
        codes.push(code);
        days.push(day);
        for i in 0..4 {
            values[i].push(counts[i]);
        }
    }
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(codes)),
        Arc::new(Int32Array::from(days)),
    ];
    for values in values {
        arrays.push(Arc::new(Int64Array::from(values)));
    }
    let temporary = out.join(format!(".bucket-{bucket}.parquet"));
    parquet_io::write(&temporary, schema, arrays)?;
    std::fs::rename(temporary, out.join(format!("bucket-{bucket}.parquet")))?;
    Ok(())
}

pub fn run(
    input: &Path,
    out: &Path,
    threads: usize,
    budget: u64,
    sorted: Option<usize>,
) -> Result<()> {
    std::fs::create_dir_all(out)?;
    let mut campaigns = 0;
    for batch in parquet_io::reader(&input.join("campaigns.parquet"), 1024)? {
        campaigns += batch?.num_rows();
    }
    ensure!(campaigns > 0, "campaign dimension requires organic");
    if let Some(bucket) = sorted {
        let (metrics, _) = streaming(input, bucket, campaigns)?;
        return write_metrics(out, bucket, metrics);
    }
    let mut jobs = vec![];
    if input.join("sessions").exists() {
        for entry in std::fs::read_dir(input.join("sessions"))? {
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy();
            let Some(number) = name.strip_prefix("bucket=") else {
                continue;
            };
            let bucket = number.parse::<usize>()?;
            let sessions = parquet_io::files(&path)?;
            let clicks = parquet_io::files(&input.join("clicks").join(format!("bucket={bucket}")))?;
            let (sn, se) = parquet_io::estimate(&sessions, std::mem::size_of::<Session>() as u64)?;
            let (cn, ce) = parquet_io::estimate(&clicks, std::mem::size_of::<Click>() as u64)?;
            let estimate = se
                .saturating_add(ce)
                .saturating_add(16 * 1024 * 1024)
                .saturating_add((campaigns * 128 * 32) as u64);
            jobs.push((estimate, bucket, sessions, clicks, sn + cn));
        }
    }
    jobs.sort_by_key(|j| std::cmp::Reverse(j.0));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()?;
    let mut deferred = vec![];
    let mut stats = Stats::default();
    while !jobs.is_empty() {
        let mut wave = vec![];
        let mut remaining = budget;
        let mut later = vec![];
        for job in jobs {
            if job.0 > budget {
                deferred.push(job.1);
            } else if wave.len() < threads && job.0 <= remaining {
                remaining -= job.0;
                wave.push(job);
            } else {
                later.push(job);
            }
        }
        jobs = later;
        let results = pool.install(|| {
            wave.into_par_iter()
                .map(|(reservation, bucket, sessions, clicks, _)| {
                    let Some((sessions, clicks, arena)) =
                        load_bucket(&sessions, &clicks, reservation, campaigns)?
                    else {
                        return Ok((bucket, None));
                    };
                    let Some((metrics, stat)) =
                        in_memory(sessions, clicks, arena, campaigns, reservation)?
                    else {
                        return Ok((bucket, None));
                    };
                    write_metrics(out, bucket, metrics)?;
                    Ok::<_, anyhow::Error>((bucket, Some(stat)))
                })
                .collect::<Result<Vec<_>>>()
        })?;
        for (bucket, result) in results {
            match result {
                Some(result) => stats.merge(result),
                None => deferred.push(bucket),
            }
        }
    }
    deferred.sort_unstable();
    stats.deferred = deferred.len();
    if !std::fs::read_dir(out)?
        .any(|e| e.is_ok_and(|e| e.file_name().to_string_lossy().starts_with("bucket-")))
        && deferred.is_empty()
    {
        write_metrics(out, 0, Metrics::new())?;
    }
    std::fs::write(
        out.join("deferred_buckets.json"),
        serde_json::to_vec(&deferred)?,
    )?;
    if std::env::var_os("CHECKPOINTS_FILE").is_some() {
        std::fs::write(
            out.join("diagnostics.json"),
            serde_json::to_vec_pretty(
                &serde_json::json!({"sessions":stats.sessions,"consumed_clicks":stats.clicks,
            "merged_intervals":stats.intervals,"largest_person_sessions":stats.largest_person_sessions,
            "largest_bucket_rows":stats.largest_bucket_rows,"deferred_buckets":stats.deferred}),
            )?,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn brute(sessions: &[Session], clicks: &[OwnedClick]) -> Metrics {
        let mut metrics = Metrics::new();
        for s in sessions {
            let eligible = clicks
                .iter()
                .filter(|c| c.person == s.person && c.time <= s.time && c.time >= s.time - WINDOW)
                .collect::<Vec<_>>();
            let winner = eligible
                .iter()
                .min_by(|a, b| b.time.cmp(&a.time).then(a.id.cmp(&b.id)))
                .map_or(0, |c| c.campaign);
            let day = s.time.div_euclid(DAY) as i32;
            add(
                &mut metrics,
                winner,
                day,
                [1, i64::from(s.purchase), s.revenue, 0],
            )
            .unwrap();
            for campaign in eligible
                .iter()
                .map(|c| c.campaign)
                .collect::<std::collections::BTreeSet<_>>()
            {
                add(&mut metrics, campaign, day, [0, 0, 0, 1]).unwrap();
            }
        }
        metrics
    }
    #[test]
    fn seeded_timelines_match_brute_force() {
        let mut state = 12345678u64;
        let mut random = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state
        };
        for _ in 0..150 {
            let mut sessions = vec![];
            let mut clicks = vec![];
            for person in 0..5 {
                for _ in 0..20 {
                    let time = (random() % (20 * DAY as u64)) as i64;
                    if !sessions
                        .iter()
                        .any(|s: &Session| s.person == person && s.time == time)
                    {
                        let purchase = random() % 4 == 0;
                        sessions.push(Session {
                            person,
                            time,
                            revenue: if purchase { 100 } else { 0 },
                            purchase,
                        });
                    }
                }
                for i in 0..80 {
                    clicks.push(OwnedClick {
                        person,
                        time: (random() % (28 * DAY as u64)) as i64 - WINDOW,
                        campaign: (random() % 4 + 1) as i32,
                        id: format!("k{i}").into_bytes(),
                    });
                }
            }
            for s in sessions.iter().take(12) {
                for delta in [-WINDOW - 1, -WINDOW, 0, 1] {
                    clicks.push(OwnedClick {
                        person: s.person,
                        time: s.time + delta,
                        campaign: 1,
                        id: b" a\r".to_vec(),
                    });
                    clicks.push(OwnedClick {
                        person: s.person,
                        time: s.time + delta,
                        campaign: 2,
                        id: b"z".to_vec(),
                    });
                }
            }
            clicks.push(clicks[0].clone());
            let mut arena = vec![];
            let compact = clicks
                .iter()
                .map(|c| {
                    let offset = arena.len();
                    arena.extend_from_slice(&c.id);
                    Click {
                        person: c.person,
                        time: c.time,
                        campaign: c.campaign,
                        offset,
                        length: c.id.len(),
                    }
                })
                .collect();
            let actual = in_memory(sessions.clone(), compact, arena, 5, u64::MAX)
                .unwrap()
                .unwrap()
                .0;
            assert_eq!(actual, brute(&sessions, &clicks));
        }
    }
    #[test]
    fn organic_and_sparse_days() {
        let sessions = vec![
            Session {
                person: 1,
                time: -1,
                revenue: 0,
                purchase: false,
            },
            Session {
                person: 1,
                time: 10 * DAY,
                revenue: 500,
                purchase: true,
            },
        ];
        let actual = in_memory(sessions.clone(), vec![], vec![], 1, u64::MAX)
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(actual, brute(&sessions, &[]));
    }
}
