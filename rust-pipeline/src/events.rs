use crate::{
    csv,
    ids::{Id, People},
    profile::{Breakdown, Step},
    timestamp,
};
use ahash::{AHashMap, RandomState};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    fs, io,
    path::Path,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
};

pub struct Event {
    pub person: Id,
    pub ts: i64,
    pub cents: i64,
    pub category: Id,
}

pub struct Catalog<'a> {
    pub products: &'a HashMap<Box<str>, Id>,
    pub unknown: Id,
    pub start: i64,
    pub end: i64,
}

// Every well-formed spelling of an ID selects the same bit. Malformed rows
// may mark extra bits, but can never hide a repeated ID.
fn raw_id(line: &[u8]) -> &[u8] {
    let id = &line[..memchr::memchr(b',', line).unwrap_or(line.len())];
    id.strip_prefix(b"\"")
        .and_then(|s| s.strip_suffix(b"\""))
        .unwrap_or(id)
}

fn bit_position(hash: &RandomState, words: usize, id: &[u8]) -> (usize, u64) {
    let h = hash.hash_one(id);
    ((h as usize >> 6) & (words - 1), 1 << (h & 63))
}

fn repeated_bits(chunks: &[&[u8]], bytes: usize, hash: &RandomState) -> Vec<u64> {
    // Size for locality from input bytes. Saturation only increases exact-map work.
    let words = (bytes / 256).clamp(1, 1 << 23).next_power_of_two();
    let zeros = || (0..words).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
    let seen = zeros();
    let repeated = zeros();
    chunks.par_iter().for_each(|chunk| {
        for line in csv::lines(chunk) {
            let (word, mask) = bit_position(hash, words, raw_id(line));
            if seen[word].load(Relaxed) & mask != 0
                || seen[word].fetch_or(mask, Relaxed) & mask != 0
            {
                if repeated[word].load(Relaxed) & mask == 0 {
                    repeated[word].fetch_or(mask, Relaxed);
                }
            }
        }
    });
    // The parallel pass has joined. Readers need no atomics.
    repeated.into_iter().map(AtomicU64::into_inner).collect()
}

#[derive(Default)]
struct Batch<'a> {
    events: Vec<Event>,
    pending: Vec<&'a [u8]>,
    singletons: Vec<&'a str>,
    profile: Breakdown,
}

// Bind the user's exact bytes only after all row-local semantic checks pass.
fn project<'a, const DETAIL: bool>(
    f: &[&'a str; 7],
    ingest: i64,
    catalog: &Catalog<'_>,
    profile: &mut Breakdown,
    resolve: impl FnOnce(&'a str) -> Id,
) -> Option<Event> {
    if profile.measure::<DETAIL, _>(Step::RequiredIds, || f[1].is_empty() || f[5].is_empty()) {
        return None;
    }
    let ts = profile.measure::<DETAIL, _>(Step::EventTimestampWindow, || {
        timestamp::in_window(f[2], catalog.start, catalog.end)
    })?;
    if profile.measure::<DETAIL, _>(Step::Lateness, || ingest - ts > timestamp::DAY) {
        return None;
    }
    let cents = profile.measure::<DETAIL, _>(Step::TypePurchasePrice, || match f[4] {
        "view" | "cart" => Some(0),
        "purchase" => timestamp::decimal(f[6]).filter(|n| (1..=10_000_000).contains(n)),
        _ => None,
    })?;
    let category = if cents > 0 {
        profile.measure::<DETAIL, _>(Step::ProductCategoryLookup, || {
            catalog
                .products
                .get(f[5])
                .copied()
                .unwrap_or(catalog.unknown)
        })
    } else {
        catalog.unknown
    };
    let person = profile.measure::<DETAIL, _>(Step::IdentityResolution, || resolve(f[1]));
    Some(Event {
        person,
        ts,
        cents,
        category,
    })
}

fn ingest<const DETAIL: bool>(f: &[&str; 7], profile: &mut Breakdown) -> i64 {
    profile.measure::<DETAIL, _>(Step::IngestTimestamp, || {
        timestamp::parse(f[3]).expect("invalid guaranteed ingest_ts")
    })
}

pub fn read<const DETAIL: bool>(
    data: &Path,
    people: &mut People,
    catalog: &Catalog<'_>,
    timer: &mut crate::timing::Timer,
) -> crate::Result<Vec<Event>> {
    let paths = csv::files(&data.join("events"))?;
    let files = paths
        .par_iter()
        .map(fs::read)
        .collect::<io::Result<Vec<_>>>()?;
    let chunks = csv::chunks(&files);
    let hash = RandomState::new();
    let repeated = repeated_bits(&chunks, files.iter().map(Vec::len).sum(), &hash);
    timer.mark("events_read_repeat_filter")?;

    let canonical = people.canonical_ids();
    let mut batches: Vec<_> = chunks
        .par_iter()
        .map(|chunk| {
            let mut batch = Batch::default();
            let mut local = AHashMap::new();
            for line in csv::lines(chunk) {
                let id = raw_id(line);
                let (word, mask) = bit_position(&hash, repeated.len(), id);
                if repeated[word] & mask != 0 {
                    batch.pending.push(line);
                    continue;
                }
                let Some(f) = batch
                    .profile
                    .measure::<DETAIL, _>(Step::CsvStructureUtf8, || csv::parse(line))
                else {
                    continue;
                };
                if f[0].is_empty() {
                    continue;
                }
                let ingest = ingest::<DETAIL>(&f, &mut batch.profile);
                if let Some(event) =
                    project::<DETAIL>(&f, ingest, catalog, &mut batch.profile, |user| {
                        if let Some(id) = people.names.lookup(user) {
                            return canonical[id];
                        }
                        *local.entry(user).or_insert_with(|| {
                            let id = canonical.len() + batch.singletons.len();
                            batch.singletons.push(user);
                            id
                        })
                    })
                {
                    batch.events.push(event);
                }
            }
            batch
        })
        .collect();
    let mut detail = Breakdown::default();
    if DETAIL {
        for batch in &mut batches {
            detail.merge(std::mem::take(&mut batch.profile));
        }
    }
    timer.mark_detailed("events_project_unique", DETAIL.then_some(&detail))?;

    // Only this pass mutates the shared name table. Convert each chunk's new
    // singleton names once, including names discovered in another chunk.
    let translations: Vec<Vec<Id>> = batches
        .iter()
        .map(|batch| {
            batch
                .singletons
                .iter()
                .map(|name| people.person(name))
                .collect()
        })
        .collect();
    batches
        .par_iter_mut()
        .zip(translations)
        .for_each(|(batch, ids)| {
            for event in &mut batch.events {
                if event.person >= canonical.len() {
                    event.person = ids[event.person - canonical.len()];
                }
            }
        });
    let mut events = Vec::with_capacity(
        batches
            .iter()
            .map(|b| b.events.len() + b.pending.len())
            .sum(),
    );
    let mut winners = AHashMap::<&str, (i64, Option<Event>)>::new();
    let mut detail = Breakdown::default();
    for mut batch in batches {
        events.append(&mut batch.events);
        for line in batch.pending {
            let Some(f) = detail.measure::<DETAIL, _>(Step::CsvStructureUtf8, || csv::parse(line))
            else {
                continue;
            };
            if f[0].is_empty() {
                continue;
            }
            let ingest = ingest::<DETAIL>(&f, &mut detail);
            let winner = detail.measure::<DETAIL, _>(Step::DedupLookup, || {
                winners.entry(f[0]).or_insert((i64::MAX, None))
            });
            if ingest < winner.0 {
                let event =
                    project::<DETAIL>(&f, ingest, catalog, &mut detail, |user| people.person(user));
                *winner = (ingest, event);
            }
        }
    }
    detail.measure::<DETAIL, _>(Step::CompactEventPush, || {
        events.extend(winners.into_values().filter_map(|(_, e)| e))
    });
    timer.mark_detailed("events_resolve_repeats", DETAIL.then_some(&detail))?;
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_filter_includes_every_spelling_and_hash_collision() {
        let hash = RandomState::with_seeds(1, 2, 3, 4);
        let slot = bit_position(&hash, 1, b"id");
        let collision = (0..)
            .map(|i| format!("other{i}"))
            .find(|s| bit_position(&hash, 1, s.as_bytes()) == slot)
            .unwrap();
        let other = format!("{collision},x");
        let chunks = [b"\"id\",a\n".as_slice(), other.as_bytes(), b"id,b\n"];
        let bits = repeated_bits(&chunks, 0, &hash);
        assert_eq!(bits[slot.0] & slot.1, slot.1);
        let collisions_only = [chunks[0], chunks[1]];
        assert_eq!(repeated_bits(&collisions_only, 0, &hash)[0], slot.1);
        assert_eq!(raw_id(b"\" x\",\"p"), b" x");
    }
}
