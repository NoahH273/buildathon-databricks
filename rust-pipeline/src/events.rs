use crate::{
    csv,
    ids::{Id, People},
    profile::{Breakdown, Step},
    timestamp,
};
use ahash::{AHashMap, RandomState};
use rayon::prelude::*;
use std::{collections::HashMap, path::Path};

#[derive(Clone, Copy)]
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

struct Pending<'a> {
    id: &'a str,
    ingest: i64,
    event: Option<Event>,
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

fn reduce<'a, const DETAIL: bool>(parts: &[Vec<Pending<'a>>]) -> (Vec<Event>, Breakdown) {
    let mut detail = Breakdown::default();
    let rows = parts.iter().map(Vec::len).sum();
    let mut winners = AHashMap::with_capacity(rows);
    for row in parts.iter().flatten() {
        detail.measure::<DETAIL, _>(Step::DedupLookup, || {
            let winner = winners.entry(row.id).or_insert(row);
            if row.ingest < winner.ingest {
                *winner = row;
            }
        });
    }
    let events = winners.into_values().filter_map(|row| row.event).collect();
    (events, detail)
}

pub fn read<const DETAIL: bool>(
    data: &Path,
    people: &mut People,
    catalog: &Catalog<'_>,
    timer: &mut crate::timing::Timer,
) -> crate::Result<Vec<Event>> {
    let files = csv::load(&data.join("events"))?;
    let chunks = csv::chunks(&files);
    let workers = rayon::current_num_threads();
    let shard_count = (workers * 8).next_power_of_two();
    let hash = RandomState::with_seeds(0, 1, 2, 3);
    let canonical = people.canonical_ids();
    timer.mark("events_read")?;
    let batches: Vec<_> = chunks
        .par_iter()
        .map(|chunk| {
            let mut shards: Vec<Vec<Pending>> = (0..shard_count).map(|_| Vec::new()).collect();
            let mut local = AHashMap::new();
            let mut names = Vec::new();
            let mut detail = Breakdown::default();
            for line in csv::lines(chunk) {
                let Some(f) =
                    detail.measure::<DETAIL, _>(Step::CsvStructureUtf8, || csv::parse(line))
                else {
                    continue;
                };
                if f[0].is_empty() {
                    continue;
                }
                let ingest = ingest::<DETAIL>(&f, &mut detail);
                let event = project::<DETAIL>(&f, ingest, catalog, &mut detail, |user| {
                    if let Some(id) = people.names.lookup(user) {
                        return canonical[id];
                    }
                    *local.entry(user).or_insert_with(|| {
                        let id = canonical.len() + names.len();
                        names.push(user);
                        id
                    })
                });
                let shard = hash.hash_one(f[0].as_bytes()) as usize & (shard_count - 1);
                shards[shard].push(Pending {
                    id: f[0],
                    ingest,
                    event,
                });
            }
            (shards, names, detail)
        })
        .collect();
    let mut parts: Vec<Vec<Vec<Pending>>> = (0..shard_count).map(|_| Vec::new()).collect();
    let mut detail = Breakdown::default();
    let local_batches: Vec<_> = batches
        .into_iter()
        .map(|(shards, names, profile)| {
            if DETAIL {
                detail.merge(profile);
            }
            let ids: Vec<_> = names.into_iter().map(|name| people.person(name)).collect();
            (shards, ids)
        })
        .collect();
    timer.mark_detailed("events_project_candidates", DETAIL.then_some(&detail))?;
    let translated: Vec<_> = local_batches
        .into_par_iter()
        .map(|(mut shards, ids)| {
            for event in shards
                .iter_mut()
                .flatten()
                .filter_map(|row| row.event.as_mut())
            {
                if event.person >= canonical.len() {
                    event.person = ids[event.person - canonical.len()];
                }
            }
            shards
        })
        .collect();
    for shards in translated {
        for (part, bucket) in parts.iter_mut().zip(shards) {
            part.push(bucket);
        }
    }
    let mut reduced: Vec<_> = parts
        .into_par_iter()
        .map(|part| reduce::<DETAIL>(&part))
        .collect();
    let mut detail = Breakdown::default();
    if DETAIL {
        for (_, profile) in &mut reduced {
            detail.merge(std::mem::take(profile));
        }
    }
    let events = detail.measure::<DETAIL, _>(Step::CompactEventPush, || {
        let mut events = Vec::with_capacity(reduced.iter().map(|(rows, _)| rows.len()).sum());
        for (mut rows, _) in reduced {
            events.append(&mut rows);
        }
        events
    });
    timer.mark_detailed("events_dedup_shards", DETAIL.then_some(&detail))?;
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn same_shard_keeps_distinct_keys_and_an_invalid_earliest_copy() {
        let event = Event {
            person: 7,
            ts: 2,
            cents: 0,
            category: 0,
        };
        let parts = [
            vec![
                Pending {
                    id: "a",
                    ingest: 3,
                    event: Some(event),
                },
                Pending {
                    id: "á",
                    ingest: 3,
                    event: Some(event),
                },
            ],
            vec![Pending {
                id: "a",
                ingest: 1,
                event: None,
            }],
        ];
        let (survivors, _) = reduce::<false>(&parts);
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].person, 7);
    }
}
