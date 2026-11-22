use crate::{
    csv,
    ids::{Id, People},
    profile::{Breakdown, Step},
    timestamp,
};
use std::{collections::HashMap, io, path::Path};

pub struct Event {
    pub person: Id,
    pub ts: i64,
    pub cents: i64,
    pub category: Id,
}
struct Winner {
    ingest: i64,
    event: Option<Event>,
}

// An invalid projection still owns its ID and ingest time. Only the earliest
// structurally valid copy may decide whether an event reaches sessionization.
pub struct Dedup(Vec<Winner>);

pub struct Catalog<'a> {
    pub products: &'a HashMap<Box<str>, Id>,
    pub unknown: Id,
    pub start: i64,
    pub end: i64,
}

impl Dedup {
    pub fn read<const DETAIL: bool>(
        data: &Path,
        people: &mut People,
        catalog: &Catalog<'_>,
        profile: &mut Breakdown,
    ) -> io::Result<Self> {
        let mut index = HashMap::<Box<str>, usize>::new();
        let mut winners = Vec::<Winner>::new();
        let paths =
            profile.measure::<DETAIL, _>(Step::FileSetup, || csv::files(&data.join("events")))?;
        for path in paths {
            let mut accept = |f: [&str; 7], _: &[u8], profile: &mut Breakdown| {
                if f[0].is_empty() {
                    return;
                }
                let ingest = profile.measure::<DETAIL, _>(Step::IngestTimestamp, || {
                    timestamp::parse(f[3]).expect("invalid guaranteed ingest_ts")
                });
                let slot =
                    profile.measure::<DETAIL, _>(Step::DedupLookup, || index.get(f[0]).copied());
                if slot.is_some_and(|i| ingest >= winners[i].ingest) {
                    return;
                }
                let event = project::<DETAIL>(&f, ingest, people, catalog, profile);
                let winner = Winner { ingest, event };
                profile.measure::<DETAIL, _>(Step::IndexInsertReplace, || match slot {
                    Some(i) => winners[i] = winner,
                    None => {
                        index.insert(f[0].into(), winners.len());
                        winners.push(winner);
                    }
                });
            };
            if DETAIL {
                csv::read_profiled::<7>(&path, profile, accept)?;
            } else {
                csv::read::<7>(&path, |f, line| accept(f, line, profile))?;
            }
        }
        Ok(Self(winners))
    }

    pub fn finish<const DETAIL: bool>(self, profile: &mut Breakdown) -> Vec<Event> {
        profile.measure::<DETAIL, _>(Step::CompactEventPush, || {
            self.0
                .into_iter()
                .filter_map(|winner| winner.event)
                .collect()
        })
    }
}

fn project<const DETAIL: bool>(
    f: &[&str; 7],
    ingest: i64,
    people: &mut People,
    catalog: &Catalog<'_>,
    profile: &mut Breakdown,
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
    let person = profile.measure::<DETAIL, _>(Step::IdentityResolution, || people.person(f[1]));
    Some(Event {
        person,
        ts,
        cents,
        category,
    })
}
