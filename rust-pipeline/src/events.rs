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
    line: Box<[u8]>,
}
pub struct Dedup(HashMap<Box<str>, Winner>);

impl Dedup {
    pub fn read<const DETAIL: bool>(data: &Path, profile: &mut Breakdown) -> io::Result<Self> {
        let mut winners: HashMap<Box<str>, Winner> = HashMap::new();
        let paths =
            profile.measure::<DETAIL, _>(Step::FileSetup, || csv::files(&data.join("events")))?;
        for path in paths {
            let mut accept = |f: [&str; 7], line: &[u8], profile: &mut Breakdown| {
                if profile.measure::<DETAIL, _>(Step::RequiredIds, || f[0].is_empty()) {
                    return;
                }
                // Guaranteed F1 by the input contract. Structural parsing already succeeded.
                let ingest = profile.measure::<DETAIL, _>(Step::IngestTimestamp, || {
                    timestamp::parse(f[3]).expect("invalid guaranteed ingest_ts")
                });
                let winner = profile.measure::<DETAIL, _>(Step::DedupLookup, || {
                    winners.get_mut(f[0]).map(|winner| {
                        let replace = ingest < winner.ingest;
                        (winner, replace)
                    })
                });
                if let Some((winner, replace)) = winner {
                    if replace {
                        let line = profile
                            .measure::<DETAIL, _>(Step::WinnerAllocationCopy, || line.into());
                        profile.measure::<DETAIL, _>(Step::IndexInsertReplace, || {
                            *winner = Winner { ingest, line }
                        });
                    }
                } else {
                    let (id, line) = profile
                        .measure::<DETAIL, _>(Step::WinnerAllocationCopy, || {
                            (Box::<str>::from(f[0]), Box::<[u8]>::from(line))
                        });
                    profile.measure::<DETAIL, _>(Step::IndexInsertReplace, || {
                        winners.insert(id, Winner { ingest, line })
                    });
                }
            };
            if DETAIL {
                csv::read_profiled::<7>(&path, profile, accept)?;
            } else {
                csv::read::<7>(&path, |f, line| accept(f, line, profile))?;
            }
        }
        Ok(Self(winners))
    }
    pub fn validate<const DETAIL: bool>(
        self,
        people: &mut People,
        products: &HashMap<Box<str>, Id>,
        unknown: Id,
        start: i64,
        end: i64,
        profile: &mut Breakdown,
    ) -> Vec<Event> {
        let mut events = profile.measure::<DETAIL, _>(Step::EventBufferAllocation, || {
            Vec::with_capacity(self.0.len())
        });
        let mut winners = self.0.into_iter();
        loop {
            let Some((id, winner)) =
                profile.measure::<DETAIL, _>(Step::WinnerTableIteration, || winners.next())
            else {
                break;
            };
            // A row-local function keeps early validation exits inside the measured operations.
            if let Some(event) =
                validate_winner::<DETAIL>(&winner, people, products, unknown, start, end, profile)
            {
                profile.measure::<DETAIL, _>(Step::CompactEventPush, || events.push(event));
            }
            profile.measure::<DETAIL, _>(Step::RawWinnerRelease, || drop((id, winner)));
        }
        profile.measure::<DETAIL, _>(Step::WinnerTableRelease, || drop(winners));
        events
    }
}
fn validate_winner<const DETAIL: bool>(
    winner: &Winner,
    people: &mut People,
    products: &HashMap<Box<str>, Id>,
    unknown: Id,
    start: i64,
    end: i64,
    profile: &mut Breakdown,
) -> Option<Event> {
    let f = profile.measure::<DETAIL, _>(Step::WinnerCsvReparse, || {
        csv::parse::<7>(&winner.line).unwrap()
    });
    if profile.measure::<DETAIL, _>(Step::RequiredIds, || f[1].is_empty() || f[5].is_empty()) {
        return None;
    }
    let ts = profile.measure::<DETAIL, _>(Step::EventTimestampWindow, || {
        timestamp::in_window(f[2], start, end)
    })?;
    let cents = profile.measure::<DETAIL, _>(Step::TypePurchasePrice, || match f[4] {
        "view" | "cart" => Some(0),
        "purchase" => timestamp::decimal(f[6]).filter(|n| (1..=10_000_000).contains(n)),
        _ => None,
    })?;
    if profile.measure::<DETAIL, _>(Step::Lateness, || winner.ingest - ts > timestamp::DAY) {
        return None;
    }
    let person = profile.measure::<DETAIL, _>(Step::IdentityResolution, || people.person(f[1]));
    let category = profile.measure::<DETAIL, _>(Step::ProductCategoryLookup, || {
        products.get(f[5]).copied().unwrap_or(unknown)
    });
    Some(Event {
        person,
        ts,
        cents,
        category,
    })
}
