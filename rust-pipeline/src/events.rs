use crate::{
    csv,
    ids::{Id, People},
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
    pub fn read(data: &Path) -> io::Result<Self> {
        let mut winners: HashMap<Box<str>, Winner> = HashMap::new();
        for path in csv::files(&data.join("events"))? {
            csv::read::<7>(&path, |f, line| {
                if f[0].is_empty() {
                    return;
                }
                // Guaranteed F1 by the input contract. Structural parsing already succeeded.
                let ingest = timestamp::parse(f[3]).expect("invalid guaranteed ingest_ts");
                if let Some(winner) = winners.get_mut(f[0]) {
                    if ingest < winner.ingest {
                        *winner = Winner {
                            ingest,
                            line: line.into(),
                        };
                    }
                } else {
                    winners.insert(
                        f[0].into(),
                        Winner {
                            ingest,
                            line: line.into(),
                        },
                    );
                }
            })?;
        }
        Ok(Self(winners))
    }
    pub fn validate(
        self,
        people: &mut People,
        products: &HashMap<Box<str>, Id>,
        unknown: Id,
        start: i64,
        end: i64,
    ) -> Vec<Event> {
        let mut events = Vec::with_capacity(self.0.len());
        for (_, winner) in self.0 {
            let f = csv::parse::<7>(&winner.line).unwrap();
            if f[1].is_empty() || f[5].is_empty() {
                continue;
            }
            let Some(ts) = timestamp::in_window(f[2], start, end) else {
                continue;
            };
            let cents = match f[4] {
                "view" | "cart" => 0,
                "purchase" => match timestamp::decimal(f[6]) {
                    Some(n) if (1..=10_000_000).contains(&n) => n,
                    _ => continue,
                },
                _ => continue,
            };
            if winner.ingest - ts > timestamp::DAY {
                continue;
            }
            events.push(Event {
                person: people.person(f[1]),
                ts,
                cents,
                category: products.get(f[5]).copied().unwrap_or(unknown),
            });
        }
        events
    }
}
