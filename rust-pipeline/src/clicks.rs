use crate::{
    aggregate::Campaigns,
    csv,
    ids::{Id, People},
    sessions::Session,
    timestamp::{self, WEEK},
};
use std::{
    collections::{HashMap, HashSet},
    io,
    path::Path,
};
pub struct Click {
    pub person: Id,
    pub ts: i64,
    pub id: Box<str>,
    pub campaign: Id,
}
pub fn read(
    data: &Path,
    people: &mut People,
    campaigns: &HashMap<Box<str>, Id>,
    start: i64,
    end: i64,
) -> io::Result<Vec<Click>> {
    let mut seen: HashSet<Box<str>> = HashSet::new();
    let mut clicks = Vec::new();
    for path in csv::files(&data.join("clicks"))? {
        csv::read::<4>(&path, |f, _| {
            if f[0].is_empty() || !seen.insert(f[0].into()) {
                return;
            }
            if f[1].is_empty() {
                return;
            }
            let Some(&campaign) = campaigns.get(f[2]) else {
                return;
            };
            let Some(ts) = timestamp::in_window(f[3], start - WEEK, end) else {
                return;
            };
            clicks.push(Click {
                person: people.person(f[1]),
                ts,
                id: f[0].into(),
                campaign,
            });
        })?;
    }
    Ok(clicks)
}
pub fn sort(clicks: &mut [Click]) {
    clicks.sort_unstable_by(|a, b| (a.person, a.ts, &a.id).cmp(&(b.person, b.ts, &b.id)));
}

/// Each person's window is [left,right), with one dense active-campaign set.
/// latest_first remembers the first (smallest ID) click in the newest timestamp group.
pub fn attribute(
    sessions: &[Session],
    clicks: &[Click],
    campaign_count: usize,
    organic: Id,
) -> Campaigns {
    let mut result = Campaigns::new();
    let mut counts = vec![0usize; campaign_count];
    let mut positions = vec![usize::MAX; campaign_count];
    let mut active = Vec::<Id>::new();
    let mut cursor = 0;
    let mut session_at = 0;
    while session_at < sessions.len() {
        let person = sessions[session_at].person;
        while cursor < clicks.len() && clicks[cursor].person < person {
            cursor += 1;
        }
        let begin = cursor;
        while cursor < clicks.len() && clicks[cursor].person == person {
            cursor += 1;
        }
        let person_clicks = &clicks[begin..cursor];
        let mut left = 0;
        let mut right = 0;
        let mut latest_first = 0;
        while session_at < sessions.len() && sessions[session_at].person == person {
            let session = &sessions[session_at];
            while right < person_clicks.len() && person_clicks[right].ts <= session.start {
                let click = &person_clicks[right];
                if right == 0 || click.ts != person_clicks[right - 1].ts {
                    latest_first = right;
                }
                if counts[click.campaign] == 0 {
                    positions[click.campaign] = active.len();
                    active.push(click.campaign);
                }
                counts[click.campaign] += 1;
                right += 1;
            }
            while left < right && person_clicks[left].ts < session.start - WEEK {
                let campaign = person_clicks[left].campaign;
                counts[campaign] -= 1;
                if counts[campaign] == 0 {
                    let position = positions[campaign];
                    active.swap_remove(position);
                    if position < active.len() {
                        positions[active[position]] = position;
                    }
                    positions[campaign] = usize::MAX;
                }
                left += 1;
            }
            let winner = if left < right {
                person_clicks[latest_first].campaign
            } else {
                organic
            };
            let credited = result.entry((winner, session.day)).or_default();
            credited.sessions += 1;
            credited.purchasing += i64::from(session.revenue > 0);
            credited.revenue += session.revenue;
            for &campaign in &active {
                result.entry((campaign, session.day)).or_default().assists += 1;
            }
            session_at += 1;
        }
        for campaign in active.drain(..) {
            counts[campaign] = 0;
            positions[campaign] = usize::MAX;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sweep_matches_brute_force_with_expiry_ties_and_swap_removal() {
        let mut clicks = Vec::new();
        // Deliberate repeated times, several persons, gaps, repeated campaigns, and byte ID ties.
        for i in 0..500 {
            clicks.push(Click {
                person: i % 4,
                ts: (i as i64 / 4) * 10000000,
                id: format!("k{}", 500 - i).into(),
                campaign: i % 7,
            });
        }
        sort(&mut clicks);
        let sessions: Vec<_> = (0..5)
            .flat_map(|person| {
                (0..180).map(move |i| Session {
                    person,
                    start: i * 11000000,
                    day: (i / 8) as i32,
                    revenue: if i % 3 == 0 { 25 } else { 0 },
                })
            })
            .collect();
        let actual = attribute(&sessions, &clicks, 8, 7);
        let mut expected = Campaigns::new();
        for s in &sessions {
            let eligible: Vec<_> = clicks
                .iter()
                .filter(|c| c.person == s.person && c.ts <= s.start && c.ts >= s.start - WEEK)
                .collect();
            let winner = eligible
                .iter()
                .min_by(|a, b| b.ts.cmp(&a.ts).then_with(|| a.id.cmp(&b.id)))
                .map_or(7, |c| c.campaign);
            let row = expected.entry((winner, s.day)).or_default();
            row.sessions += 1;
            row.purchasing += i64::from(s.revenue > 0);
            row.revenue += s.revenue;
            for campaign in eligible.iter().map(|c| c.campaign).collect::<HashSet<_>>() {
                expected.entry((campaign, s.day)).or_default().assists += 1;
            }
        }
        assert_eq!(actual, expected);
    }
}
