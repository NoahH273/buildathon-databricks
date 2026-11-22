use crate::{
    aggregate::Users,
    events::Event,
    ids::{Id, Interner},
    timestamp::{DAY, GAP},
};
pub struct Session {
    pub person: Id,
    pub start: i64,
    pub day: i32,
    pub revenue: i64,
}

pub fn sort(events: &mut [Event]) {
    events.sort_unstable_by_key(|e| (e.person, e.ts));
}
pub fn build(events: Vec<Event>, categories: &Interner) -> (Vec<Session>, Users) {
    let mut sessions: Vec<Session> = Vec::new();
    let mut users = Users::new();
    let mut previous = None;
    // Person and session-start day are monotone in the sorted stream.
    let mut totals = vec![0i64; categories.values.len()];
    let mut touched = Vec::new();
    for event in events {
        let new = previous.is_none_or(|(person, ts)| person != event.person || event.ts - ts > GAP);
        if new {
            sessions.push(Session {
                person: event.person,
                start: event.ts,
                day: i32::try_from(event.ts.div_euclid(DAY)).expect("day outside Date32"),
                revenue: 0,
            });
        }
        let session = sessions.last_mut().unwrap();
        session.revenue += event.cents;
        let key = (event.person, session.day);
        if users.last().is_none_or(|(last, _)| *last != key) {
            for category in touched.drain(..) {
                totals[category] = 0;
            }
            users.push((key, Default::default()));
        }
        let day = &mut users.last_mut().unwrap().1;
        day.sessions += i64::from(new);
        day.events += 1;
        day.revenue += event.cents;
        if event.cents > 0 {
            let category = event.category;
            if totals[category] == 0 {
                touched.push(category);
            }
            totals[category] += event.cents;
            if day.top.is_none_or(|best| {
                totals[category] > totals[best]
                    || (totals[category] == totals[best]
                        && categories.values[category] < categories.values[best])
            }) {
                day.top = Some(category);
            }
        }
        previous = Some((event.person, event.ts));
    }
    (sessions, users)
}
