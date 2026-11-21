use crate::{
    aggregate::Users,
    events::Event,
    ids::Id,
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
pub fn build(events: Vec<Event>) -> (Vec<Session>, Users) {
    let mut sessions: Vec<Session> = Vec::new();
    let mut users = Users::new();
    let mut previous = None;
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
        let day = users.entry((event.person, session.day)).or_default();
        day.sessions += i64::from(new);
        day.events += 1;
        day.revenue += event.cents;
        if event.cents > 0 {
            *day.categories.entry(event.category).or_default() += event.cents;
        }
        previous = Some((event.person, event.ts));
    }
    (sessions, users)
}
