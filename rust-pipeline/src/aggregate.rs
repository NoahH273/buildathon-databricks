use crate::ids::Id;
use std::collections::HashMap;
#[derive(Default)]
pub struct UserDay {
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub categories: HashMap<Id, i64>,
}
#[derive(Default, Debug, PartialEq)]
pub struct CampaignDay {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assists: i64,
}
pub type Users = HashMap<(Id, i32), UserDay>;
pub type Campaigns = HashMap<(Id, i32), CampaignDay>;
