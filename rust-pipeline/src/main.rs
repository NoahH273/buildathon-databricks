mod aggregate;
mod clicks;
mod csv;
mod events;
mod ids;
mod parquet;
mod profile;
mod sessions;
mod timestamp;
use ids::{Interner, People};
use std::{collections::HashMap, env, fs, io::Write, path::PathBuf, time::Instant};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Timer {
    started: Instant,
    last: Instant,
    enabled: bool,
    checkpoints: Option<fs::File>,
}
impl Timer {
    fn mark(&mut self, phase: &str) -> Result<()> {
        self.mark_detailed(phase, None)
    }
    fn mark_detailed(&mut self, phase: &str, detail: Option<&profile::Breakdown>) -> Result<()> {
        if !self.enabled && self.checkpoints.is_none() {
            return Ok(());
        }
        let now = Instant::now();
        let seconds = now.duration_since(self.last).as_secs_f64();
        let metrics = detail.map(|detail| detail.metrics(seconds));
        if self.enabled {
            eprintln!("phase {phase}: {:.6}s", seconds);
            if let Some(metrics) = &metrics {
                eprintln!("phase_detail {phase}: {metrics}");
            }
        }
        if let Some(file) = &mut self.checkpoints {
            writeln!(
                file,
                "{}",
                serde_json::json!({"name":phase,"elapsed_seconds":now.duration_since(self.started).as_secs_f64(),"metrics":metrics.unwrap_or_else(||serde_json::json!({}))})
            )?;
        }
        self.last = now;
        Ok(())
    }
}
fn run() -> Result<()> {
    let total = Instant::now();
    let args: Vec<_> = env::args_os().skip(1).collect();
    if !args.is_empty() && args.len() != 2 {
        return Err("usage: shopping-sessions [DATA_DIR OUTPUT_DIR]".into());
    }
    let path = |key: &str, default: &str| {
        PathBuf::from(env::var_os(key).unwrap_or_else(|| default.into()))
    };
    let data = args
        .first()
        .map(PathBuf::from)
        .unwrap_or_else(|| path("DATA_DIR", "/data"));
    let out = args
        .get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| path("OUTPUT_DIR", "/output"));
    let mut timer = Timer {
        started: total,
        last: Instant::now(),
        enabled: env::var_os("DIAGNOSTICS").is_some(),
        checkpoints: if env::var("CHECKPOINTS_ENABLED").as_deref() == Ok("1") {
            env::var_os("CHECKPOINTS_FILE")
                .map(fs::File::create)
                .transpose()?
        } else {
            None
        },
    };
    // Dispatch once per phase; const specialization removes per-row clocks and
    // profile branches from ordinary measured runs. Coarse-only profiles use 0.
    let detailed_events = (timer.enabled || timer.checkpoints.is_some())
        && env::var("CHECKPOINTS_ENABLED").as_deref() != Ok("0")
        && env::var("EVENT_PROFILE_DETAIL").as_deref() != Ok("0");
    // The implementation is serial and in-memory: NCPU/MEM_GB are resource ceilings,
    // not a promise to allocate all available resources. No runtime cache/spills.
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json"))?)?;
    let start = manifest["window_start"]
        .as_str()
        .and_then(timestamp::parse)
        .ok_or("bad window_start")?;
    let end = manifest["window_end"]
        .as_str()
        .and_then(timestamp::parse)
        .ok_or("bad window_end")?;
    if start >= end {
        return Err("empty or reversed manifest window".into());
    }
    let mut categories = Interner::default();
    let unknown = categories.intern("unknown");
    let mut products = HashMap::new();
    csv::read::<2>(&data.join("products.csv"), |f, _| {
        products.insert(f[0].into(), categories.intern(f[1]));
    })?;
    let mut campaign_names = Interner::default();
    let mut channels: Vec<Box<str>> = Vec::new();
    let mut campaign_lookup = HashMap::new();
    csv::read::<2>(&data.join("campaigns.csv"), |f, _| {
        let id = campaign_names.intern(f[0]);
        channels.push(f[1].into());
        campaign_lookup.insert(f[0].into(), id);
    })?;
    let organic = campaign_names.intern("organic");
    channels.push("organic".into());
    timer.mark("catalogs")?;
    let mut people = People::default();
    for path in csv::files(&data.join("links"))? {
        csv::read::<2>(&path, |f, _| {
            if !f[0].is_empty() && !f[1].is_empty() {
                people.union(f[0], f[1]);
            }
        })?;
    }
    timer.mark("links_parse_intern_union")?;
    let mut detail = profile::Breakdown::default();
    let dedup = if detailed_events {
        events::Dedup::read::<true>(&data, &mut detail)?
    } else {
        events::Dedup::read::<false>(&data, &mut detail)?
    };
    timer.mark_detailed("events_parse_dedup", detailed_events.then_some(&detail))?;
    let mut detail = profile::Breakdown::default();
    let mut events = if detailed_events {
        dedup.validate::<true>(&mut people, &products, unknown, start, end, &mut detail)
    } else {
        dedup.validate::<false>(&mut people, &products, unknown, start, end, &mut detail)
    };
    timer.mark_detailed(
        "events_validate_resolve",
        detailed_events.then_some(&detail),
    )?;
    sessions::sort(&mut events);
    timer.mark("events_sort")?;
    let event_count = events.len();
    let (sessions, users) = sessions::build(events);
    timer.mark("sessions_user_aggregation")?;
    let mut clicks = clicks::read(&data, &mut people, &campaign_lookup, start, end)?;
    timer.mark("clicks_parse_dedup_validate")?;
    clicks::sort(&mut clicks);
    timer.mark("clicks_sort")?;
    let campaigns = clicks::attribute(&sessions, &clicks, channels.len(), organic);
    timer.mark("attribution_campaign_aggregation")?;
    // Necessary conservation checks; exact correctness is checked by the independent harness.
    let us: i128 = users.values().map(|u| i128::from(u.sessions)).sum();
    let ur: i128 = users.values().map(|u| i128::from(u.revenue)).sum();
    let cs: i128 = campaigns.values().map(|c| i128::from(c.sessions)).sum();
    let cr: i128 = campaigns.values().map(|c| i128::from(c.revenue)).sum();
    let assisted: i128 = campaigns.values().map(|c| i128::from(c.assists)).sum();
    let nonorganic: i128 = campaigns
        .iter()
        .filter(|((c, _), _)| *c != organic)
        .map(|(_, c)| i128::from(c.sessions))
        .sum();
    if us != cs
        || ur != cr
        || assisted < nonorganic
        || campaigns
            .iter()
            .any(|((c, _), v)| *c == organic && v.assists != 0)
    {
        return Err("aggregate conservation failure".into());
    }
    let user_rows = users.len();
    let campaign_rows = campaigns.len();
    drop(clicks);
    drop(products);
    parquet::users(&out, users, &people.names, &categories)?;
    parquet::campaigns(&out, campaigns, &campaign_names, &channels)?;
    timer.mark("parquet_write")?;
    eprintln!(
        "events={event_count} sessions={} user_rows={user_rows} campaign_rows={campaign_rows} elapsed={:.6}s",
        sessions.len(),
        total.elapsed().as_secs_f64()
    );
    Ok(())
}
fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("shopping-sessions: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
