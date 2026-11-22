mod aggregate;
mod clicks;
mod csv;
mod events;
mod ids;
mod parquet;
mod profile;
mod sessions;
mod timestamp;
mod timing;
use ids::{Interner, People};
#[cfg(feature = "profiling")]
use std::time::Instant;
use std::{collections::HashMap, env, fs, path::PathBuf};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn run() -> Result<()> {
    #[cfg(feature = "profiling")]
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
    let mut timer = timing::Timer::new()?;
    let detailed_events = timer.detailed_events();
    let available = std::thread::available_parallelism()?.get();
    let requested = env::var("NCPU")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0);
    // Exclusive operation timers need one clock timeline. Coarse profiles and
    // submission builds both use the regular worker count.
    let workers = if detailed_events {
        1
    } else {
        requested.unwrap_or(available).min(available)
    };
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build_global()?;
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
    let catalog = events::Catalog {
        products: &products,
        unknown,
        start,
        end,
    };
    let mut events = if detailed_events {
        events::read::<true>(&data, &mut people, &catalog, &mut timer)?
    } else {
        events::read::<false>(&data, &mut people, &catalog, &mut timer)?
    };
    sessions::sort(&mut events);
    timer.mark("events_sort")?;
    let event_count = events.len();
    let (sessions, users) = sessions::build(events, &categories);
    timer.mark("sessions_user_aggregation")?;
    let click_files = csv::load(&data.join("clicks"))?;
    let mut clicks = clicks::read(&click_files, &mut people, &campaign_lookup, start, end);
    timer.mark("clicks_read_validate")?;
    clicks::sort(&mut clicks);
    timer.mark("clicks_sort_dedup")?;
    let campaigns = clicks::attribute(&sessions, &clicks, channels.len(), organic);
    timer.mark("attribution_campaign_aggregation")?;
    // Necessary conservation checks; exact correctness is checked by the independent harness.
    let us: i128 = users.iter().map(|(_, u)| i128::from(u.sessions)).sum();
    let ur: i128 = users.iter().map(|(_, u)| i128::from(u.revenue)).sum();
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
    drop(click_files);
    drop(products);
    parquet::users(&out, users, &people.names, &categories)?;
    parquet::campaigns(&out, campaigns, &campaign_names, &channels)?;
    timer.mark("parquet_write")?;
    #[cfg(feature = "profiling")]
    eprintln!(
        "events={event_count} sessions={} user_rows={user_rows} campaign_rows={campaign_rows} elapsed={:.6}s",
        sessions.len(),
        total.elapsed().as_secs_f64()
    );
    #[cfg(not(feature = "profiling"))]
    eprintln!(
        "events={event_count} sessions={} user_rows={user_rows} campaign_rows={campaign_rows}",
        sessions.len()
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
