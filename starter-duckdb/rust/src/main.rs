use anyhow::{Context, Result, bail};
use shopping_hybrid::{attribution, ingest};
use std::{collections::HashMap, path::PathBuf};
fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().context("expected ingest or attribute")?;
    let mut options = HashMap::new();
    while let Some(key) = args.next() {
        let value = args
            .next()
            .with_context(|| format!("missing value for {key}"))?;
        options.insert(key, value);
    }
    let get = |key: &str| options.get(key).with_context(|| format!("missing {key}"));
    let out = PathBuf::from(get("--out")?);
    let threads = get("--threads")?.parse::<usize>()?.max(1);
    match mode.as_str() {
        "ingest" => ingest::run(&PathBuf::from(get("--data")?), &out, threads),
        "attribute" => attribution::run(
            &PathBuf::from(get("--input")?),
            &out,
            threads,
            get("--memory-bytes")?.parse()?,
            options
                .get("--sorted-bucket")
                .map(|s| s.parse())
                .transpose()?,
        ),
        _ => bail!("unknown subcommand {mode}"),
    }
}
