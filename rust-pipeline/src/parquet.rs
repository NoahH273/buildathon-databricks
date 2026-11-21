use crate::{
    aggregate::{Campaigns, Users},
    ids::Interner,
};
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const BATCH: usize = 65536;
fn schema(fields: &[(&str, DataType, bool)]) -> Arc<Schema> {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(n, t, null)| Field::new(*n, t.clone(), *null))
            .collect::<Vec<_>>(),
    ))
}
fn writer(out: &Path, table: &str, schema: Arc<Schema>) -> Result<ArrowWriter<File>> {
    let directory = out.join(table);
    fs::create_dir_all(&directory)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(BATCH)
        .build();
    Ok(ArrowWriter::try_new(
        File::create(directory.join("part-0.parquet"))?,
        schema,
        Some(props),
    )?)
}
fn strings<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}
fn ints(values: impl Iterator<Item = i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(values))
}

pub fn users(out: &Path, users: Users, people: &Interner, categories: &Interner) -> Result<()> {
    let schema = schema(&[
        ("user_id", DataType::Utf8, false),
        ("day", DataType::Date32, false),
        ("sessions", DataType::Int64, false),
        ("events", DataType::Int64, false),
        ("revenue_cents", DataType::Int64, false),
        ("top_category", DataType::Utf8, true),
    ]);
    let mut writer = writer(out, "user_daily", schema.clone())?;
    let mut rows: Vec<_> = users.into_iter().collect();
    rows.sort_unstable_by_key(|(key, _)| *key);
    for batch in rows.chunks(BATCH) {
        let top = batch.iter().map(|(_, u)| {
            u.categories
                .iter()
                .min_by(|(a, ar), (b, br)| {
                    br.cmp(ar)
                        .then_with(|| categories.values[**a].cmp(&categories.values[**b]))
                })
                .map(|(&c, _)| &*categories.values[c])
        });
        writer.write(&RecordBatch::try_new(
            schema.clone(),
            vec![
                strings(batch.iter().map(|((p, _), _)| &*people.values[*p])),
                Arc::new(Date32Array::from_iter_values(
                    batch.iter().map(|((_, d), _)| *d),
                )),
                ints(batch.iter().map(|(_, u)| u.sessions)),
                ints(batch.iter().map(|(_, u)| u.events)),
                ints(batch.iter().map(|(_, u)| u.revenue)),
                Arc::new(StringArray::from_iter(top)),
            ],
        )?)?;
    }
    writer.close()?;
    Ok(())
}
pub fn campaigns(
    out: &Path,
    campaigns: Campaigns,
    names: &Interner,
    channels: &[Box<str>],
) -> Result<()> {
    let schema = schema(&[
        ("campaign_id", DataType::Utf8, false),
        ("channel", DataType::Utf8, false),
        ("day", DataType::Date32, false),
        ("sessions", DataType::Int64, false),
        ("purchasing_sessions", DataType::Int64, false),
        ("revenue_cents", DataType::Int64, false),
        ("assisted_sessions", DataType::Int64, false),
    ]);
    let mut writer = writer(out, "campaign_daily", schema.clone())?;
    let mut rows: Vec<_> = campaigns.into_iter().collect();
    rows.sort_unstable_by_key(|(key, _)| *key);
    for batch in rows.chunks(BATCH) {
        writer.write(&RecordBatch::try_new(
            schema.clone(),
            vec![
                strings(batch.iter().map(|((c, _), _)| &*names.values[*c])),
                strings(batch.iter().map(|((c, _), _)| &*channels[*c])),
                Arc::new(Date32Array::from_iter_values(
                    batch.iter().map(|((_, d), _)| *d),
                )),
                ints(batch.iter().map(|(_, c)| c.sessions)),
                ints(batch.iter().map(|(_, c)| c.purchasing)),
                ints(batch.iter().map(|(_, c)| c.revenue)),
                ints(batch.iter().map(|(_, c)| c.assists)),
            ],
        )?)?;
    }
    writer.close()?;
    Ok(())
}
