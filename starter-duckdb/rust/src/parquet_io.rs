use anyhow::{Context, Result};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    },
    basic::Compression,
    file::properties::WriterProperties,
};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

pub const BATCH_ROWS: usize = 32768;
pub const BATCH_BYTES: usize = 8 * 1024 * 1024;

pub fn writer(path: &Path, schema: SchemaRef) -> Result<ArrowWriter<File>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(BATCH_ROWS)
        .build();
    Ok(ArrowWriter::try_new(
        File::create(path)?,
        schema,
        Some(properties),
    )?)
}

pub fn write(path: &Path, schema: Schema, arrays: Vec<ArrayRef>) -> Result<()> {
    let schema = Arc::new(schema);
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let mut target = writer(path, schema)?;
    target.write(&batch)?;
    target.close()?;
    Ok(())
}

pub fn reader(path: &Path, batch_size: usize) -> Result<ParquetRecordBatchReader> {
    Ok(ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_batch_size(batch_size)
        .build()?)
}

pub fn files(path: &Path) -> Result<Vec<PathBuf>> {
    if !path.exists() {
        return Ok(vec![]);
    }
    let mut paths = vec![];
    for entry in std::fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_file() && path.extension().is_some_and(|v| v == "parquet") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

pub fn estimate(paths: &[PathBuf], record_size: u64) -> Result<(u64, u64)> {
    let mut rows = 0;
    let mut bytes = 0;
    for path in paths {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
            .with_context(|| format!("reading footer {}", path.display()))?;
        rows += builder.metadata().file_metadata().num_rows() as u64;
        bytes += builder
            .metadata()
            .row_groups()
            .iter()
            .map(|g| g.total_byte_size() as u64)
            .sum::<u64>();
    }
    // Capacity growth, decoded Arrow buffers, arenas and sort headroom.
    Ok((
        rows,
        rows.saturating_mul(record_size * 2)
            .saturating_add(bytes * 2),
    ))
}
