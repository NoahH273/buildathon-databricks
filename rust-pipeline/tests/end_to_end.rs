//! Supplemental black-box checks; the shared 36 fixtures remain the primary oracle.
use arrow_array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{
    fs::{self, File},
    path::Path,
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Workspace(std::path::PathBuf);
impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn dataset() -> Workspace {
    let path = std::env::temp_dir().join(format!(
        "shopping-rust-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&path).unwrap();
    for (name, header) in [
        (
            "events",
            "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
        ),
        ("clicks", "click_id,user_id,campaign_id,ts"),
        ("links", "user_id_a,user_id_b"),
    ] {
        fs::create_dir(path.join(name)).unwrap();
        fs::write(path.join(name).join("empty.csv"), format!("{header}\n")).unwrap();
    }
    fs::write(
        path.join("products.csv"),
        "product_id,category\np1,shoes\np2,books\n",
    )
    .unwrap();
    fs::write(
        path.join("campaigns.csv"),
        "campaign_id,channel\nc_1,search\nc_2,social\n",
    )
    .unwrap();
    fs::write(
        path.join("manifest.json"),
        r#"{"window_start":"2028-02-28T00:00:00Z","window_end":"2028-03-03T00:00:00Z"}"#,
    )
    .unwrap();
    Workspace(path)
}
fn run(path: &Path) {
    assert!(
        Command::new(env!("CARGO_BIN_EXE_shopping-sessions"))
            .env("DATA_DIR", path)
            .env("OUTPUT_DIR", path.join("out"))
            .env("CHECKPOINTS_ENABLED", "0")
            .status()
            .unwrap()
            .success()
    );
}
fn read(path: &Path, table: &str) -> Vec<arrow_array::RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(
        File::open(path.join("out").join(table).join("part-0.parquet")).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap()
    .map(Result::unwrap)
    .collect()
}
#[test]
fn empty_outputs_are_schema_bearing_date_parquet() {
    let w = dataset();
    run(&w.0);
    for table in ["user_daily", "campaign_daily"] {
        let r = ParquetRecordBatchReaderBuilder::try_new(
            File::open(w.0.join("out").join(table).join("part-0.parquet")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            r.schema().field_with_name("day").unwrap().data_type(),
            &arrow_schema::DataType::Date32
        );
        assert_eq!(r.metadata().file_metadata().num_rows(), 0);
        for field in r.schema().fields() {
            assert_eq!(field.is_nullable(), field.name() == "top_category");
        }
    }
}
#[test]
fn dedup_cross_person_midnight_category_ties_and_invalid_utf8() {
    let w = dataset();
    fs::write(w.0.join("links/part.csv"), "a,b\nu2,u1\nu1,A_unused\n").unwrap();
    let header = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n";
    let mut data=format!("{header}e1,u2,2028-02-28T23:50:00Z,2028-02-28T23:51:00Z,purchase,p1,500\ne2,u1,2028-02-29T00:10:00Z,2028-02-29T00:11:00Z,purchase,p2,500\ne3,u1,bad,2028-02-29T01:00:00Z,view,p1,\n").into_bytes();
    data.extend_from_slice(b"e4,\xff,2028-02-29T00:10:00Z,2028-02-29T00:11:00Z,view,p1,\n");
    fs::write(w.0.join("events/part.csv"), data).unwrap();
    fs::write(w.0.join("events/other.csv"),format!("{header}e1,u9,2028-02-29T01:50:00Z,2028-02-29T01:51:00Z,purchase,p1,9999\ne3,u1,2028-02-29T01:00:00Z,2028-02-29T01:01:00Z,view,p1,\n")).unwrap();
    fs::write(w.0.join("clicks/part.csv"),"click_id,user_id,campaign_id,ts\nk2,u1,c_1,2028-02-28T23:50:00Z\nk1,u2,c_2,2028-02-28T23:50:00Z\nk0,u1,c_1,2028-02-28T23:50:01Z\n").unwrap();
    run(&w.0);
    let u = read(&w.0, "user_daily");
    assert_eq!(u.len(), 1);
    let u = &u[0];
    assert_eq!(u.num_rows(), 1);
    let strings = |name| {
        u.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    };
    assert_eq!(strings("user_id").value(0), "A_unused");
    assert_eq!(strings("top_category").value(0), "books");
    let ints = |name| {
        u.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(
        (ints("sessions"), ints("events"), ints("revenue_cents")),
        (1, 2, 1000)
    );
    let c = read(&w.0, "campaign_daily");
    let c = &c[0];
    assert_eq!(c.num_rows(), 2);
    let names = c
        .column_by_name("campaign_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sessions = c
        .column_by_name("sessions")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let assists = c
        .column_by_name("assisted_sessions")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..2 {
        assert_eq!(assists.value(i), 1);
        assert_eq!(sessions.value(i), i64::from(names.value(i) == "c_2"));
    }
    assert_eq!(
        u.column_by_name("day").unwrap().data_type(),
        &arrow_schema::DataType::Date32
    );
    assert_eq!(u.column_by_name("user_id").unwrap().null_count(), 0);
}
