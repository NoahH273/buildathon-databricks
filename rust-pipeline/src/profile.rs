//! Exclusive operation wall times include clock-read cost, and run only in detailed profiles.
//! Untimed control flow and counter bookkeeping remain in the parent phase's residual.
#[cfg(feature = "profiling")]
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Step {
    FileSetup,
    BufferedLineRead,
    CsvStructureUtf8,
    RequiredIds,
    IngestTimestamp,
    DedupLookup,
    IndexInsertReplace,
    EventTimestampWindow,
    TypePurchasePrice,
    Lateness,
    IdentityResolution,
    ProductCategoryLookup,
    CompactEventPush,
}
#[cfg(feature = "profiling")]
const NAMES: [&str; 13] = [
    "file_discovery_open_header",
    "buffered_line_read",
    "csv_structure_utf8",
    "required_id_checks",
    "ingest_timestamp_parse",
    "dedup_hash_lookup_compare",
    "index_insert_replace_growth",
    "event_timestamp_window",
    "event_type_purchase_price",
    "lateness_check",
    "identity_resolution",
    "product_category_lookup",
    "collect_winners",
];
#[derive(Default)]
pub struct Breakdown {
    #[cfg(feature = "profiling")]
    elapsed: [Duration; NAMES.len()],
    #[cfg(feature = "profiling")]
    calls: [u64; NAMES.len()],
}
impl Breakdown {
    #[inline(always)]
    pub fn measure<const ENABLED: bool, T>(
        &mut self,
        step: Step,
        operation: impl FnOnce() -> T,
    ) -> T {
        #[cfg(feature = "profiling")]
        if ENABLED {
            let started = Instant::now();
            let value = operation();
            let elapsed = started.elapsed();
            self.elapsed[step as usize] += elapsed;
            self.calls[step as usize] += 1;
            return value;
        }
        let _ = step;
        operation()
    }
    #[cfg(feature = "profiling")]
    pub fn metrics(&self, parent_seconds: f64) -> serde_json::Value {
        let timed: f64 = self.elapsed.iter().map(Duration::as_secs_f64).sum();
        let steps: Vec<_> = NAMES.iter().enumerate().filter(|(i,_)|self.calls[*i] > 0).map(|(i,name)| {
            serde_json::json!({"name":name,"seconds":self.elapsed[i].as_secs_f64(),"calls":self.calls[i]})
        }).collect();
        serde_json::json!({"detailed_event_profile":true,"phase_seconds":parent_seconds,
            "timed_operations_seconds":timed,"unattributed_seconds":(parent_seconds-timed).max(0.0),
            "subphases":steps,
            "timing_note":"Exclusive operation wall times include clock-read cost. Residual includes instrumentation bookkeeping and uninstrumented control flow. Diagnostic runs only."})
    }
}
