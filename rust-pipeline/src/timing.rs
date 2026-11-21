//! Submission builds exclude environment-controlled clocks and checkpoint I/O entirely.
#[cfg(feature = "profiling")]
use std::{env, fs, io::Write, time::Instant};

#[cfg(feature = "profiling")]
pub struct Timer {
    started: Instant,
    last: Instant,
    diagnostics: bool,
    checkpoints: Option<fs::File>,
}
#[cfg(feature = "profiling")]
impl Timer {
    pub fn new() -> crate::Result<Self> {
        let now = Instant::now();
        Ok(Self {
            started: now,
            last: now,
            diagnostics: env::var_os("DIAGNOSTICS").is_some(),
            checkpoints: if env::var("CHECKPOINTS_ENABLED").as_deref() == Ok("1") {
                env::var_os("CHECKPOINTS_FILE")
                    .map(fs::File::create)
                    .transpose()?
            } else {
                None
            },
        })
    }
    pub fn detailed_events(&self) -> bool {
        (self.diagnostics || self.checkpoints.is_some())
            && env::var("CHECKPOINTS_ENABLED").as_deref() != Ok("0")
            && env::var("EVENT_PROFILE_DETAIL").as_deref() != Ok("0")
    }
    pub fn mark(&mut self, phase: &str) -> crate::Result<()> {
        self.mark_detailed(phase, None)
    }
    pub fn mark_detailed(
        &mut self,
        phase: &str,
        detail: Option<&crate::profile::Breakdown>,
    ) -> crate::Result<()> {
        if !self.diagnostics && self.checkpoints.is_none() {
            return Ok(());
        }
        let now = Instant::now();
        let seconds = now.duration_since(self.last).as_secs_f64();
        let metrics = detail.map(|detail| detail.metrics(seconds));
        if self.diagnostics {
            eprintln!("phase {phase}: {seconds:.6}s");
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

#[cfg(not(feature = "profiling"))]
pub struct Timer {}
#[cfg(not(feature = "profiling"))]
impl Timer {
    #[inline(always)]
    pub fn new() -> crate::Result<Self> {
        Ok(Self {})
    }
    #[inline(always)]
    pub fn detailed_events(&self) -> bool {
        false
    }
    #[inline(always)]
    pub fn mark(&mut self, _phase: &str) -> crate::Result<()> {
        Ok(())
    }
    #[inline(always)]
    pub fn mark_detailed(
        &mut self,
        _phase: &str,
        _detail: Option<&crate::profile::Breakdown>,
    ) -> crate::Result<()> {
        Ok(())
    }
}
