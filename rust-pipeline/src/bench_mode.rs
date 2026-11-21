// Micro-benchmarks of parse stages on one file held in memory. Run with BENCH=<path to csv>.
use crate::parse::*;
use std::hint::black_box;
use std::time::Instant;

pub fn run(path: &str) {
    let data = std::fs::read(path).unwrap();
    let len = data.len();
    let mb = len as f64 / 1e6;
    let t = |name: &str, f: &dyn Fn() -> u64| {
        let mut best = f64::MAX;
        let mut r = 0;
        for _ in 0..3 {
            let s = Instant::now();
            r = f();
            best = best.min(s.elapsed().as_secs_f64());
        }
        eprintln!("{:<28} {:>7.0} MB/s  ({})", name, mb / best, r);
    };
    t("memchr lines", &|| {
        let mut n = 0u64;
        let mut p = 0;
        while let Some(x) = memchr::memchr(b'\n', &data[p..]) {
            p += x + 1;
            n += 1;
        }
        n
    });
    t("scan_lines<7>", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            n += (pos as u64) ^ f[3].0 as u64;
        });
        n
    });
    t("+ hash eid", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            n ^= hash(&data[pos + f[0].0 as usize..pos + f[0].1 as usize]);
        });
        n
    });
    t("+ hash eid,user,prod", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            n ^= hash(&data[pos + f[0].0 as usize..pos + f[0].1 as usize]);
            n ^= hash(&data[pos + f[1].0 as usize..pos + f[1].1 as usize]);
            n ^= hash(&data[pos + f[5].0 as usize..pos + f[5].1 as usize]);
        });
        n
    });
    t("ts + ingest only", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            n ^= parse_ts(&data[pos + f[2].0 as usize..pos + f[2].1 as usize]).unwrap_or(1) as u64;
            n ^= parse_f1(&data[pos + f[3].0 as usize..pos + f[3].1 as usize]).unwrap_or(1) as u64;
        });
        n
    });
    black_box(&data);
}
