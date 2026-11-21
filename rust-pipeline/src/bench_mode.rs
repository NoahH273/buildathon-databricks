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
    let fast = |pos: usize| pos >= 16 && pos + 128 <= len;
    t("scan only", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            n = n.wrapping_add(pos as u64 ^ f[6].1 as u64);
        });
        n
    });
    t("+ ingest f1 fast", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            if fast(pos) {
                n = n.wrapping_add(unsafe { parse_f1_fast(data.as_ptr().add(pos + f[3].0 as usize), (f[3].1 - f[3].0) as usize) }.unwrap_or(1) as u64);
            }
        });
        n
    });
    t("+ ts fast", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            if fast(pos) {
                n = n.wrapping_add(unsafe { parse_ts_fast(data.as_ptr().add(pos + f[2].0 as usize), (f[2].1 - f[2].0) as usize) }.unwrap_or(1) as u64);
            }
        });
        n
    });
    t("+ product_num", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            if fast(pos) {
                n = n.wrapping_add(unsafe { product_num(data.as_ptr().add(pos + f[5].0 as usize), (f[5].1 - f[5].0) as usize) }.unwrap_or(1) as u64);
            }
        });
        n
    });
    t("+ hash eid+user", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            n = n.wrapping_add(hash(&data[pos + f[0].0 as usize..pos + f[0].1 as usize]));
            n = n.wrapping_add(hash(&data[pos + f[1].0 as usize..pos + f[1].1 as usize]));
        });
        n
    });
    t("+ ts_fast (branch-free)", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            if fast(pos) {
                n = n.wrapping_add(unsafe { ts_fast(data.as_ptr().add(pos + f[2].0 as usize), (f[2].1 - f[2].0) as usize) }.unwrap_or(1) as u64);
            }
        });
        n
    });
    t("+ hash_fast eid+user", &|| {
        let mut n = 0u64;
        scan_lines::<7>(&data, 0, len, |pos, f| {
            if fast(pos) {
                n = n.wrapping_add(unsafe { hash_fast(data.as_ptr().add(pos + f[0].0 as usize), (f[0].1 - f[0].0) as usize) });
                n = n.wrapping_add(unsafe { hash_fast(data.as_ptr().add(pos + f[1].0 as usize), (f[1].1 - f[1].0) as usize) });
            }
        });
        n
    });
    // equivalence checks
    let mut bad = 0;
    scan_lines::<7>(&data, 0, len, |pos, f| {
        if fast(pos) {
            for k in [0usize, 1, 5] {
                let sl = &data[pos + f[k].0 as usize..pos + f[k].1 as usize];
                if !sl.is_empty() && unsafe { hash_fast(sl.as_ptr(), sl.len()) } != hash(sl) { bad += 1; }
            }
            let sl = &data[pos + f[2].0 as usize..pos + f[2].1 as usize];
            if unsafe { ts_fast(sl.as_ptr(), sl.len()) } != parse_ts(sl) { bad += 1; }
            if unsafe { parse_ts_fast(sl.as_ptr(), sl.len()) } != parse_ts(sl) { bad += 1; }
        }
    });
    eprintln!("mismatches: {}", bad);
    black_box(&data);
}
