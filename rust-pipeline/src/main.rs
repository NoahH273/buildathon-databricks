mod bench_mode;
mod links;
mod parquet;
mod parse;
mod scatter;

use links::{LinkRec, Linked};
use memmap2::Mmap;
use parse::*;
use scatter::Scatter;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: u32 = 1_800_000;
const INVALID: u32 = u32::MAX;
const OFF_BITS: u32 = 40;
const OFF_MASK: u64 = (1u64 << OFF_BITS) - 1;
pub const LINKED_BIT: u64 = 1 << 63;
const B1: usize = 6; // staging depth, event_id partitions
const B2: usize = 8; // staging depth, person partitions
const RING: usize = 8; // link lookups in flight per parsing thread
const NO_PROD: u16 = u16::MAX;

/// Stage-1 event record, partitioned by event_id hash.
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct Ev1 {
    eid: u64,
    person: u64, // user hash until resolved
    name: u64,   // ref to the user's own ID, or LINKED_BIT | component
    ts: u32,     // relative ms, INVALID if the row fails validation
    ingest: i32,
    pc: u32, // price << 8 | category
}

/// Stage-2 event record, partitioned by person.
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct Ev2 {
    person: u64,
    name: u64,
    ts: u32,
    pc: u32,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct Cl {
    person: u64, // user hash until resolved
    cid: u64,    // ref to click_id bytes
    ts: u32,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Chunk {
    kind: u8, // 0 link, 1 click, 2 event
    fid: u32,
    start: usize,
    end: usize,
}

enum FileData {
    Map(Mmap),
    Mem(Vec<u8>),
}

struct Ctx {
    files: Vec<Option<FileData>>,
    base: i64, // window_start - 7 days, ms
    ev_lo: u32,
    ev_hi: u32,
    cl_hi: u32,
    products: U64Map,
    prod_arr: Vec<u16>,
    unknown_cat: u32,
    campaigns: U64Map,
    b1: u32,
    b2: u32,
}

impl Ctx {
    #[inline(always)]
    fn data(&self, fid: usize) -> &[u8] {
        match &self.files[fid] {
            Some(FileData::Map(m)) => &m[..],
            Some(FileData::Mem(v)) => &v[..],
            None => &[],
        }
    }
    /// Category index of a product id (`num` is its `product_num`).
    #[inline(always)]
    fn category(&self, num: Option<u32>, id: &[u8]) -> u32 {
        if let Some(n) = num {
            if (n as usize) < self.prod_arr.len() {
                let c = unsafe { *self.prod_arr.get_unchecked(n as usize) };
                return if c == NO_PROD { self.unknown_cat } else { c as u32 };
            }
        }
        self.products.get(hash(id)).unwrap_or(self.unknown_cat)
    }
    #[inline(always)]
    fn ref_ptr(&self, r: u64) -> *const u8 {
        let d = self.data((r >> OFF_BITS) as usize);
        unsafe { d.as_ptr().add((r & OFF_MASK) as usize) }
    }
    /// The field value a ref points at: bytes up to the next ',', '"', '\n' or EOF.
    #[inline]
    fn ref_str(&self, r: u64) -> &[u8] {
        let d = self.data((r >> OFF_BITS) as usize);
        let off = (r & OFF_MASK) as usize;
        let s = &d[off..];
        let mut e = 0;
        while e < s.len() {
            let c = s[e];
            if c == b',' || c == b'"' || c == b'\n' {
                break;
            }
            e += 1;
        }
        &s[..e]
    }
}

/// Ask Linux to back a large, not yet touched allocation with transparent huge pages, so
/// random probes into it do not miss the TLB. A no-op elsewhere.
pub fn advise_huge<T>(v: &Vec<T>) {
    #[cfg(target_os = "linux")]
    unsafe {
        const H: usize = 2 << 20;
        let p = v.as_ptr() as usize;
        let len = v.capacity() * std::mem::size_of::<T>();
        let s = (p + H - 1) & !(H - 1);
        let e = (p + len) & !(H - 1);
        if e > s {
            libc::madvise(s as *mut libc::c_void, e - s, libc::MADV_HUGEPAGE);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = v;
}

#[inline(always)]
fn prefetch(p: *const u8) {
    unsafe { std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0) };
}

fn env_or(name: &str, idx: usize, default: &str) -> PathBuf {
    if let Ok(v) = std::env::var(name) {
        if !v.is_empty() {
            return PathBuf::from(v);
        }
    }
    if let Some(a) = std::env::args().nth(idx) {
        return PathBuf::from(a);
    }
    PathBuf::from(default)
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).expect("open");
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return None;
    }
    let m = unsafe { Mmap::map(&f).expect("mmap") };
    Some(m)
}

/// Iterate the data lines of a small whole file (skipping the header).
fn small_lines(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut pos = match memchr::memchr(b'\n', data) {
        Some(p) => p + 1,
        None => data.len(),
    };
    std::iter::from_fn(move || {
        if pos >= data.len() {
            return None;
        }
        let e = memchr::memchr(b'\n', &data[pos..]).map(|x| pos + x).unwrap_or(data.len());
        let line = &data[pos..e];
        pos = e + 1;
        Some(line)
    })
}

fn json_str_field(s: &str, key: &str) -> String {
    let k = format!("\"{}\"", key);
    let i = s.find(&k).expect("manifest key") + k.len();
    let rest = &s[i..];
    let q1 = rest.find('"').unwrap() + 1;
    let q2 = rest[q1..].find('"').unwrap() + q1;
    rest[q1..q2].to_string()
}

/// Run `f(thread_index)` on `n` scoped threads and collect the results.
fn par<T: Send>(n: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    std::thread::scope(|s| {
        let f = &f;
        let hs: Vec<_> = (0..n).map(|t| s.spawn(move || f(t))).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn main() {
    if let Ok(p) = std::env::var("BENCH") {
        bench_mode::run(&p);
        return;
    }
    let t0 = Instant::now();
    let data_dir = env_or("DATA_DIR", 1, "/data");
    let out_dir = env_or("OUTPUT_DIR", 2, "/output");
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));

    // ---- manifest, products, campaigns ----
    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = parse_f1(json_str_field(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = parse_f1(json_str_field(&manifest, "window_end").as_bytes()).expect("window_end");
    let base = ws - WEEK_MS;
    assert!(we - base < INVALID as i64, "window too long");

    let prod_data = std::fs::read(data_dir.join("products.csv")).unwrap_or_default();
    let mut raw_cats: Vec<&[u8]> = Vec::new();
    let mut cat_ix: std::collections::HashMap<&[u8], u32> = std::collections::HashMap::new();
    let mut prod_rows: Vec<(u64, Option<u32>, u32)> = Vec::with_capacity(prod_data.len() / 16);
    for line in small_lines(&prod_data) {
        let mut f = [(0, 0); 2];
        if !split::<2>(line, &mut f) || f[0].0 == f[0].1 {
            continue;
        }
        let pid = &line[f[0].0..f[0].1];
        let c = &line[f[1].0..f[1].1];
        let ci = *cat_ix.entry(c).or_insert_with(|| {
            raw_cats.push(c);
            raw_cats.len() as u32 - 1
        });
        prod_rows.push((hash(pid), product_num_slice(pid), ci));
    }
    let mut cat_names: Vec<Vec<u8>> = raw_cats.iter().map(|c| c.to_vec()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    assert!(cat_names.len() <= 256, "too many categories");
    let cat_id: Vec<u32> =
        raw_cats.iter().map(|c| cat_names.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u32).collect();
    // `p<number>` ids index an array; any other id goes through the hash map
    const PROD_LIMIT: u32 = 1 << 22;
    let mut products = U64Map::with_capacity(prod_rows.len());
    let mut prod_arr: Vec<u16> = Vec::new();
    for &(ref h, ref num, ci) in &prod_rows {
        let id = cat_id[ci as usize];
        match num {
            Some(n) if *n < PROD_LIMIT => {
                let n = *n as usize;
                if prod_arr.len() <= n {
                    prod_arr.resize(n + 1, NO_PROD);
                }
                if prod_arr[n] == NO_PROD {
                    prod_arr[n] = id as u16;
                }
            }
            _ => {
                products.insert(*h, id);
            }
        }
    }
    let unknown_cat = cat_names.binary_search(&b"unknown".to_vec()).unwrap() as u32;

    let camp_data = std::fs::read(data_dir.join("campaigns.csv")).unwrap_or_default();
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    let mut campaigns = U64Map::with_capacity(4096);
    for line in small_lines(&camp_data) {
        let mut f = [(0, 0); 2];
        if !split::<2>(line, &mut f) || f[0].0 == f[0].1 {
            continue;
        }
        let id = &line[f[0].0..f[0].1];
        let idx = camp_ids.len() as u32;
        if campaigns.insert(hash(id), idx) == idx {
            camp_ids.push(id.to_vec());
            camp_channels.push(line[f[1].0..f[1].1].to_vec());
        }
    }
    let ncamp = camp_ids.len();
    let organic = ncamp as u32;

    // ---- map input files ----
    let mut files: Vec<Option<FileData>> = Vec::new();
    let load_mem = std::env::var("LOAD_MEM").is_ok();
    let mut link_chunks: Vec<Chunk> = Vec::new();
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut ev_bytes = 0usize;
    let mut cl_bytes = 0usize;
    const CHUNK: usize = 2 << 20;
    const LCHUNK: usize = 1 << 20;
    let mut paths: Vec<(u8, PathBuf)> = Vec::new();
    for (kind, sub) in [(0u8, "links"), (1, "clicks"), (2, "events")] {
        for p in list_csv(&data_dir.join(sub)) {
            paths.push((kind, p));
        }
    }
    let mut maps: Vec<Option<FileData>> = {
        let next = AtomicUsize::new(0);
        let mut got: Vec<(usize, Option<FileData>)> = par(nthreads, |_| {
            let mut v = Vec::new();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= paths.len() {
                    break;
                }
                let p = &paths[i].1;
                let m = if load_mem {
                    let d = std::fs::read(p).unwrap();
                    if d.is_empty() {
                        None
                    } else {
                        Some(FileData::Mem(d))
                    }
                } else {
                    map_file(p).map(FileData::Map)
                };
                v.push((i, m));
            }
            v
        })
        .into_iter()
        .flatten()
        .collect();
        got.sort_unstable_by_key(|x| x.0);
        got.into_iter().map(|x| x.1).collect()
    };
    for (i, &(kind, _)) in paths.iter().enumerate() {
        {
            let fid = files.len() as u32;
            let m = maps[i].take();
            let len = match &m {
                Some(FileData::Map(x)) => x.len(),
                Some(FileData::Mem(x)) => x.len(),
                None => 0,
            };
            files.push(m);
            match kind {
                1 => cl_bytes += len,
                2 => ev_bytes += len,
                _ => {}
            }
            let cs = if kind == 0 { LCHUNK } else { CHUNK };
            let mut s = 0;
            while s < len {
                let e = (s + cs).min(len);
                let c = Chunk { kind, fid, start: s, end: e };
                if kind == 0 {
                    link_chunks.push(c);
                } else {
                    chunks.push(c);
                }
                s = e;
            }
        }
    }
    let est_events = ev_bytes / 70 + 1;
    let est_clicks = cl_bytes / 45 + 1;
    let b1 = ((est_events / 80_000).max(16).next_power_of_two().trailing_zeros()).min(12);
    let b2 = ((est_events / 40_000).max(16).next_power_of_two().trailing_zeros()).min(11);
    let p1 = 1usize << b1;
    let p2 = 1usize << b2;

    let ctx = Ctx {
        files,
        base,
        ev_lo: (ws - base) as u32,
        ev_hi: (we - base) as u32,
        cl_hi: (we - base) as u32,
        products,
        prod_arr,
        unknown_cat,
        campaigns,
        b1,
        b2,
    };
    eprintln!(
        "setup {:?}: {} chunks, {} link chunks, p1={} p2={} threads={}",
        t0.elapsed(),
        chunks.len(),
        link_chunks.len(),
        p1,
        p2,
        nthreads
    );

    // ---- phase 1a: events (by event_id), with the linked-ID build running alongside ----
    let ev_chunks: Vec<Chunk> = chunks.iter().copied().filter(|c| c.kind == 2).collect();
    let cl_chunks: Vec<Chunk> = chunks.iter().copied().filter(|c| c.kind == 1).collect();
    let per_bucket = est_events / (p1 * nthreads) + 16;
    let cl_bucket = est_clicks / (p2 * nthreads) + 16;
    let (linked, ev_locals): (Linked, Vec<Vec<Vec<Ev1>>>) = std::thread::scope(|s| {
        let lh = s.spawn(|| {
            let mut out = Vec::new();
            for &c in &link_chunks {
                parse_links(&ctx, c, &mut out);
            }
            // half the threads: links finish well before events, and fewer workers fight the parsers
            let l = links::build(vec![out], (nthreads / 2).max(1), |r| ctx.ref_str(r));
            eprintln!("links done {:?}", t0.elapsed());
            l
        });
        let next = AtomicUsize::new(0);
        let ev = par(nthreads, |_| {
            let mut evb: Scatter<Ev1, B1> = Scatter::new(p1, per_bucket);
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= ev_chunks.len() {
                    break;
                }
                parse_events(&ctx, ev_chunks[i], &mut evb);
            }
            evb.finish()
        });
        eprintln!("events parsed {:?}", t0.elapsed());
        (lh.join().unwrap(), ev)
    });

    // ---- phase 1b: clicks (by person) ----
    let next = AtomicUsize::new(0);
    let cl_locals: Vec<Vec<Vec<Cl>>> = par(nthreads, |_| {
        let mut clb: Scatter<Cl, B2> = Scatter::new(p2, cl_bucket);
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= cl_chunks.len() {
                break;
            }
            parse_clicks(&ctx, &linked, cl_chunks[i], &mut clb);
        }
        clb.finish()
    });
    eprintln!("phase1 {:?}", t0.elapsed());

    // ---- phase 2: dedup events, repartition by person ----
    let next = AtomicUsize::new(0);
    let n_ev1: usize = ev_locals.iter().map(|l| l.iter().map(|b| b.len()).sum::<usize>()).sum();
    let ev2_cap = n_ev1 * 5 / 4 / (p2 * nthreads) + 32;
    // A record is addressed as (source thread << SRC_SHIFT) | index within that thread's bucket.
    const SRC_SHIFT: u32 = 27;
    assert!(nthreads < 32);
    let p2_events: Vec<Vec<Vec<Ev2>>> = par(nthreads, |_| {
        let mut evb: Scatter<Ev2, B2> = Scatter::new(p2, ev2_cap);
        let mut table: Vec<u32> = Vec::new();
        let mut keep: Vec<u8> = Vec::new();
        let mut srcs: Vec<&[Ev1]> = Vec::with_capacity(nthreads);
        let mut offs: Vec<usize> = Vec::with_capacity(nthreads + 1);
        let shift2 = 64 - ctx.b2;
        loop {
            let part = next.fetch_add(1, Ordering::Relaxed);
            if part >= p1 {
                break;
            }
            srcs.clear();
            offs.clear();
            let mut n = 0usize;
            for l in ev_locals.iter() {
                offs.push(n);
                n += l[part].len();
                srcs.push(&l[part][..]);
            }
            if n == 0 {
                continue;
            }
            let cap = (n * 2).next_power_of_two();
            let mask = cap - 1;
            table.clear();
            table.resize(cap, u32::MAX);
            let get = |id: u32| -> &Ev1 {
                unsafe { srcs.get_unchecked((id >> SRC_SHIFT) as usize).get_unchecked((id & ((1 << SRC_SHIFT) - 1)) as usize) }
            };
            // keep[flat index] = 1 while that record is its event_id's current winner
            keep.clear();
            keep.resize(n, 0);
            for (t, src) in srcs.iter().enumerate() {
                let base = offs[t];
                for (i, r) in src.iter().enumerate() {
                    let id = ((t as u32) << SRC_SHIFT) | i as u32;
                    let h = r.eid;
                    let mut slot = (h as usize) & mask;
                    loop {
                        let cur = unsafe { *table.get_unchecked(slot) };
                        if cur == u32::MAX {
                            table[slot] = id;
                            unsafe { *keep.get_unchecked_mut(base + i) = 1 };
                            break;
                        }
                        let other = get(cur);
                        if other.eid == h {
                            if r.ingest < other.ingest {
                                table[slot] = id;
                                let old = offs[(cur >> SRC_SHIFT) as usize] + (cur & ((1 << SRC_SHIFT) - 1)) as usize;
                                unsafe {
                                    *keep.get_unchecked_mut(old) = 0;
                                    *keep.get_unchecked_mut(base + i) = 1;
                                }
                            }
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
            for (t, src) in srcs.iter().enumerate() {
                let kp = &keep[offs[t]..offs[t] + src.len()];
                // resolve linked IDs here, prefetching the link table a few records ahead
                const AHEAD: usize = 8;
                for (i, (r, &k)) in src.iter().zip(kp.iter()).enumerate() {
                    if let Some(f) = src.get(i + 2 * AHEAD) {
                        linked.map.prefetch_filter(f.person);
                    }
                    if let Some(f) = src.get(i + AHEAD) {
                        linked.map.prefetch(f.person);
                    }
                    if k == 0 || r.ts == INVALID {
                        continue;
                    }
                    let (person, name) = linked.map.resolve(r.person, r.name);
                    evb.push((person >> shift2) as usize, Ev2 { person, name, ts: r.ts, pc: r.pc });
                }
            }
        }
        evb.finish()
    });
    eprintln!("phase2 {:?}", t0.elapsed());

    // ---- phase 3: sessionize, attribute, aggregate ----
    let ws_day = ws.div_euclid(DAY_MS);
    let nd = ((we - 1).div_euclid(DAY_MS) - ws_day + 1).max(1) as usize;
    let mut order: Vec<(usize, usize)> = (0..p2)
        .map(|q| (p2_events.iter().map(|l| l[q].len()).sum::<usize>(), q))
        .collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let next = AtomicUsize::new(0);
    let ud_dir = out_dir.join("user_daily");
    let cd_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).expect("mkdir");
    std::fs::create_dir_all(&cd_dir).expect("mkdir");
    let ncat = cat_names.len();
    let results: Vec<(Vec<[u64; 4]>, usize)> = par(nthreads, |tid| {
        let mut st = P3State::new(&linked.names, ncamp + 1, nd, ncat, ctx.base, ws_day, organic);
        let mut evs: Vec<Ev2> = Vec::new();
        let mut cls: Vec<Cl> = Vec::new();
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= order.len() {
                break;
            }
            let q = order[i].1;
            let ev_srcs: Vec<&[Ev2]> = p2_events.iter().map(|l| &l[q][..]).collect();
            let cl_srcs: Vec<&[Cl]> = cl_locals.iter().map(|l| &l[q][..]).collect();
            st.process_partition(&ctx, &ev_srcs, &cl_srcs, &mut evs, &mut cls);
        }
        let rows = st.rows;
        let stats = std::mem::take(&mut st.stats);
        if rows > 0 {
            st.write_user_daily(&cat_names, &ud_dir.join(format!("part-{:05}.parquet", tid)));
        }

        (stats, rows)
    });
    eprintln!("phase3 {:?}", t0.elapsed());

    let total_rows: usize = results.iter().map(|r| r.1).sum();
    if total_rows == 0 {
        let st = P3State::new(&[], 1, 1, 1, 0, 0, 0);
        st.write_user_daily(&cat_names, &ud_dir.join("part-empty.parquet"));
    }

    // ---- campaign_daily ----
    let mut stats = vec![[0u64; 4]; (ncamp + 1) * nd];
    for (s, _) in &results {
        for (a, b) in stats.iter_mut().zip(s.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    let mut c_id = Vec::new();
    let mut c_ch = Vec::new();
    let mut c_day = Vec::new();
    let mut c_s = Vec::new();
    let mut c_p = Vec::new();
    let mut c_r = Vec::new();
    let mut c_a = Vec::new();
    let mut n = 0;
    for c in 0..=ncamp {
        for d in 0..nd {
            let x = stats[c * nd + d];
            if x[0] == 0 && x[3] == 0 {
                continue;
            }
            let (id, ch): (&[u8], &[u8]) =
                if c == ncamp { (b"organic", b"organic") } else { (&camp_ids[c], &camp_channels[c]) };
            parquet::put_plain_str(&mut c_id, id);
            parquet::put_plain_str(&mut c_ch, ch);
            c_day.push((ws_day + d as i64) as i32);
            c_s.push(x[0] as i64);
            c_p.push(x[1] as i64);
            c_r.push(x[2] as i64);
            c_a.push(x[3] as i64);
            n += 1;
        }
    }
    let cols = vec![
        parquet::Column::strings("campaign_id", c_id),
        parquet::Column::strings("channel", c_ch),
        parquet::Column::dates("day", &c_day),
        parquet::Column::i64s("sessions", &c_s),
        parquet::Column::i64s("purchasing_sessions", &c_p),
        parquet::Column::i64s("revenue_cents", &c_r),
        parquet::Column::i64s("assisted_sessions", &c_a),
    ];
    parquet::write_file(&cd_dir.join("part-00000.parquet"), n, &cols).expect("write campaign_daily");
    eprintln!("done {:?}: user rows {}, campaign rows {}", t0.elapsed(), total_rows, n);
    // Skip destructors of large structures.
    std::process::exit(0);
}

// ---------------------------------------------------------------------------
// Phase 0/1 parsers

#[inline(always)]
fn fld<'a>(data: &'a [u8], pos: usize, r: (u32, u32)) -> &'a [u8] {
    unsafe { data.get_unchecked(pos + r.0 as usize..pos + r.1 as usize) }
}

fn parse_links(ctx: &Ctx, c: Chunk, out: &mut Vec<LinkRec>) {
    let data = ctx.data(c.fid as usize);
    let fbase = (c.fid as u64) << OFF_BITS;
    scan_lines::<2>(data, c.start, c.end, |pos, f| {
        let a = fld(data, pos, f[0]);
        let b = fld(data, pos, f[1]);
        if a.is_empty() || b.is_empty() || a == b {
            return;
        }
        out.push(LinkRec {
            ha: hash(a),
            hb: hash(b),
            ra: fbase | (pos + f[0].0 as usize) as u64,
            rb: fbase | (pos + f[1].0 as usize) as u64,
            pa: links::prefix8(a),
            pb: links::prefix8(b),
        });
    });
}

/// A small ring of records whose link lookups were prefetched; the oldest is resolved
/// once the ring is full, by which time its table slot has usually arrived in cache.
struct Ring<T: Copy> {
    buf: [T; RING],
    n: usize,
}

impl<T: Copy> Ring<T> {
    fn new() -> Self {
        Ring { buf: unsafe { std::mem::zeroed() }, n: 0 }
    }
    #[inline(always)]
    fn push(&mut self, x: T, mut done: impl FnMut(T)) {
        let i = self.n & (RING - 1);
        if self.n >= RING {
            done(self.buf[i]);
        }
        self.buf[i] = x;
        self.n += 1;
    }
    fn drain(&mut self, mut done: impl FnMut(T)) {
        for k in self.n.saturating_sub(RING)..self.n {
            done(self.buf[k & (RING - 1)]);
        }
        self.n = 0;
    }
}

fn parse_clicks(ctx: &Ctx, linked: &Linked, c: Chunk, out: &mut Scatter<Cl, B2>) {
    let data = ctx.data(c.fid as usize);
    let fbase = (c.fid as u64) << OFF_BITS;
    let base = ctx.base;
    let hi = ctx.cl_hi as i64;
    let shift2 = 64 - ctx.b2;
    let mut ring: Ring<Cl> = Ring::new();
    let mut done = |mut x: Cl| {
        x.person = linked.map.resolve(x.person, 0).0;
        out.push((x.person >> shift2) as usize, x);
    };
    scan_lines::<4>(data, c.start, c.end, |pos, f| {
        if f[0].0 == f[0].1 || f[1].0 == f[1].1 {
            return;
        }
        let fast = pos >= 16 && pos + 128 <= data.len();
        let fp = |k: usize| unsafe { data.as_ptr().add(pos + f[k].0 as usize) };
        let fl = |k: usize| (f[k].1 - f[k].0) as usize;
        let tsr = if fast { unsafe { parse_ts_fast(fp(3), fl(3)) } } else { parse_ts(fld(data, pos, f[3])) };
        let ts = match tsr {
            Some(t) => t - base,
            None => return,
        };
        if ts < 0 || ts >= hi {
            return;
        }
        let ch = if fast { unsafe { hash_fast(fp(2), fl(2)) } } else { hash(fld(data, pos, f[2])) };
        let camp = match ctx.campaigns.get(ch) {
            Some(x) => x,
            None => return,
        };
        let user = if fast { unsafe { hash_fast(fp(1), fl(1)) } } else { hash(fld(data, pos, f[1])) };
        linked.map.prefetch(user);
        ring.push(Cl { person: user, cid: fbase | (pos + f[0].0 as usize) as u64, ts: ts as u32, camp }, &mut done);
    });
    ring.drain(&mut done);
}

const W_VIEW: u32 = u32::from_le_bytes(*b"view");
const W_CART: u32 = u32::from_le_bytes(*b"cart");
const W_PURCHASE: u64 = u64::from_le_bytes(*b"purchase");

fn parse_events(ctx: &Ctx, c: Chunk, out: &mut Scatter<Ev1, B1>) {
    let data = ctx.data(c.fid as usize);
    let fbase = (c.fid as u64) << OFF_BITS;
    let base = ctx.base;
    let lo = ctx.ev_lo as i64;
    let hi = ctx.ev_hi as i64;
    let shift = 64 - ctx.b1;
    let base_s = base.div_euclid(1000);
    scan_lines::<7>(data, c.start, c.end, |pos, f| {
        if f[0].0 == f[0].1 {
            return;
        }
        // the SIMD field parsers may read up to 16 bytes before a field and 25 after its start
        if pos >= 16 && pos + 128 <= data.len() {
            let fp = |k: usize| unsafe { data.as_ptr().add(pos + f[k].0 as usize) };
            let fl = |k: usize| (f[k].1 - f[k].0) as usize;
            let eid = unsafe { hash_fast(fp(0), fl(0)) };
            let ingest_ms = unsafe { parse_f1_fast(fp(3), fl(3)) }.unwrap_or(i64::MAX / 4);
            let ingest = (ingest_ms.div_euclid(1000) - base_s).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
            // validity without data-dependent branches: event types and timestamp forms are
            // mixed row by row, so branching on them would mispredict
            let tn = fl(4);
            let tw = unsafe { (fp(4) as *const u64).read_unaligned() };
            let four = (tn == 4) & (((tw as u32) == W_VIEW) | ((tw as u32) == W_CART));
            let purchase = (tn == 8) & (tw == W_PURCHASE);
            let tsr = unsafe { parse_ts_fast(fp(2), fl(2)) };
            let ts_abs = tsr.unwrap_or(i64::MIN / 4);
            let ts = ts_abs - base;
            let mut valid = (f[1].0 != f[1].1)
                & (f[5].0 != f[5].1)
                & (four | purchase)
                & tsr.is_some()
                & (ts >= lo)
                & (ts < hi)
                & (ingest_ms - ts_abs <= DAY_MS);
            let mut price = 0u32;
            if purchase {
                match unsafe { parse_price_fast(fp(6), fl(6)) } {
                    Some(p) => price = p,
                    None => valid = false,
                }
            }
            if valid {
                let num = unsafe { product_num(fp(5), fl(5)) };
                let cat = ctx.category(num, fld(data, pos, f[5]));
                let user = unsafe { hash_fast(fp(1), fl(1)) };
                let rec = Ev1 {
                    eid,
                    person: user,
                    name: fbase | (pos + f[1].0 as usize) as u64,
                    ts: ts as u32,
                    ingest,
                    pc: (price << 8) | cat,
                };
                out.push((eid >> shift) as usize, rec);
            } else {
                // invalid rows still take part in dedup; they need no person
                out.push((eid >> shift) as usize, Ev1 { eid, person: 0, name: 0, ts: INVALID, ingest, pc: 0 });
            }
            return;
        }
        let eid = hash(fld(data, pos, f[0]));
        let ingest_ms = parse_f1(fld(data, pos, f[3])).unwrap_or(i64::MAX / 4);
        let ingest = (ingest_ms.div_euclid(1000) - base_s).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        let mut rec = Ev1 { eid, person: 0, name: 0, ts: INVALID, ingest, pc: 0 };
        'v: {
            if f[1].0 == f[1].1 || f[5].0 == f[5].1 {
                break 'v;
            }
            let et = fld(data, pos, f[4]);
            let purchase = if et.len() == 4 {
                let w = u32::from_le_bytes([et[0], et[1], et[2], et[3]]);
                if w != W_VIEW && w != W_CART {
                    break 'v;
                }
                false
            } else if et.len() == 8 {
                let w = u64::from_le_bytes([et[0], et[1], et[2], et[3], et[4], et[5], et[6], et[7]]);
                if w != W_PURCHASE {
                    break 'v;
                }
                true
            } else {
                break 'v;
            };
            let ts_abs = match parse_ts(fld(data, pos, f[2])) {
                Some(t) => t,
                None => break 'v,
            };
            let ts = ts_abs - base;
            if ts < lo || ts >= hi {
                break 'v;
            }
            if ingest_ms - ts_abs > DAY_MS {
                break 'v;
            }
            let mut price = 0u32;
            if purchase {
                price = match parse_price(fld(data, pos, f[6])) {
                    Some(p) => p,
                    None => break 'v,
                };
            }
            let pid = fld(data, pos, f[5]);
            let cat = ctx.category(product_num_slice(pid), pid);
            rec.person = hash(fld(data, pos, f[1]));
            rec.name = fbase | (pos + f[1].0 as usize) as u64;
            rec.ts = ts as u32;
            rec.pc = (price << 8) | cat;
        }
        out.push((eid >> shift) as usize, rec);
    });
}

// ---------------------------------------------------------------------------
// Phase 3

/// Gather `srcs` into `v` sorted by (person, ts): LSD radix on ts bits 11..33 (two 11-bit
/// digits), then a stable pass on the person bits below the partition bits. People sharing a
/// bucket are separated by a stable sort on person, and a last insertion pass orders the rare
/// same-person events that fall in one 2 s step.
fn radix_sort<T: Copy>(
    srcs: &[&[T]],
    v: &mut Vec<T>,
    tmp: &mut Vec<T>,
    hist: &mut Vec<u32>,
    b2: u32,
    person: impl Fn(&T) -> u64,
    ts: impl Fn(&T) -> u32,
) {
    let n: usize = srcs.iter().map(|s| s.len()).sum();
    v.clear();
    if n < 64 {
        for s in srcs {
            v.extend_from_slice(s);
        }
        v.sort_unstable_by(|a, b| person(a).cmp(&person(b)).then_with(|| ts(a).cmp(&ts(b))));
        return;
    }
    let kb = ((n / 4).next_power_of_two().trailing_zeros()).clamp(1, 16);
    let k = 1usize << kb;
    let shift = 64 - b2 - kb;
    let bucket = |x: &T| ((person(x) >> shift) as usize) & (k - 1);
    const D: usize = 2048;
    hist.clear();
    hist.resize(2 * D + k, 0);
    let (h1, rest) = hist.split_at_mut(D);
    let (h2, hb) = rest.split_at_mut(D);
    for s in srcs {
        for x in s.iter() {
            let t = ts(x) as usize;
            unsafe {
                *h1.get_unchecked_mut((t >> 11) & (D - 1)) += 1;
                *h2.get_unchecked_mut((t >> 22) & (D - 1)) += 1;
                *hb.get_unchecked_mut(bucket(x)) += 1;
            }
        }
    }
    for h in [&mut *h1, &mut *h2, &mut *hb] {
        let mut acc = 0u32;
        for c in h.iter_mut() {
            let t = *c;
            *c = acc;
            acc += t;
        }
    }
    v.reserve(n);
    tmp.reserve(n);
    unsafe {
        v.set_len(n);
        tmp.set_len(n);
        let (vp, tp) = (v.as_mut_ptr(), tmp.as_mut_ptr());
        for s in srcs {
            for x in s.iter() {
                let p = h1.get_unchecked_mut((ts(x) as usize >> 11) & (D - 1));
                *vp.add(*p as usize) = *x;
                *p += 1;
            }
        }
        for i in 0..n {
            let x = &*vp.add(i);
            let p = h2.get_unchecked_mut((ts(x) as usize >> 22) & (D - 1));
            *tp.add(*p as usize) = *x;
            *p += 1;
        }
        for i in 0..n {
            let x = &*tp.add(i);
            let p = hb.get_unchecked_mut(bucket(x));
            *vp.add(*p as usize) = *x;
            *p += 1;
        }
    }
    // hb[b] is now the end of bucket b
    let mut start = 0usize;
    for b in 0..k {
        let end = hb[b] as usize;
        if end - start > 1 {
            let s = &mut v[start..end];
            let p0 = person(&s[0]);
            if s.iter().any(|x| person(x) != p0) {
                s.sort_by_key(|x| person(x));
            }
        }
        start = end;
    }
    for i in 1..n {
        if ts(&v[i]) < ts(&v[i - 1]) && person(&v[i]) == person(&v[i - 1]) {
            let x = v[i];
            let (px, tx) = (person(&x), ts(&x));
            let mut j = i;
            while j > 0 && person(&v[j - 1]) == px && ts(&v[j - 1]) > tx {
                v[j] = v[j - 1];
                j -= 1;
            }
            v[j] = x;
        }
    }
}

#[derive(Clone, Copy)]
struct Sess {
    start: u32,
    day: u32,
    rev: u64,
    purch: bool,
}

struct P3State<'a> {
    link_names: &'a [u64],
    nd: usize,
    base: i64,
    ws_day: i64,
    organic: u32,
    stats: Vec<[u64; 4]>,
    catsum: Vec<u64>,
    touched: Vec<u32>,
    sess: Vec<Sess>,
    starts: Vec<u32>,
    daygroups: Vec<(u32, u32)>, // (day idx, first session index), then an end sentinel
    sgroup: Vec<u32>,           // day group of each session
    bounds: Vec<u32>,
    win: Vec<(u64, u64)>, // open assist window per campaign, (MAX, 0) when none
    wfrom: Vec<u32>,      // per campaign: first session index not yet passed
    wtouched: Vec<u32>,
    stamp: Vec<u32>, // per campaign: last session (by generation) it was counted for
    stamp_gen: u32,
    counts: Vec<u32>,
    tmp_ev: Vec<Ev2>,
    tmp_cl: Vec<Cl>,
    // user_daily columns
    rows: usize,
    names: Vec<u8>,
    days: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    topcat: Vec<i32>,
}

impl<'a> P3State<'a> {
    fn new(
        link_names: &'a [u64],
        ncampx: usize,
        nd: usize,
        ncat: usize,
        base: i64,
        ws_day: i64,
        organic: u32,
    ) -> P3State<'a> {
        P3State {
            link_names,
            nd,
            base,
            ws_day,
            organic,
            stats: vec![[0; 4]; ncampx * nd],
            catsum: vec![0; ncat],
            touched: Vec::new(),
            sess: Vec::new(),
            starts: Vec::new(),
            daygroups: Vec::new(),
            sgroup: Vec::new(),
            bounds: Vec::new(),
            win: vec![(u64::MAX, 0); ncampx],
            wfrom: vec![0; ncampx],
            wtouched: Vec::new(),
            stamp: vec![0; ncampx],
            stamp_gen: 0,
            counts: Vec::new(),
            tmp_ev: Vec::new(),
            tmp_cl: Vec::new(),
            rows: 0,
            names: Vec::new(),
            days: Vec::new(),
            sessions: Vec::new(),
            events: Vec::new(),
            revenue: Vec::new(),
            topcat: Vec::new(),
        }
    }

    fn process_partition(
        &mut self,
        ctx: &Ctx,
        ev_srcs: &[&[Ev2]],
        cl_srcs: &[&[Cl]],
        evs: &mut Vec<Ev2>,
        cls: &mut Vec<Cl>,
    ) {
        radix_sort(ev_srcs, evs, &mut self.tmp_ev, &mut self.counts, ctx.b2, |e| e.person, |e| e.ts);
        radix_sort(cl_srcs, cls, &mut self.tmp_cl, &mut self.counts, ctx.b2, |c| c.person, |c| c.ts);
        let n = evs.len();
        // person boundaries, so that name lookups can be prefetched a few people ahead
        self.bounds.clear();
        let mut prev = 0u64;
        for (k, e) in evs.iter().enumerate() {
            let p = e.person;
            if k == 0 || p != prev {
                self.bounds.push(k as u32);
            }
            prev = p;
        }
        self.bounds.push(n as u32);
        let np = self.bounds.len() - 1;
        let mut ci = 0;
        for pi in 0..np {
            if pi + 8 < np {
                let nw = evs[self.bounds[pi + 8] as usize].name;
                if nw & LINKED_BIT != 0 {
                    prefetch(unsafe { self.link_names.as_ptr().add((nw & !LINKED_BIT) as usize) } as *const u8);
                }
            }
            if pi + 4 < np {
                let r = self.name_ref(evs[self.bounds[pi + 4] as usize].name);
                prefetch(ctx.ref_ptr(r));
            }
            let (i, j) = (self.bounds[pi] as usize, self.bounds[pi + 1] as usize);
            let p = evs[i].person;
            while ci < cls.len() && cls[ci].person < p {
                ci += 1;
            }
            let mut ck = ci;
            while ck < cls.len() && cls[ck].person == p {
                ck += 1;
            }
            self.process_person(ctx, &evs[i..j], &cls[ci..ck]);
            ci = ck;
        }
    }

    fn flush_day(&mut self, name: &[u8], day: u32, s: i64, e: i64, r: i64) {
        let mut best: i32 = -1;
        let mut bestv = 0u64;
        for &c in &self.touched {
            let v = self.catsum[c as usize];
            if best < 0 || v > bestv || (v == bestv && (c as i32) < best) {
                best = c as i32;
                bestv = v;
            }
            self.catsum[c as usize] = 0;
        }
        self.touched.clear();
        parquet::put_plain_str(&mut self.names, name);
        self.days.push((self.ws_day + day as i64) as i32);
        self.sessions.push(s);
        self.events.push(e);
        self.revenue.push(r);
        self.topcat.push(best);
        self.rows += 1;
    }

    #[inline(always)]
    fn name_ref(&self, w: u64) -> u64 {
        if w & LINKED_BIT != 0 {
            unsafe { *self.link_names.get_unchecked((w & !LINKED_BIT) as usize) }
        } else {
            w
        }
    }

    fn process_person(&mut self, ctx: &Ctx, ev: &[Ev2], cl: &[Cl]) {
        let name = ctx.ref_str(self.name_ref(ev[0].name));
        self.sess.clear();
        let mut k = 0;
        let n = ev.len();
        let mut cur_day = u32::MAX;
        let (mut ds, mut de, mut dr) = (0i64, 0i64, 0i64);
        while k < n {
            let start = ev[k].ts;
            let day = ((self.base + start as i64).div_euclid(DAY_MS) - self.ws_day) as u32;
            if day != cur_day {
                if cur_day != u32::MAX {
                    self.flush_day(name, cur_day, ds, de, dr);
                }
                cur_day = day;
                ds = 0;
                de = 0;
                dr = 0;
            }
            let mut last = start;
            let mut rev = 0u64;
            let mut m = k;
            while m < n {
                let t = ev[m].ts;
                if t - last > GAP_MS {
                    break;
                }
                let pc = ev[m].pc;
                let price = (pc >> 8) as u64;
                if price > 0 {
                    rev += price;
                    let c = (pc & 0xFF) as usize;
                    if self.catsum[c] == 0 {
                        self.touched.push(c as u32);
                    }
                    self.catsum[c] += price;
                }
                last = t;
                m += 1;
            }
            ds += 1;
            de += (m - k) as i64;
            dr += rev as i64;
            self.sess.push(Sess { start, day, rev, purch: rev > 0 });
            k = m;
        }
        self.flush_day(name, cur_day, ds, de, dr);

        // credit
        let nd = self.nd;
        let mut hi = 0;
        let (mut last_hi, mut last_camp) = (0usize, 0u32);
        for s in self.sess.iter() {
            while hi < cl.len() && cl[hi].ts <= s.start {
                hi += 1;
            }
            let mut camp = self.organic;
            if hi > 0 && (s.start - cl[hi - 1].ts) as i64 <= WEEK_MS {
                if hi != last_hi {
                    // latest click; same-ms ties go to the smallest click_id
                    let t = cl[hi - 1].ts;
                    let mut best = hi - 1;
                    let mut kk = hi - 1;
                    while kk > 0 && cl[kk - 1].ts == t {
                        kk -= 1;
                        if ctx.ref_str(cl[kk].cid) < ctx.ref_str(cl[best].cid) {
                            best = kk;
                        }
                    }
                    last_hi = hi;
                    last_camp = cl[best].camp;
                }
                camp = last_camp;
            }
            let x = &mut self.stats[camp as usize * nd + s.day as usize];
            x[0] += 1;
            x[1] += s.purch as u64;
            x[2] += s.rev;
        }

        // assists
        if cl.is_empty() {
            return;
        }
        if self.sess.len() * cl.len() <= 4096 {
            // few sessions and clicks: slide the 7-day click window along the sessions and
            // count each campaign once per session
            let (mut lo, mut hi) = (0usize, 0usize);
            for s in self.sess.iter() {
                let t = s.start as u64;
                while hi < cl.len() && cl[hi].ts as u64 <= t {
                    hi += 1;
                }
                while lo < hi && cl[lo].ts as u64 + (WEEK_MS as u64) < t {
                    lo += 1;
                }
                self.stamp_gen = self.stamp_gen.wrapping_add(1);
                if self.stamp_gen == 0 {
                    self.stamp.iter_mut().for_each(|x| *x = 0);
                    self.stamp_gen = 1;
                }
                let g = self.stamp_gen;
                for c in &cl[lo..hi] {
                    let camp = c.camp as usize;
                    if self.stamp[camp] != g {
                        self.stamp[camp] = g;
                        self.stats[camp * nd + s.day as usize][3] += 1;
                    }
                }
            }
            return;
        }
        // otherwise: union of [click, click + 7d] per campaign, count sessions starting inside
        self.starts.clear();
        self.daygroups.clear();
        self.sgroup.clear();
        for (idx, s) in self.sess.iter().enumerate() {
            self.starts.push(s.start);
            if self.daygroups.last().map(|g| g.0) != Some(s.day) {
                self.daygroups.push((s.day, idx as u32));
            }
            self.sgroup.push(self.daygroups.len() as u32 - 1);
        }
        let nsess = self.sess.len() as u32;
        self.daygroups.push((u32::MAX, nsess));
        // clicks arrive in ts order: grow one open window per campaign, flush it on a gap
        for c in cl.iter() {
            let (camp, t) = (c.camp as usize, c.ts as u64);
            let w = self.win[camp];
            if w.0 == u64::MAX {
                self.wtouched.push(camp as u32);
                self.wfrom[camp] = 0;
            } else if t <= w.1 + 1 {
                self.win[camp].1 = t + WEEK_MS as u64;
                continue;
            } else {
                self.flush_window(camp, w.0, w.1);
            }
            self.win[camp] = (t, t + WEEK_MS as u64);
        }
        for k in 0..self.wtouched.len() {
            let camp = self.wtouched[k] as usize;
            let w = self.win[camp];
            self.flush_window(camp, w.0, w.1);
            self.win[camp] = (u64::MAX, 0);
        }
        self.wtouched.clear();
    }

    /// Count the sessions starting in [l, r] as assisted by `camp`, per day.
    #[inline]
    fn flush_window(&mut self, camp: usize, l: u64, r: u64) {
        let from = self.wfrom[camp] as usize;
        let a = from + self.starts[from..].partition_point(|&x| (x as u64) < l);
        let b = a + self.starts[a..].partition_point(|&x| (x as u64) <= r);
        self.wfrom[camp] = b as u32;
        if a < b {
            let nd = self.nd;
            let mut gi = self.sgroup[a] as usize;
            loop {
                let (day, first) = self.daygroups[gi];
                let end = self.daygroups[gi + 1].1 as usize;
                let n = b.min(end) - a.max(first as usize);
                self.stats[camp * nd + day as usize][3] += n as u64;
                if end >= b {
                    break;
                }
                gi += 1;
            }
        }
    }

    fn write_user_daily(self, cat_names: &[Vec<u8>], path: &Path) {
        let mut defs = Vec::with_capacity(self.rows);
        let mut tc = Vec::new();
        for &c in &self.topcat {
            if c >= 0 {
                defs.push(true);
                parquet::put_plain_str(&mut tc, &cat_names[c as usize]);
            } else {
                defs.push(false);
            }
        }
        let cols = vec![
            parquet::Column::strings("user_id", self.names),
            parquet::Column::dates("day", &self.days),
            parquet::Column::i64s("sessions", &self.sessions),
            parquet::Column::i64s("events", &self.events),
            parquet::Column::i64s("revenue_cents", &self.revenue),
            parquet::Column::opt_strings("top_category", &defs, &tc),
        ];
        parquet::write_file(path, self.rows, &cols).expect("write user_daily");
    }
}
