mod bench_mode;
mod links;
mod parquet;
mod parse;
mod scatter;

use links::{LinkRec, Linked};
use memmap2::Mmap;
use parse::*;
use scatter::Scatter;
use std::cmp::Ordering as CmpOrd;
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
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut prod_rows: Vec<(u64, Vec<u8>)> = Vec::new();
    for line in small_lines(&prod_data) {
        let mut f = [(0, 0); 2];
        if !split::<2>(line, &mut f) || f[0].0 == f[0].1 {
            continue;
        }
        prod_rows.push((hash(&line[f[0].0..f[0].1]), line[f[1].0..f[1].1].to_vec()));
        cat_names.push(line[f[1].0..f[1].1].to_vec());
    }
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    assert!(cat_names.len() <= 256, "too many categories");
    let mut products = U64Map::with_capacity(prod_rows.len());
    for (h, c) in &prod_rows {
        let id = cat_names.binary_search(c).unwrap() as u32;
        products.insert(*h, id);
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
    const CHUNK: usize = 4 << 20;
    const LCHUNK: usize = 1 << 20;
    for (kind, sub) in [(0u8, "links"), (1, "clicks"), (2, "events")] {
        for p in list_csv(&data_dir.join(sub)) {
            let fid = files.len() as u32;
            let m = if load_mem {
                let v = std::fs::read(&p).unwrap();
                if v.is_empty() {
                    None
                } else {
                    Some(FileData::Mem(v))
                }
            } else {
                map_file(&p).map(FileData::Map)
            };
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
    let b1 = ((est_events / 40_000).max(16).next_power_of_two().trailing_zeros()).min(12);
    let b2 = ((est_events / 80_000).max(16).next_power_of_two().trailing_zeros()).min(9);
    let p1 = 1usize << b1;
    let p2 = 1usize << b2;

    let ctx = Ctx {
        files,
        base,
        ev_lo: (ws - base) as u32,
        ev_hi: (we - base) as u32,
        cl_hi: (we - base) as u32,
        products,
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

    // ---- phase 0: links ----
    let next = AtomicUsize::new(0);
    let link_recs: Vec<Vec<LinkRec>> = par(nthreads, |_| {
        let mut out = Vec::new();
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= link_chunks.len() {
                break;
            }
            parse_links(&ctx, link_chunks[i], &mut out);
        }
        out
    });
    let linked: Linked = links::build(link_recs, nthreads, |r| ctx.ref_str(r));
    eprintln!("phase0 {:?}", t0.elapsed());

    // ---- phase 1: parse events (by event_id) and clicks (by person) ----
    let next = AtomicUsize::new(0);
    let per_bucket = est_events / (p1 * nthreads) + 16;
    let cl_bucket = est_clicks / (p2 * nthreads) + 16;
    let p1_locals: Vec<(Vec<Vec<Ev1>>, Vec<Vec<Cl>>)> = par(nthreads, |_| {
        let mut evb: Scatter<Ev1, B1> = Scatter::new(p1, per_bucket);
        let mut clb: Scatter<Cl, B2> = Scatter::new(p2, cl_bucket);
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= chunks.len() {
                break;
            }
            let c = chunks[i];
            if c.kind == 1 {
                parse_clicks(&ctx, &linked, c, &mut clb);
            } else {
                parse_events(&ctx, &linked, c, &mut evb);
            }
        }
        (evb.finish(), clb.finish())
    });
    eprintln!("phase1 {:?}", t0.elapsed());

    // ---- phase 2: dedup events, repartition by person ----
    let next = AtomicUsize::new(0);
    let n_ev1: usize = p1_locals.iter().map(|l| l.0.iter().map(|b| b.len()).sum::<usize>()).sum();
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
            for l in p1_locals.iter() {
                offs.push(n);
                n += l.0[part].len();
                srcs.push(&l.0[part][..]);
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
            for (t, src) in srcs.iter().enumerate() {
                for (i, r) in src.iter().enumerate() {
                    let id = ((t as u32) << SRC_SHIFT) | i as u32;
                    let h = r.eid;
                    let mut slot = (h as usize) & mask;
                    loop {
                        let cur = unsafe { *table.get_unchecked(slot) };
                        if cur == u32::MAX {
                            table[slot] = id;
                            break;
                        }
                        let other = get(cur);
                        if other.eid == h {
                            if r.ingest < other.ingest {
                                table[slot] = id;
                            }
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
            keep.clear();
            keep.resize(n, 0);
            for &id in table.iter() {
                if id != u32::MAX {
                    let flat = offs[(id >> SRC_SHIFT) as usize] + (id & ((1 << SRC_SHIFT) - 1)) as usize;
                    unsafe { *keep.get_unchecked_mut(flat) = 1 };
                }
            }
            for (t, src) in srcs.iter().enumerate() {
                let kp = &keep[offs[t]..offs[t] + src.len()];
                for (r, &k) in src.iter().zip(kp.iter()) {
                    if k == 0 || r.ts == INVALID {
                        continue;
                    }
                    let person = r.person;
                    evb.push((person >> shift2) as usize, Ev2 { person, name: r.name, ts: r.ts, pc: r.pc });
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
            let cl_srcs: Vec<&[Cl]> = p1_locals.iter().map(|l| &l.1[q][..]).collect();
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
        let ts = match parse_ts(fld(data, pos, f[3])) {
            Some(t) => t - base,
            None => return,
        };
        if ts < 0 || ts >= hi {
            return;
        }
        let camp = match ctx.campaigns.get(hash(fld(data, pos, f[2]))) {
            Some(x) => x,
            None => return,
        };
        let user = hash(fld(data, pos, f[1]));
        linked.map.prefetch(user);
        ring.push(Cl { person: user, cid: fbase | (pos + f[0].0 as usize) as u64, ts: ts as u32, camp }, &mut done);
    });
    ring.drain(&mut done);
}

const W_VIEW: u32 = u32::from_le_bytes(*b"view");
const W_CART: u32 = u32::from_le_bytes(*b"cart");
const W_PURCHASE: u64 = u64::from_le_bytes(*b"purchase");

fn parse_events(ctx: &Ctx, linked: &Linked, c: Chunk, out: &mut Scatter<Ev1, B1>) {
    let data = ctx.data(c.fid as usize);
    let fbase = (c.fid as u64) << OFF_BITS;
    let base = ctx.base;
    let lo = ctx.ev_lo as i64;
    let hi = ctx.ev_hi as i64;
    let shift = 64 - ctx.b1;
    let base_s = base.div_euclid(1000);
    let mut ring: Ring<Ev1> = Ring::new();
    // `out` is shared by the ring's resolver and the invalid-row path
    let out = std::cell::RefCell::new(out);
    let mut done = |mut x: Ev1| {
        let (p, nm) = linked.map.resolve(x.person, x.name);
        x.person = p;
        x.name = nm;
        out.borrow_mut().push((x.eid >> shift) as usize, x);
    };
    scan_lines::<7>(data, c.start, c.end, |pos, f| {
        if f[0].0 == f[0].1 {
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
            let cat = ctx.products.get(hash(fld(data, pos, f[5]))).unwrap_or(ctx.unknown_cat);
            let user = hash(fld(data, pos, f[1]));
            linked.map.prefetch(user);
            rec.person = user;
            rec.name = fbase | (pos + f[1].0 as usize) as u64;
            rec.ts = ts as u32;
            rec.pc = (price << 8) | cat;
            ring.push(rec, &mut done);
            return;
        }
        // invalid rows still take part in dedup; they need no person
        out.borrow_mut().push((eid >> shift) as usize, rec);
    });
    ring.drain(&mut done);
}

// ---------------------------------------------------------------------------
// Phase 3

/// Gather the records of one person partition from `srcs` into `v`, sorted by (person, then
/// `cmp`): a counting pass on the person bits just below the partition bits, then a small sort
/// inside each bucket.
fn bucket_sort<T: Copy>(
    srcs: &[&[T]],
    v: &mut Vec<T>,
    counts: &mut Vec<u32>,
    b2: u32,
    person: impl Fn(&T) -> u64,
    cmp: impl Fn(&T, &T) -> CmpOrd,
) {
    let n: usize = srcs.iter().map(|s| s.len()).sum();
    v.clear();
    if n < 64 {
        for s in srcs {
            v.extend_from_slice(s);
        }
        v.sort_unstable_by(|a, b| person(a).cmp(&person(b)).then_with(|| cmp(a, b)));
        return;
    }
    let kb = ((n / 4).next_power_of_two().trailing_zeros()).clamp(1, 16);
    let k = 1usize << kb;
    let shift = 64 - b2 - kb;
    let bucket = |x: &T| ((person(x) >> shift) as usize) & (k - 1);
    counts.clear();
    counts.resize(k, 0);
    for s in srcs {
        for x in s.iter() {
            counts[bucket(x)] += 1;
        }
    }
    let mut acc = 0u32;
    for c in counts.iter_mut() {
        let t = *c;
        *c = acc;
        acc += t;
    }
    v.reserve(n);
    unsafe {
        v.set_len(n);
        for s in srcs {
            for x in s.iter() {
                let b = bucket(x);
                let p = counts.get_unchecked_mut(b);
                *v.get_unchecked_mut(*p as usize) = *x;
                *p += 1;
            }
        }
    }
    // counts[b] is now the end of bucket b
    let mut start = 0usize;
    for b in 0..k {
        let end = counts[b] as usize;
        let len = end - start;
        if len > 1 {
            let s = &mut v[start..end];
            if len <= 16 {
                for i in 1..len {
                    let mut j = i;
                    while j > 0 {
                        let (a, bb) = (&s[j - 1], &s[j]);
                        if person(a).cmp(&person(bb)).then_with(|| cmp(a, bb)) == CmpOrd::Greater {
                            s.swap(j - 1, j);
                            j -= 1;
                        } else {
                            break;
                        }
                    }
                }
            } else {
                s.sort_unstable_by(|a, b| person(a).cmp(&person(b)).then_with(|| cmp(a, b)));
            }
        }
        start = end;
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
    daygroups: Vec<(u32, u32)>, // (day idx, first session index)
    tmpcl: Vec<(u32, u32)>,     // (camp, ts)
    counts: Vec<u32>,
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
            tmpcl: Vec::new(),
            counts: Vec::new(),
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
        bucket_sort(ev_srcs, evs, &mut self.counts, ctx.b2, |e| e.person, |a, b| {
            let (at, bt) = (a.ts, b.ts);
            at.cmp(&bt)
        });
        bucket_sort(cl_srcs, cls, &mut self.counts, ctx.b2, |c| c.person, |a, b| {
            let (at, bt) = (a.ts, b.ts);
            at.cmp(&bt).then_with(|| ctx.ref_str(a.cid).cmp(ctx.ref_str(b.cid)))
        });
        let n = evs.len();
        let mut i = 0;
        let mut ci = 0;
        while i < n {
            let p = evs[i].person;
            let mut j = i + 1;
            while j < n && evs[j].person == p {
                j += 1;
            }
            while ci < cls.len() && cls[ci].person < p {
                ci += 1;
            }
            let mut ck = ci;
            while ck < cls.len() && cls[ck].person == p {
                ck += 1;
            }
            self.process_person(ctx, &evs[i..j], &cls[ci..ck]);
            i = j;
            ci = ck;
        }
    }

    fn flush_day(&mut self, ctx: &Ctx, name: u64, day: u32, s: i64, e: i64, r: i64) {
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
        parquet::put_plain_str(&mut self.names, ctx.ref_str(name));
        self.days.push((self.ws_day + day as i64) as i32);
        self.sessions.push(s);
        self.events.push(e);
        self.revenue.push(r);
        self.topcat.push(best);
        self.rows += 1;
    }

    fn process_person(&mut self, ctx: &Ctx, ev: &[Ev2], cl: &[Cl]) {
        let name = if ev[0].name & LINKED_BIT != 0 {
            self.link_names[(ev[0].name & !LINKED_BIT) as usize]
        } else {
            ev[0].name
        };
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
                    self.flush_day(ctx, name, cur_day, ds, de, dr);
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
        self.flush_day(ctx, name, cur_day, ds, de, dr);

        // credit
        let nd = self.nd;
        let mut hi = 0;
        for s in self.sess.iter() {
            while hi < cl.len() && cl[hi].ts <= s.start {
                hi += 1;
            }
            let mut camp = self.organic;
            if hi > 0 && (s.start - cl[hi - 1].ts) as i64 <= WEEK_MS {
                let t = cl[hi - 1].ts;
                let mut kk = hi - 1;
                while kk > 0 && cl[kk - 1].ts == t {
                    kk -= 1;
                }
                camp = cl[kk].camp;
            }
            let x = &mut self.stats[camp as usize * nd + s.day as usize];
            x[0] += 1;
            x[1] += s.purch as u64;
            x[2] += s.rev;
        }

        // assists: union of [click, click + 7d] per campaign, sessions starting inside
        if cl.is_empty() {
            return;
        }
        self.starts.clear();
        self.daygroups.clear();
        for (idx, s) in self.sess.iter().enumerate() {
            self.starts.push(s.start);
            if self.daygroups.last().map(|g| g.0) != Some(s.day) {
                self.daygroups.push((s.day, idx as u32));
            }
        }
        let nsess = self.sess.len() as u32;
        self.tmpcl.clear();
        self.tmpcl.extend(cl.iter().map(|c| (c.camp, c.ts)));
        self.tmpcl.sort_unstable();
        let mut g = 0;
        let tl = self.tmpcl.len();
        while g < tl {
            let camp = self.tmpcl[g].0;
            let mut l = self.tmpcl[g].1 as u64;
            let mut r = l + WEEK_MS as u64;
            let mut h = g + 1;
            loop {
                let done = h >= tl || self.tmpcl[h].0 != camp;
                if !done && (self.tmpcl[h].1 as u64) <= r + 1 {
                    r = r.max(self.tmpcl[h].1 as u64 + WEEK_MS as u64);
                    h += 1;
                    continue;
                }
                let a = self.starts.partition_point(|&x| (x as u64) < l) as u32;
                let b = self.starts.partition_point(|&x| (x as u64) <= r) as u32;
                if a < b {
                    for (gi, &(day, first)) in self.daygroups.iter().enumerate() {
                        let end = if gi + 1 < self.daygroups.len() { self.daygroups[gi + 1].1 } else { nsess };
                        let lo = a.max(first);
                        let hi2 = b.min(end);
                        if lo < hi2 {
                            self.stats[camp as usize * nd + day as usize][3] += (hi2 - lo) as u64;
                        }
                    }
                }
                if done {
                    break;
                }
                l = self.tmpcl[h].1 as u64;
                r = l + WEEK_MS as u64;
                h += 1;
            }
            g = h;
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
