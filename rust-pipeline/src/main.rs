mod parquet;
mod parse;

use memmap2::Mmap;
use parse::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: u32 = 1_800_000;
const INVALID: u32 = u32::MAX;
const OFF_BITS: u32 = 40;
const OFF_MASK: u64 = (1u64 << OFF_BITS) - 1;

/// Stage-1 event record, partitioned by event_id hash.
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct Ev1 {
    eid: u64,
    user: u64,
    name: u64,
    ts: u32, // relative ms, INVALID if the row fails validation
    ingest: i32,
    pc: u32, // price << 8 | category
}

/// Stage-2 event record, partitioned by person hash.
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
    person: u64, // user hash before the link lookup, person hash after
    cid: u64,    // ref to click_id bytes
    ts: u32,
    camp: u32,
}

#[derive(Clone, Copy)]
struct LinkRec {
    ha: u64,
    hb: u64,
    ra: u64,
    rb: u64,
}

#[derive(Clone, Copy)]
struct Chunk {
    kind: u8, // 0 link, 1 click, 2 event
    fid: u32,
    start: usize,
    end: usize,
}

struct Ctx {
    files: Vec<Option<Mmap>>,
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
            Some(m) => &m[..],
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

struct Linked {
    map: U64Map,
    comps: Vec<(u64, u64)>, // (person hash, name ref)
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

fn main() {
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
    let mut files: Vec<Option<Mmap>> = Vec::new();
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut ev_bytes = 0usize;
    const CHUNK: usize = 4 << 20;
    for (kind, sub) in [(0u8, "links"), (1, "clicks"), (2, "events")] {
        for p in list_csv(&data_dir.join(sub)) {
            let fid = files.len() as u32;
            let m = map_file(&p);
            let len = m.as_ref().map(|m| m.len()).unwrap_or(0);
            files.push(m);
            if kind == 2 {
                ev_bytes += len;
            }
            let mut s = 0;
            while s < len {
                let e = (s + CHUNK).min(len);
                chunks.push(Chunk { kind, fid, start: s, end: e });
                s = e;
            }
        }
    }
    let est_events = ev_bytes / 70 + 1;
    let b1 = ((est_events / 40_000).max(16).next_power_of_two().trailing_zeros()).min(12);
    let b2 = ((est_events / 20_000).max(16).next_power_of_two().trailing_zeros()).min(13);
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
    let n_link_chunks = chunks.iter().filter(|c| c.kind == 0).count();
    eprintln!(
        "setup {:?}: {} chunks, {} link chunks, p1={} p2={} threads={}",
        t0.elapsed(),
        chunks.len(),
        n_link_chunks,
        p1,
        p2,
        nthreads
    );

    // ---- phase 1: parse ----
    let next = AtomicUsize::new(0);
    let links_done = AtomicUsize::new(0);
    let link_recs: Mutex<Vec<LinkRec>> = Mutex::new(Vec::new());
    let linked: OnceLock<Linked> = OnceLock::new();
    if n_link_chunks == 0 {
        let _ = linked.set(build_links(&ctx, Vec::new()));
    }
    let per_bucket = est_events / (p1 * nthreads) + 16;
    let p1_locals: Vec<(Vec<Vec<Ev1>>, Vec<Cl>)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut evb: Vec<Vec<Ev1>> = (0..p1).map(|_| Vec::with_capacity(per_bucket)).collect();
                    let mut cls: Vec<Cl> = Vec::new();
                    let mut lrecs: Vec<LinkRec> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= chunks.len() {
                            break;
                        }
                        let c = chunks[i];
                        match c.kind {
                            0 => {
                                lrecs.clear();
                                parse_links(&ctx, c, &mut lrecs);
                                link_recs.lock().unwrap().extend_from_slice(&lrecs);
                                if links_done.fetch_add(1, Ordering::AcqRel) + 1 == n_link_chunks {
                                    let recs = std::mem::take(&mut *link_recs.lock().unwrap());
                                    let _ = linked.set(build_links(&ctx, recs));
                                }
                            }
                            1 => parse_clicks(&ctx, c, &mut cls),
                            _ => parse_events(&ctx, c, &mut evb),
                        }
                    }
                    (evb, cls)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let linked = linked.get().expect("links built");
    eprintln!("phase1 {:?}", t0.elapsed());

    // ---- phase 2: dedup events, link lookup, repartition by person ----
    let next = AtomicUsize::new(0);
    let n_click_items = nthreads;
    let n_ev1: usize = p1_locals.iter().map(|l| l.0.iter().map(|b| b.len()).sum::<usize>()).sum();
    let n_cl: usize = p1_locals.iter().map(|l| l.1.len()).sum();
    let ev2_cap = n_ev1 * 5 / 4 / (p2 * nthreads) + 32;
    let cl2_cap = n_cl * 5 / 4 / (p2 * nthreads) + 16;
    let total_items = p1 + n_click_items;
    let p2_locals: Vec<(Vec<Vec<Ev2>>, Vec<Vec<Cl>>)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut evb: Vec<Vec<Ev2>> = (0..p2).map(|_| Vec::with_capacity(ev2_cap)).collect();
                    let mut clb: Vec<Vec<Cl>> = (0..p2).map(|_| Vec::with_capacity(cl2_cap)).collect();
                    let mut recs: Vec<Ev1> = Vec::new();
                    let mut table: Vec<u32> = Vec::new();
                    let shift2 = 64 - ctx.b2;
                    let (mut tc, mut tg, mut td, mut te) = (0f64, 0f64, 0f64, 0f64);
                    let tstart = Instant::now();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= total_items {
                            break;
                        }
                        let tt = Instant::now();
                        if i < n_click_items {
                            for c in p1_locals[i].1.iter() {
                                let mut c = *c;
                                if let Some(ci) = linked.map.get(c.person) {
                                    c.person = linked.comps[ci as usize].0;
                                }
                                clb[(c.person >> shift2) as usize].push(c);
                            }
                            tc += tt.elapsed().as_secs_f64();
                            continue;
                        }
                        let part = i - n_click_items;
                        recs.clear();
                        for l in p1_locals.iter() {
                            recs.extend_from_slice(&l.0[part]);
                        }
                        let n = recs.len();
                        if n == 0 {
                            continue;
                        }
                        tg += tt.elapsed().as_secs_f64();
                        let tt = Instant::now();
                        let cap = (n * 2).next_power_of_two();
                        let mask = cap - 1;
                        table.clear();
                        table.resize(cap, u32::MAX);
                        for idx in 0..n {
                            let h = recs[idx].eid;
                            let mut slot = (h as usize) & mask;
                            loop {
                                let t = table[slot];
                                if t == u32::MAX {
                                    table[slot] = idx as u32;
                                    break;
                                }
                                let other = recs[t as usize];
                                if other.eid == h {
                                    if recs[idx].ingest < other.ingest {
                                        table[slot] = idx as u32;
                                    }
                                    break;
                                }
                                slot = (slot + 1) & mask;
                            }
                        }
                        td += tt.elapsed().as_secs_f64();
                        let tt = Instant::now();
                        for &t in table.iter() {
                            if t == u32::MAX {
                                continue;
                            }
                            let r = recs[t as usize];
                            if r.ts == INVALID {
                                continue;
                            }
                            let (person, name) = match linked.map.get(r.user) {
                                Some(ci) => linked.comps[ci as usize],
                                None => (r.user, r.name),
                            };
                            evb[(person >> shift2) as usize].push(Ev2 { person, name, ts: r.ts, pc: r.pc });
                        }
                        te += tt.elapsed().as_secs_f64();
                    }
                    eprintln!("p2 thread: clicks {:.3} gather {:.3} dedup {:.3} emit {:.3} total {:.3}", tc, tg, td, te, tstart.elapsed().as_secs_f64());
                    (evb, clb)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(p1_locals);
    eprintln!("phase2 {:?}", t0.elapsed());

    // ---- phase 3: sessionize, attribute, aggregate ----
    let ws_day = ws.div_euclid(DAY_MS);
    let nd = ((we - 1).div_euclid(DAY_MS) - ws_day + 1).max(1) as usize;
    let mut order: Vec<(usize, usize)> = (0..p2)
        .map(|q| (p2_locals.iter().map(|l| l.0[q].len()).sum::<usize>(), q))
        .collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let next = AtomicUsize::new(0);
    let ud_dir = out_dir.join("user_daily");
    let cd_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).expect("mkdir");
    std::fs::create_dir_all(&cd_dir).expect("mkdir");
    let ncat = cat_names.len();
    let results: Vec<(Vec<[u64; 4]>, usize)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|tid| {
                let order = &order;
                let p2_locals = &p2_locals;
                let ctx = &ctx;
                let cat_names = &cat_names;
                let ud_dir = &ud_dir;
                let next = &next;
                s.spawn(move || {
                    let mut st = P3State::new(ncamp + 1, nd, ncat, ctx.base, ws_day, organic);
                    let mut evs: Vec<Ev2> = Vec::new();
                    let mut cls: Vec<Cl> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= order.len() {
                            break;
                        }
                        let q = order[i].1;
                        evs.clear();
                        cls.clear();
                        for l in p2_locals.iter() {
                            evs.extend_from_slice(&l.0[q]);
                            cls.extend_from_slice(&l.1[q]);
                        }
                        st.process_partition(ctx, &mut evs, &mut cls);
                    }
                    let rows = st.rows;
                    let stats = std::mem::take(&mut st.stats);
                    if rows > 0 {
                        st.write_user_daily(cat_names, &ud_dir.join(format!("part-{:05}.parquet", tid)));
                    }
                    (stats, rows)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    eprintln!("phase3 {:?}", t0.elapsed());

    let total_rows: usize = results.iter().map(|r| r.1).sum();
    if total_rows == 0 {
        let st = P3State::new(1, 1, 1, 0, 0, 0);
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
// Phase 1 parsers

/// Iterate the lines whose first byte lies in [c.start, c.end), skipping the file header.
#[inline(always)]
fn for_lines(data: &[u8], c: Chunk, mut f: impl FnMut(&[u8], usize)) {
    let len = data.len();
    let mut pos = c.start;
    if pos == 0 {
        pos = match memchr::memchr(b'\n', data) {
            Some(p) => p + 1,
            None => len,
        };
    } else if data[pos - 1] != b'\n' {
        pos = match memchr::memchr(b'\n', &data[pos..]) {
            Some(p) => pos + p + 1,
            None => len,
        };
    }
    while pos < c.end {
        let e = match memchr::memchr(b'\n', &data[pos..]) {
            Some(p) => pos + p,
            None => len,
        };
        f(&data[pos..e], pos);
        pos = e + 1;
    }
}

fn parse_links(ctx: &Ctx, c: Chunk, out: &mut Vec<LinkRec>) {
    let data = ctx.data(c.fid as usize);
    let fbase = (c.fid as u64) << OFF_BITS;
    for_lines(data, c, |line, off| {
        let mut f = [(0, 0); 2];
        if !split::<2>(line, &mut f) {
            return;
        }
        let a = &line[f[0].0..f[0].1];
        let b = &line[f[1].0..f[1].1];
        if a.is_empty() || b.is_empty() || a == b {
            return;
        }
        out.push(LinkRec {
            ha: hash(a),
            hb: hash(b),
            ra: fbase | (off + f[0].0) as u64,
            rb: fbase | (off + f[1].0) as u64,
        });
    });
}

fn parse_clicks(ctx: &Ctx, c: Chunk, out: &mut Vec<Cl>) {
    let data = ctx.data(c.fid as usize);
    let fbase = (c.fid as u64) << OFF_BITS;
    let base = ctx.base;
    let hi = ctx.cl_hi as i64;
    for_lines(data, c, |line, off| {
        let mut f = [(0, 0); 4];
        if !split::<4>(line, &mut f) {
            return;
        }
        if f[0].0 == f[0].1 || f[1].0 == f[1].1 {
            return;
        }
        let ts = match parse_ts(&line[f[3].0..f[3].1]) {
            Some(t) => t - base,
            None => return,
        };
        if ts < 0 || ts >= hi {
            return;
        }
        let camp = match ctx.campaigns.get(hash(&line[f[2].0..f[2].1])) {
            Some(x) => x,
            None => return,
        };
        out.push(Cl {
            person: hash(&line[f[1].0..f[1].1]),
            cid: fbase | (off + f[0].0) as u64,
            ts: ts as u32,
            camp,
        });
    });
}

fn parse_events(ctx: &Ctx, c: Chunk, out: &mut [Vec<Ev1>]) {
    let data = ctx.data(c.fid as usize);
    let fbase = (c.fid as u64) << OFF_BITS;
    let base = ctx.base;
    let lo = ctx.ev_lo as i64;
    let hi = ctx.ev_hi as i64;
    let shift = 64 - ctx.b1;
    let base_s = base.div_euclid(1000);
    for_lines(data, c, |line, off| {
        let mut f = [(0, 0); 7];
        if !split::<7>(line, &mut f) {
            return;
        }
        if f[0].0 == f[0].1 {
            return;
        }
        let eid = hash(&line[f[0].0..f[0].1]);
        let ingest_ms = parse_f1(&line[f[3].0..f[3].1]).unwrap_or(i64::MAX / 4);
        let ingest = (ingest_ms.div_euclid(1000) - base_s).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        let mut rec = Ev1 { eid, user: 0, name: 0, ts: INVALID, ingest, pc: 0 };
        'v: {
            if f[1].0 == f[1].1 || f[5].0 == f[5].1 {
                break 'v;
            }
            let et = &line[f[4].0..f[4].1];
            let purchase = match et {
                b"view" | b"cart" => false,
                b"purchase" => true,
                _ => break 'v,
            };
            let ts_abs = match parse_ts(&line[f[2].0..f[2].1]) {
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
                price = match parse_price(&line[f[6].0..f[6].1]) {
                    Some(p) => p,
                    None => break 'v,
                };
            }
            let cat = ctx.products.get(hash(&line[f[5].0..f[5].1])).unwrap_or(ctx.unknown_cat);
            rec.user = hash(&line[f[1].0..f[1].1]);
            rec.name = fbase | (off + f[1].0) as u64;
            rec.ts = ts as u32;
            rec.pc = (price << 8) | cat;
        }
        out[(eid >> shift) as usize].push(rec);
    });
}

// ---------------------------------------------------------------------------
// Links: union-find over hashed IDs, named by the byte-wise smallest ID.

fn build_links(ctx: &Ctx, recs: Vec<LinkRec>) -> Linked {
    let t = Instant::now();
    let mut ids = U64Map::with_capacity(recs.len() * 2 + 16);
    let mut refs: Vec<u64> = Vec::with_capacity(recs.len() * 2);
    let mut hashes: Vec<u64> = Vec::with_capacity(recs.len() * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(recs.len() * 2);
    let mut get_id = |h: u64, r: u64, refs: &mut Vec<u64>, hashes: &mut Vec<u64>, parent: &mut Vec<u32>| -> u32 {
        let n = refs.len() as u32;
        let id = ids.insert(h, n);
        if id == n {
            refs.push(r);
            hashes.push(h);
            parent.push(n);
        }
        id
    };
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for r in &recs {
        let a = get_id(r.ha, r.ra, &mut refs, &mut hashes, &mut parent);
        let b = get_id(r.hb, r.rb, &mut refs, &mut hashes, &mut parent);
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            // union by index (smaller root becomes parent) keeps it simple; path halving bounds depth
            if ra < rb {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    let n = refs.len();
    // min ref per root
    let mut minref: Vec<u64> = vec![u64::MAX; n];
    let mut roots: Vec<u32> = vec![0; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        roots[i] = r;
        let cur = minref[r as usize];
        if cur == u64::MAX || ctx.ref_str(refs[i]) < ctx.ref_str(cur) {
            minref[r as usize] = refs[i];
        }
    }
    let mut comp_of_root: Vec<u32> = vec![u32::MAX; n];
    let mut comps: Vec<(u64, u64)> = Vec::new();
    let mut map = U64Map::with_capacity(n + 16);
    for i in 0..n {
        let r = roots[i] as usize;
        if comp_of_root[r] == u32::MAX {
            comp_of_root[r] = comps.len() as u32;
            let mr = minref[r];
            comps.push((hash(ctx.ref_str(mr)), mr));
        }
        map.insert(hashes[i], comp_of_root[r]);
    }
    eprintln!("links: {} recs, {} ids, {} comps in {:?}", recs.len(), n, comps.len(), t.elapsed());
    Linked { map, comps }
}

// ---------------------------------------------------------------------------
// Phase 3

#[derive(Clone, Copy)]
struct Sess {
    start: u32,
    day: u32,
    rev: u64,
    purch: bool,
}

struct P3State {
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
    // user_daily columns
    rows: usize,
    names: Vec<u8>,
    days: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    topcat: Vec<i32>,
}

impl P3State {
    fn new(ncampx: usize, nd: usize, ncat: usize, base: i64, ws_day: i64, organic: u32) -> P3State {
        P3State {
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
            rows: 0,
            names: Vec::new(),
            days: Vec::new(),
            sessions: Vec::new(),
            events: Vec::new(),
            revenue: Vec::new(),
            topcat: Vec::new(),
        }
    }

    fn process_partition(&mut self, ctx: &Ctx, evs: &mut Vec<Ev2>, cls: &mut Vec<Cl>) {
        evs.sort_unstable_by_key(|e| ((e.person as u128) << 32) | e.ts as u128);
        cls.sort_unstable_by(|a, b| {
            let (ap, bp, at, bt) = (a.person, b.person, a.ts, b.ts);
            ap.cmp(&bp).then(at.cmp(&bt)).then_with(|| ctx.ref_str(a.cid).cmp(ctx.ref_str(b.cid)))
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
        let name = ev[0].name;
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

        // assists
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
                // emit [l, r]
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
