//! CSV dialect, timestamps, prices, and hashing.

pub const DAY_MS: i64 = 86_400_000;

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[inline(always)]
fn hash_words(a: u64, c: u64, n: usize) -> u64 {
    let x = mum(a ^ 0x2d358dccaa6c78a5, c ^ 0x8bb84b93962eacc9 ^ (n as u64));
    mum(x ^ 0x4b33a62ed433d4a3, (n as u64) ^ 0x4d5a2da51de1aa47)
}

/// 64-bit hash of a byte string. Up to 16 bytes: the little-endian words of the first and
/// last min(n, 8) bytes, mixed with two 128-bit multiplies; longer strings use xxh3.
#[inline(always)]
pub fn hash(b: &[u8]) -> u64 {
    let n = b.len();
    if n > 16 {
        return xxhash_rust::xxh3::xxh3_64(b) ^ 0x9e3779b97f4a7c15;
    }
    let (a, c) = if n >= 8 {
        unsafe {
            (
                (b.as_ptr() as *const u64).read_unaligned(),
                (b.as_ptr().add(n - 8) as *const u64).read_unaligned(),
            )
        }
    } else {
        let mut w = [0u8; 8];
        w[..n].copy_from_slice(b);
        let v = u64::from_le_bytes(w);
        (v, v)
    };
    hash_words(a, c, n)
}

/// `hash` of the n bytes at p, branch-free for 1..=16 bytes; needs 8 readable bytes on both
/// sides of the field (the fast-path guarantee).
#[inline(always)]
pub unsafe fn hash_fast(p: *const u8, n: usize) -> u64 {
    if n.wrapping_sub(1) >= 16 {
        return hash(std::slice::from_raw_parts(p, n));
    }
    let sh = 64 - 8 * n.min(8) as u32;
    let a = (p as *const u64).read_unaligned() & (!0u64 >> sh);
    let c = (p.add(n).sub(8) as *const u64).read_unaligned() >> sh;
    hash_words(a, c, n)
}

/// Split one line (without its '\n') into exactly N fields, returning unquoted content ranges.
/// Returns false for a malformed line.
#[inline(always)]
pub fn split<const N: usize>(line: &[u8], f: &mut [(usize, usize); N]) -> bool {
    let n = line.len();
    if n == 0 {
        return false;
    }
    let mut i = 0usize;
    let mut k = 0usize;
    loop {
        if i < n && unsafe { *line.get_unchecked(i) } == b'"' {
            let s = i + 1;
            let mut j = s;
            loop {
                if j >= n {
                    return false;
                }
                let c = unsafe { *line.get_unchecked(j) };
                if c == b'"' {
                    break;
                }
                if c == b',' {
                    return false;
                }
                j += 1;
            }
            f[k] = (s, j);
            i = j + 1;
        } else {
            let s = i;
            let mut j = s;
            while j < n {
                let c = unsafe { *line.get_unchecked(j) };
                if c == b',' {
                    break;
                }
                if c == b'"' {
                    return false;
                }
                j += 1;
            }
            f[k] = (s, j);
            i = j;
        }
        k += 1;
        if k < N {
            if i >= n || unsafe { *line.get_unchecked(i) } != b',' {
                return false;
            }
            i += 1;
        } else {
            return i == n;
        }
    }
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<u32> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some(a as u32 * 10 + c as u32)
    } else {
        None
    }
}

#[inline(always)]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline(always)]
fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
#[inline(always)]
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

const fn build_year_days() -> [i32; 256] {
    let mut t = [0i32; 256];
    let mut i = 0;
    while i < 256 {
        let y = 1900 + i as i64;
        // days_from_civil(y, 1, 1) inlined for const context
        let yy = y - 1;
        let era = yy / 400;
        let yoe = yy - era * 400;
        let doy = (153 * 10 + 2) / 5; // March-based day of year for Jan 1 (mp = 10)
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        t[i] = (era * 146097 + doe - 719468) as i32;
        i += 1;
    }
    t
}
static YEAR_DAYS: [i32; 256] = build_year_days();
static MONTH_CUM: [[u16; 13]; 2] = [
    [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334],
    [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
];
static MONTH_LEN: [[u8; 13]; 2] = [
    [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
];

#[inline(always)]
fn swar_digits_ok(w: u64, dmask: u64) -> bool {
    let d = w & dmask;
    let hi = 0xF0F0_F0F0_F0F0_F0F0u64 & dmask;
    let thirty = 0x3030_3030_3030_3030u64 & dmask;
    (d & hi) == thirty && ((d.wrapping_add(0x0606_0606_0606_0606 & dmask)) & hi) == thirty
}

/// Parse `YYYY-MM-DDTHH:MM:SS` at p (19 readable bytes, plus 1 more byte readable) to local epoch ms.
#[inline(always)]
unsafe fn dt_swar(p: *const u8) -> Option<i64> {
    let w0 = (p as *const u64).read_unaligned(); // Y Y Y Y - M M -
    let w1 = (p.add(8) as *const u64).read_unaligned(); // D D T H H : M M
    let w2 = (p.add(16) as *const u32).read_unaligned() as u64; // : S S x
    const S0M: u64 = 0xFF00_00FF_0000_0000;
    const S0V: u64 = 0x2D00_002D_0000_0000;
    const S1M: u64 = 0x0000_FF00_00FF_0000;
    const S1V: u64 = 0x0000_3A00_0054_0000;
    const D0M: u64 = 0x00FF_FF00_FFFF_FFFF;
    const D1M: u64 = 0xFFFF_00FF_FF00_FFFF;
    const D2M: u64 = 0x0000_0000_00FF_FF00;
    if (w0 & S0M) != S0V
        || (w1 & S1M) != S1V
        || (w2 & 0xFF) != 0x3A
        || !swar_digits_ok(w0, D0M)
        || !swar_digits_ok(w1, D1M)
        || !swar_digits_ok(w2, D2M)
    {
        return None;
    }
    // digits validated >= '0', separators masked to 0: no borrows between bytes
    let v0 = (w0 & D0M) - (0x3030_3030_3030_3030 & D0M);
    let v1 = (w1 & D1M) - (0x3030_3030_3030_3030 & D1M);
    let v2 = (w2 & D2M) - (0x3030_3030 & D2M);
    let b = |w: u64, i: u32| ((w >> (8 * i)) & 0xFF) as u32;
    let y = b(v0, 0) * 1000 + b(v0, 1) * 100 + b(v0, 2) * 10 + b(v0, 3);
    let mo = b(v0, 5) * 10 + b(v0, 6);
    let d = b(v1, 0) * 10 + b(v1, 1);
    let h = b(v1, 3) * 10 + b(v1, 4);
    let mi = b(v1, 6) * 10 + b(v1, 7);
    let s = b(v2, 1) * 10 + b(v2, 2);
    if mo.wrapping_sub(1) > 11 || d == 0 || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let days: i64;
    if y.wrapping_sub(1900) < 256 {
        let leap = ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) as usize;
        if d > MONTH_LEN[leap][mo as usize] as u32 {
            return None;
        }
        days = YEAR_DAYS[(y - 1900) as usize] as i64 + MONTH_CUM[leap][mo as usize] as i64 + d as i64 - 1;
    } else {
        if d > days_in_month(y as i64, mo) {
            return None;
        }
        days = days_from_civil(y as i64, mo, d);
    }
    Some(((days * 86400) + (h as i64) * 3600 + (mi as i64) * 60 + s as i64) * 1000)
}

/// F1 only (used for ingest_ts).
#[inline(always)]
pub fn parse_f1(b: &[u8]) -> Option<i64> {
    if b.len() != 20 || b[19] != b'Z' {
        return None;
    }
    unsafe { dt_swar(b.as_ptr()) }
}

/// Any of F1/F2/F3. F3 values that do not fit are returned as None (they are out of any window).
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n >= 5 && b[4] == b'-' {
        if n == 20 {
            if b[19] != b'Z' {
                return None;
            }
            return unsafe { dt_swar(b.as_ptr()) };
        }
        if n == 25 {
            let sign = b[19];
            if sign != b'+' && sign != b'-' {
                return None;
            }
            if b[22] != b':' {
                return None;
            }
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let local = unsafe { dt_swar(b.as_ptr())? };
            let off = ((oh as i64) * 60 + om as i64) * 60_000;
            return Some(if sign == b'+' { local - off } else { local + off });
        }
        return None;
    }
    // F3: digits only
    let mut i = 0;
    while i < n && b[i] == b'0' {
        i += 1;
    }
    let digits = n - i;
    if digits > 18 {
        // may not fit; anything this large is out of every window (still must be all digits to matter: it is bad either way)
        return None;
    }
    let mut v: u64 = 0;
    for &c in &b[i..] {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
    }
    Some(v as i64)
}

/// Purchase price: ASCII digits only, value 1..=10_000_000.
#[inline(always)]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // keep scanning for non-digits? value already invalid either way
            return None;
        }
    }
    if v >= 1 {
        Some(v as u32)
    } else {
        None
    }
}

/// Open-addressing u64 -> u32 map. Keys are hashes; 0 is remapped.
pub struct U64Map {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
}

impl U64Map {
    pub fn with_capacity(n: usize) -> U64Map {
        let cap = (n.max(8) * 2).next_power_of_two();
        U64Map { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1 }
    }
    #[inline(always)]
    fn nz(k: u64) -> u64 {
        if k == 0 {
            1
        } else {
            k
        }
    }
    /// Insert if absent; returns the existing or inserted value.
    pub fn insert(&mut self, k: u64, v: u32) -> u32 {
        let k = Self::nz(k);
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                return v;
            }
            if kk == k {
                return self.vals[i];
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<u32> {
        let k = Self::nz(k);
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if kk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Person key for linked component `idx` (unlinked people use their ID's hash).
#[inline(always)]
pub fn comp_person(idx: u64) -> u64 {
    let mut z = idx.wrapping_add(0x9E37_79B9_7F4A_7C15).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// ---------------------------------------------------------------------------
// SIMD line scanning

#[cfg(not(target_feature = "avx2"))]
compile_error!("build with AVX2 (see .cargo/config.toml)");

/// Bitmasks of '\n', ',' and '"' over the 64 bytes starting at p.
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |ch: u8| -> u64 {
        let v = _mm256_set1_epi8(ch as i8);
        (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64) << 32)
    };
    (m(b'\n'), m(b','), m(b'"'))
}

/// 128 mask bits starting at bit `r` of a block-mask array (reads entries r/64 ..= r/64+2).
#[inline(always)]
fn window(m: &[u64], r: usize) -> u128 {
    let w = r >> 6;
    let o = (r & 63) as u32;
    unsafe {
        let lo = *m.get_unchecked(w) as u128;
        let mid = *m.get_unchecked(w + 1) as u128;
        let hi = *m.get_unchecked(w + 2) as u128;
        (((mid << 64) | lo) >> o) | ((hi << 64) << (64 - o))
    }
}

/// Inclusive prefix XOR of a 128-bit mask (bit i = parity of bits 0..=i).
#[inline(always)]
fn prefix_xor128(x: u128) -> u128 {
    use std::arch::x86_64::*;
    unsafe {
        let ones = _mm_set1_epi8(-1);
        let lo = _mm_cvtsi128_si64(_mm_clmulepi64_si128(_mm_set_epi64x(0, x as u64 as i64), ones, 0)) as u64;
        let hi = _mm_cvtsi128_si64(_mm_clmulepi64_si128(_mm_set_epi64x(0, (x >> 64) as u64 as i64), ones, 0)) as u64;
        let hi = hi ^ (((lo as i64) >> 63) as u64);
        ((hi as u128) << 64) | lo as u128
    }
}

/// Fields of a line of length l (< 128) from its comma/quote masks; false if malformed.
/// A field may be wrapped in one pair of quotes; any other quote, or a separator inside
/// quotes, makes the line malformed.
#[inline(always)]
fn fields_from_masks<const N: usize>(line: *const u8, l: u32, cm: u128, qt: u128, f: &mut [(u32, u32); N]) -> bool {
    let lm = (1u128 << l) - 1;
    let mut c = cm & lm;
    let q = qt & lm;
    let mut ok = c.count_ones() as usize == N - 1;
    // every quote opens at a field start or closes at a field end (never both), and no
    // separator lies between an opening and its closing quote
    let sep = c | (1u128 << l);
    let starts = (sep << 1) | 1;
    let ends = sep >> 1;
    let open = q & starts;
    let close = q & ends;
    ok &= ((open | close) == q) & ((open & close) == 0) & ((prefix_xor128(q) & sep) == 0);
    let mut s = 0u32;
    for k in 0..N {
        let e = if k + 1 < N {
            let e = c.trailing_zeros().min(l);
            c &= c.wrapping_sub(1);
            e
        } else {
            l
        };
        // in a valid line, a field starting with a quote also ends with one
        let qd = ((s < e) && unsafe { *line.add(s as usize) } == b'"') as u32;
        f[k] = (s + qd, e - qd);
        s = e + 1;
    }
    ok
}

const SUB: usize = 16 << 10;
const NB: usize = SUB / 64 + 2;

/// Call `f(line_start, fields)` for every well-formed line starting in [start, end),
/// skipping the file's header line. Field ranges are relative to line_start.
#[inline(always)]
pub fn scan_lines<const N: usize>(data: &[u8], start: usize, end: usize, mut f: impl FnMut(usize, &[(u32, u32); N])) {
    let len = data.len();
    let mut pos = start;
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
    let mut fr = [(0u32, 0u32); N];
    let ptr = data.as_ptr();
    let mut mnl = [0u64; NB + 2];
    let mut mcm = [0u64; NB + 2];
    let mut mqt = [0u64; NB + 2];
    while pos < end {
        // index a region; every line starting in its first `lim` bytes lies inside it
        let base = pos;
        let nb = ((len - base) / 64).min(NB);
        for k in 0..nb {
            let (a, b, c) = unsafe { masks64(ptr.add(base + 64 * k)) };
            mnl[k] = a;
            mcm[k] = b;
            mqt[k] = c;
        }
        let lim = (nb * 64).saturating_sub(128).min(SUB).min(end - base);
        let mut r = 0usize;
        while r < lim {
            let nl = window(&mnl, r);
            if nl == 0 {
                // a line of 128+ bytes
                let p = base + r;
                let e = match memchr::memchr(b'\n', &data[p..]) {
                    Some(x) => p + x,
                    None => len,
                };
                slow_line::<N>(data, p, e, &mut f);
                r = e + 1 - base;
                continue;
            }
            let l = nl.trailing_zeros();
            if fields_from_masks::<N>(unsafe { ptr.add(base + r) }, l, window(&mcm, r), window(&mqt, r), &mut fr) {
                f(base + r, &fr);
            }
            r += l as usize + 1;
        }
        pos = base + r;
        if nb < NB {
            // end of the file: plain scalar
            while pos < end {
                let e = match memchr::memchr(b'\n', &data[pos..]) {
                    Some(p) => pos + p,
                    None => len,
                };
                slow_line::<N>(data, pos, e, &mut f);
                pos = e + 1;
            }
        }
    }
}

#[inline(never)]
fn slow_line<const N: usize>(data: &[u8], p: usize, e: usize, f: &mut impl FnMut(usize, &[(u32, u32); N])) {
    let mut ff = [(0usize, 0usize); N];
    if split::<N>(&data[p..e], &mut ff) {
        let mut fr = [(0u32, 0u32); N];
        for k in 0..N {
            fr[k] = (ff[k].0 as u32, ff[k].1 as u32);
        }
        f(p, &fr);
    }
}

// ---------------------------------------------------------------------------
// SIMD field parsers. Callers guarantee that the 16 bytes before the field's end and the
// 20 bytes from its start are readable (any field of a line that starts at least 16 bytes
// into the data and has 128 bytes after its start).

const fn build_tables() -> ([u8; 256], [u8; 256], [u16; 256]) {
    let mut leap = [0u8; 256];
    let mut mlen = [0u8; 256];
    let mut mcum = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let y = 1900 + i;
        leap[i] = ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) as u8;
        i += 1;
    }
    let lens = [31u8, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut l = 0;
    while l < 2 {
        let mut m = 1;
        let mut cum = 0u16;
        while m <= 12 {
            let len = if m == 2 && l == 1 { 29 } else { lens[m - 1] };
            mlen[l * 128 + m] = len;
            mcum[l * 128 + m] = cum;
            cum += len as u16;
            m += 1;
        }
        l += 1;
    }
    (leap, mlen, mcum)
}
static TABLES: ([u8; 256], [u8; 256], [u16; 256]) = build_tables();

/// `YYYY-MM-DDTHH:MM:SS` at p to local epoch seconds.
#[inline(always)]
unsafe fn dt_fast(p: *const u8) -> Option<i64> {
    use std::arch::x86_64::*;
    let a = _mm_loadu_si128(p as *const __m128i);
    let b = _mm_loadu_si128(p.add(4) as *const __m128i);
    let pat = _mm_setr_epi8(b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0, b':' as i8, 0, 0, 0);
    let sep = (_mm_movemask_epi8(_mm_cmpeq_epi8(b, pat)) & 0x1249) == 0x1249;
    let db = _mm_shuffle_epi8(b, _mm_setr_epi8(1, 2, 4, 5, 7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1));
    let da = _mm_shuffle_epi8(a, _mm_setr_epi8(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, -1, -1));
    let d = _mm_sub_epi8(_mm_or_si128(db, da), _mm_set1_epi8(b'0' as i8));
    let dig = (_mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d)) & 0x3FFF) == 0x3FFF;
    if !(sep & dig) {
        return None;
    }
    let v = _mm_maddubs_epi16(d, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0));
    let mo = _mm_extract_epi16(v, 0) as u32;
    let dd = _mm_extract_epi16(v, 1) as u32;
    let h = _mm_extract_epi16(v, 2) as u32;
    let mi = _mm_extract_epi16(v, 3) as u32;
    let s = _mm_extract_epi16(v, 4) as u32;
    let y = _mm_extract_epi16(v, 5) as u32 * 100 + _mm_extract_epi16(v, 6) as u32;
    let yi = y.wrapping_sub(1900);
    if yi >= 256 {
        return dt_swar(p).map(|ms| ms / 1000);
    }
    let (leap, mlen, mcum) = &TABLES;
    let li = (*leap.get_unchecked(yi as usize) as usize) * 128 + mo as usize;
    let ml = *mlen.get_unchecked(li) as u32;
    if (dd.wrapping_sub(1) >= ml) | (h > 23) | (mi > 59) | (s > 59) {
        return None;
    }
    let days = *YEAR_DAYS.get_unchecked(yi as usize) as i64 + *mcum.get_unchecked(li) as i64 + dd as i64 - 1;
    Some(days * 86400 + (h * 3600 + mi * 60 + s) as i64)
}

/// The value of the n (1..=16) ASCII digits ending at `end`, or None if any is not a digit.
#[inline(always)]
pub unsafe fn digits16(end: *const u8, n: usize) -> Option<u64> {
    use std::arch::x86_64::*;
    let v = _mm_loadu_si128(end.sub(16) as *const __m128i);
    let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
    let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    let keep = _mm_cmpgt_epi8(idx, _mm_set1_epi8(15 - n as i8));
    let isdig = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
    if _mm_movemask_epi8(_mm_andnot_si128(isdig, keep)) != 0 {
        return None;
    }
    let d = _mm_and_si128(d, keep);
    let t1 = _mm_maddubs_epi16(d, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
    let t2 = _mm_madd_epi16(t1, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
    let t3 = _mm_packus_epi32(t2, t2);
    let t4 = _mm_madd_epi16(t3, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
    let hi = _mm_cvtsi128_si32(t4) as u32 as u64;
    let lo = _mm_extract_epi32(t4, 1) as u32 as u64;
    Some(hi * 100_000_000 + lo)
}

/// `YYYY-MM-DDTHH:MM:SS` at p: (valid, local epoch seconds, year outside 1900..2155).
/// Branch-free; when the year is outside the table the caller must use `dt_swar`.
#[inline(always)]
unsafe fn dt_core(p: *const u8) -> (bool, i64, bool) {
    use std::arch::x86_64::*;
    let a = _mm_loadu_si128(p as *const __m128i);
    let b = _mm_loadu_si128(p.add(4) as *const __m128i);
    let pat = _mm_setr_epi8(b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0, b':' as i8, 0, 0, 0);
    let sep = (_mm_movemask_epi8(_mm_cmpeq_epi8(b, pat)) & 0x1249) == 0x1249;
    let db = _mm_shuffle_epi8(b, _mm_setr_epi8(1, 2, 4, 5, 7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1));
    let da = _mm_shuffle_epi8(a, _mm_setr_epi8(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, -1, -1));
    let d = _mm_sub_epi8(_mm_or_si128(db, da), _mm_set1_epi8(b'0' as i8));
    let dig = (_mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d)) & 0x3FFF) == 0x3FFF;
    let v = _mm_maddubs_epi16(d, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0));
    let mo = _mm_extract_epi16(v, 0) as u32 & 127;
    let dd = _mm_extract_epi16(v, 1) as u32;
    let h = _mm_extract_epi16(v, 2) as u32;
    let mi = _mm_extract_epi16(v, 3) as u32;
    let s = _mm_extract_epi16(v, 4) as u32;
    let y = _mm_extract_epi16(v, 5) as u32 * 100 + _mm_extract_epi16(v, 6) as u32;
    let yi = y.wrapping_sub(1900);
    let out = yi >= 256;
    let yi = (yi & 255) as usize;
    let (leap, mlen, mcum) = &TABLES;
    let li = (*leap.get_unchecked(yi) as usize) * 128 + mo as usize;
    let ml = *mlen.get_unchecked(li) as u32;
    let ok = sep & dig & (dd.wrapping_sub(1) < ml) & (h < 24) & (mi < 60) & (s < 60);
    let days = *YEAR_DAYS.get_unchecked(yi) as i64 + *mcum.get_unchecked(li) as i64 + dd as i64 - 1;
    (ok, days * 86400 + (h * 3600 + mi * 60 + s) as i64, out & sep & dig)
}

/// The n (0..=16) ASCII digits ending at `end`: (all digits and n >= 1, value). Branch-free.
#[inline(always)]
unsafe fn digits16_bl(end: *const u8, n: usize) -> (bool, u64) {
    use std::arch::x86_64::*;
    let v = _mm_loadu_si128(end.sub(16) as *const __m128i);
    let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
    let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    let keep = _mm_cmpgt_epi8(idx, _mm_set1_epi8(15 - n.min(16) as i8));
    let isdig = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
    let ok = _mm_movemask_epi8(_mm_andnot_si128(isdig, keep)) == 0;
    let d = _mm_and_si128(d, keep);
    let t1 = _mm_maddubs_epi16(d, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
    let t2 = _mm_madd_epi16(t1, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
    let t3 = _mm_packus_epi32(t2, t2);
    let t4 = _mm_madd_epi16(t3, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
    let hi = _mm_cvtsi128_si32(t4) as u32 as u64;
    let lo = _mm_extract_epi32(t4, 1) as u32 as u64;
    (ok & (n >= 1), hi * 100_000_000 + lo)
}

/// `parse_ts` under the fast-path read guarantee, with the F1/F2/F3 choice made without
/// branching (formats are mixed row by row, so a branch would mispredict often).
#[inline(always)]
pub unsafe fn ts_fast(p: *const u8, n: usize) -> Option<i64> {
    let is_date = (n >= 5) & (*p.add(4) == b'-');
    let (dok, secs, out) = dt_core(p);
    let c19 = *p.add(19);
    let f1 = (n == 20) & (c19 == b'Z');
    let oh = ((*p.add(20)).wrapping_sub(b'0') as u32, (*p.add(21)).wrapping_sub(b'0') as u32);
    let om = ((*p.add(23)).wrapping_sub(b'0') as u32, (*p.add(24)).wrapping_sub(b'0') as u32);
    let ohv = oh.0 * 10 + oh.1;
    let omv = om.0 * 10 + om.1;
    let f2 = (n == 25)
        & ((c19 == b'+') | (c19 == b'-'))
        & (*p.add(22) == b':')
        & (oh.0 < 10)
        & (oh.1 < 10)
        & (om.0 < 10)
        & (om.1 < 10)
        & (ohv < 24)
        & (omv < 60);
    let off = ((ohv * 60 + omv) * 60_000) as i64;
    let off = if c19 == b'+' { -off } else { off };
    let date_ms = secs * 1000 + if f2 { off } else { 0 };
    let (gok, g) = digits16_bl(p.add(n), n);
    if (is_date & out & (f1 | f2)) | (!is_date & (n > 16)) {
        return parse_ts(std::slice::from_raw_parts(p, n));
    }
    let ok = if is_date { dok & (f1 | f2) } else { gok };
    let v = if is_date { date_ms } else { g as i64 };
    if ok {
        Some(v)
    } else {
        None
    }
}

/// `parse_ts` for a field at p of length n, under the fast-path read guarantee.
#[inline(always)]
pub unsafe fn parse_ts_fast(p: *const u8, n: usize) -> Option<i64> {
    if n >= 5 && *p.add(4) == b'-' {
        if n == 20 {
            if *p.add(19) != b'Z' {
                return None;
            }
            return dt_fast(p).map(|s| s * 1000);
        }
        if n == 25 {
            let sign = *p.add(19);
            if (sign != b'+' && sign != b'-') || *p.add(22) != b':' {
                return None;
            }
            let oh = (*p.add(20)).wrapping_sub(b'0') as u32;
            let oh2 = (*p.add(21)).wrapping_sub(b'0') as u32;
            let om = (*p.add(23)).wrapping_sub(b'0') as u32;
            let om2 = (*p.add(24)).wrapping_sub(b'0') as u32;
            if oh > 9 || oh2 > 9 || om > 9 || om2 > 9 {
                return None;
            }
            let (oh, om) = (oh * 10 + oh2, om * 10 + om2);
            if oh > 23 || om > 59 {
                return None;
            }
            let local = dt_fast(p)? * 1000;
            let off = ((oh as i64) * 60 + om as i64) * 60_000;
            return Some(if sign == b'+' { local - off } else { local + off });
        }
        return None;
    }
    if n.wrapping_sub(1) < 16 {
        return digits16(p.add(n), n).map(|v| v as i64);
    }
    parse_ts(std::slice::from_raw_parts(p, n))
}

/// `parse_f1` under the fast-path read guarantee.
#[inline(always)]
pub unsafe fn parse_f1_fast(p: *const u8, n: usize) -> Option<i64> {
    if n != 20 || *p.add(19) != b'Z' {
        return None;
    }
    dt_fast(p).map(|s| s * 1000)
}

/// Purchase price under the fast-path read guarantee.
#[inline(always)]
pub unsafe fn parse_price_fast(p: *const u8, n: usize) -> Option<u32> {
    if n.wrapping_sub(1) >= 16 {
        return parse_price(std::slice::from_raw_parts(p, n));
    }
    let v = digits16(p.add(n), n)?;
    if v.wrapping_sub(1) < 10_000_000 {
        Some(v as u32)
    } else {
        None
    }
}

/// Product id `p<digits>` with no leading zero (or exactly "p0") as its number; None otherwise.
#[inline(always)]
pub unsafe fn product_num(p: *const u8, n: usize) -> Option<u32> {
    if n < 2 || n > 8 || *p != b'p' || (*p.add(1) == b'0' && n > 2) {
        return None;
    }
    digits16(p.add(n), n - 1).map(|v| v as u32)
}

/// Scalar `product_num` for any slice.
pub fn product_num_slice(b: &[u8]) -> Option<u32> {
    let n = b.len();
    if n < 2 || n > 8 || b[0] != b'p' || (b[1] == b'0' && n > 2) {
        return None;
    }
    let mut v = 0u32;
    for &c in &b[1..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u32;
    }
    Some(v)
}
