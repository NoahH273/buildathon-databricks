//! CSV dialect, timestamps, prices, and hashing.

pub const DAY_MS: i64 = 86_400_000;

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

/// 64-bit hash of a byte string. Short strings (the common case) use two overlapping loads
/// and two 128-bit multiplies; long ones fall back to xxh3.
#[inline(always)]
pub fn hash(b: &[u8]) -> u64 {
    let n = b.len();
    let (a, c) = if n >= 8 {
        if n > 16 {
            return xxhash_rust::xxh3::xxh3_64(b) ^ 0x9e3779b97f4a7c15;
        }
        unsafe {
            (
                (b.as_ptr() as *const u64).read_unaligned(),
                (b.as_ptr().add(n - 8) as *const u64).read_unaligned(),
            )
        }
    } else if n >= 4 {
        unsafe {
            (
                (b.as_ptr() as *const u32).read_unaligned() as u64,
                (b.as_ptr().add(n - 4) as *const u32).read_unaligned() as u64,
            )
        }
    } else if n > 0 {
        (((b[0] as u64) << 16) | ((b[n >> 1] as u64) << 8) | (b[n - 1] as u64), 0)
    } else {
        (0, 0)
    };
    let x = mum(a ^ 0x2d358dccaa6c78a5, c ^ 0x8bb84b93962eacc9 ^ (n as u64));
    mum(x ^ 0x4b33a62ed433d4a3, (n as u64) ^ 0x4d5a2da51de1aa47)
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

/// Bitmasks of '\n', ',' and '"' over the 128 bytes starting at p.
#[inline(always)]
pub unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    let nlv = _mm256_set1_epi8(b'\n' as i8);
    let cv = _mm256_set1_epi8(b',' as i8);
    let qv = _mm256_set1_epi8(b'"' as i8);
    let mut nl = [0u32; 4];
    let mut cm = [0u32; 4];
    let mut qt = [0u32; 4];
    for k in 0..4 {
        let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
        nl[k] = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nlv)) as u32;
        cm[k] = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, cv)) as u32;
        qt[k] = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, qv)) as u32;
    }
    let j = |a: [u32; 4]| -> u128 {
        (a[0] as u128) | ((a[1] as u128) << 32) | ((a[2] as u128) << 64) | ((a[3] as u128) << 96)
    };
    (j(nl), j(cm), j(qt))
}

#[inline(always)]
fn range_mask(s: u32, e: u32) -> u128 {
    // bits [s, e); e <= 127
    ((1u128 << e) - 1) & !((1u128 << s) - 1)
}

/// Fields of a line of length l (< 128) from its comma/quote masks. Returns false if malformed.
#[inline(always)]
pub fn fields_from_masks<const N: usize>(l: u32, cm: u128, qt: u128, f: &mut [(u32, u32); N]) -> bool {
    if l == 0 {
        return false;
    }
    let m = (1u128 << l) - 1;
    let mut c = cm & m;
    let q = qt & m;
    if c.count_ones() as usize != N - 1 {
        return false;
    }
    let mut s = 0u32;
    for k in 0..N {
        let e = if k + 1 < N {
            let e = c.trailing_zeros();
            c &= c - 1;
            e
        } else {
            l
        };
        f[k] = (s, e);
        s = e + 1;
    }
    if q != 0 {
        for k in 0..N {
            let (s, e) = f[k];
            let qq = q & range_mask(s, e);
            if qq != 0 {
                if e - s >= 2 && qq == ((1u128 << s) | (1u128 << (e - 1))) {
                    f[k] = (s + 1, e - 1);
                } else {
                    return false;
                }
            }
        }
    }
    true
}

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
    while pos < end {
        if pos + 128 <= len {
            let (nl, cm, qt) = unsafe { masks128(ptr.add(pos)) };
            if nl != 0 {
                let l = nl.trailing_zeros();
                if fields_from_masks::<N>(l, cm, qt, &mut fr) {
                    f(pos, &fr);
                }
                pos += l as usize + 1;
                continue;
            }
        }
        let e = match memchr::memchr(b'\n', &data[pos..]) {
            Some(p) => pos + p,
            None => len,
        };
        let mut ff = [(0usize, 0usize); N];
        if split::<N>(&data[pos..e], &mut ff) {
            for k in 0..N {
                fr[k] = (ff[k].0 as u32, ff[k].1 as u32);
            }
            f(pos, &fr);
        }
        pos = e + 1;
    }
}
