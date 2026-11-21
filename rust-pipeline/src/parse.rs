//! CSV dialect, timestamps, prices, and hashing.

pub const DAY_MS: i64 = 86_400_000;

#[inline(always)]
pub fn hash(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
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

/// Parse the first 19 bytes `YYYY-MM-DDTHH:MM:SS` into epoch milliseconds (local).
#[inline(always)]
fn parse_datetime(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? as i64 * 100 + d2(b, 2)? as i64;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    Some(((days * 86400) + (h as i64) * 3600 + (mi as i64) * 60 + s as i64) * 1000)
}

/// F1 only (used for ingest_ts).
#[inline(always)]
pub fn parse_f1(b: &[u8]) -> Option<i64> {
    if b.len() != 20 || b[19] != b'Z' {
        return None;
    }
    parse_datetime(b)
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
            return parse_datetime(b);
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
            let local = parse_datetime(b)?;
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
    let mut v: u64 = 0;
    let digits = n - i;
    for &c in &b[i..] {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.wrapping_mul(10).wrapping_add(d as u64);
    }
    if digits > 18 {
        // may not fit; anything this large is out of every window
        return None;
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
