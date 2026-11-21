//! Exact source dialect and integer timestamp conversion, independent of Arrow.
use anyhow::Result;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

pub const DAY: i64 = 86_400_000;
pub const WINDOW: i64 = 7 * DAY;

pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut result = [&b""[..]; N];
    let mut count = 0;
    for field in line.split(|&byte| byte == b',') {
        if count == N {
            return None;
        }
        let value = if field.first() == Some(&b'"') {
            if field.len() < 2 || field.last() != Some(&b'"') {
                return None;
            }
            &field[1..field.len() - 1]
        } else {
            field
        };
        if value.contains(&b'"') {
            return None;
        }
        result[count] = value;
        count += 1;
    }
    (count == N).then_some(result)
}

pub fn scan<const N: usize>(
    path: &Path,
    mut consume: impl FnMut([&[u8]; N]) -> Result<()>,
) -> Result<()> {
    let mut reader = BufReader::with_capacity(256 * 1024, File::open(path)?);
    let mut line = Vec::with_capacity(1024);
    reader.read_until(b'\n', &mut line)?; // exactly the first physical record
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if let Some(values) = fields(&line) {
            consume(values)?;
        }
    }
    Ok(())
}

pub fn decimal(value: &[u8]) -> Option<i64> {
    if value.is_empty() {
        return None;
    }
    let mut result = 0i64;
    for &digit in value {
        if !digit.is_ascii_digit() {
            return None;
        }
        result = result.checked_mul(10)?.checked_add((digit - b'0') as i64)?;
    }
    Some(result)
}

fn number(value: &[u8]) -> Option<i64> {
    decimal(value)
}

pub fn timestamp(value: &[u8]) -> Option<i64> {
    if !value.is_empty() && value[0].is_ascii_digit() && value.iter().all(u8::is_ascii_digit) {
        return decimal(value);
    }
    if !matches!(value.len(), 20 | 25) {
        return None;
    }
    for (index, byte) in [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')] {
        if value[index] != byte {
            return None;
        }
    }
    let year = number(&value[..4])?;
    let month = number(&value[5..7])?;
    let day = number(&value[8..10])?;
    let hour = number(&value[11..13])?;
    let minute = number(&value[14..16])?;
    let second = number(&value[17..19])?;
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day < 1 || day > days {
        return None;
    }
    // Gregorian civil date to Unix epoch days, including pre-1970 dates.
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let epoch_day = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    let mut result = epoch_day * DAY + (hour * 3600 + minute * 60 + second) * 1000;
    if value.len() == 20 {
        if value[19] != b'Z' {
            return None;
        }
    } else {
        if !matches!(value[19], b'+' | b'-') || value[22] != b':' {
            return None;
        }
        let hours = number(&value[20..22])?;
        let minutes = number(&value[23..25])?;
        if hours > 23 || minutes > 59 {
            return None;
        }
        let offset = (hours * 60 + minutes) * 60_000;
        result -= if value[19] == b'+' { offset } else { -offset };
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dialect() {
        assert_eq!(
            fields::<2>(b"\" u1\",u2\r"),
            Some([&b" u1"[..], &b"u2\r"[..]])
        );
        assert_eq!(fields::<2>(b"\"\","), Some([&b""[..], &b""[..]]));
        for bad in [b"a,b,c".as_slice(), b"a,b\"", b"a,\"b\"x", b"a", b""] {
            assert!(fields::<2>(bad).is_none());
        }
        assert!(fields::<2>(b"user_id_a,user_id_b").is_some());
    }
    #[test]
    fn timestamps_and_numbers() {
        assert_eq!(timestamp(b"1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(timestamp(b"1969-12-31T23:59:59Z"), Some(-1000));
        assert_eq!(
            timestamp(b"2028-02-29T01:00:00+01:00"),
            timestamp(b"2028-02-29T00:00:00Z")
        );
        assert_eq!(
            timestamp(b"2026-10-04T23:59:00+23:59"),
            timestamp(b"2026-10-04T00:00:00Z")
        );
        assert_eq!(
            timestamp(b"2026-10-04T00:00:00-00:00"),
            timestamp(b"2026-10-04T00:00:00Z")
        );
        assert_eq!(
            decimal(format!("{}250", "0".repeat(1000)).as_bytes()),
            Some(250)
        );
        for bad in [
            "2026-02-29T00:00:00Z",
            "2028-02-30T00:00:00Z",
            "2026-10-01T24:00:00Z",
            "2026-10-01T00:00:60Z",
            "2026-10-01T00:00:00+24:00",
            "2026-10-01T00:00:00+05:60",
            "2026-10-01T00:00:00+0200",
            "2026-10-01T00:00:00Z\r",
            "٢٥٠",
            "1791071890000.0",
            "2026-10-01t00:00:00Z",
            "2026-10-01T00:00:00z",
            "-1",
            "999999999999999999999",
        ] {
            assert!(timestamp(bad.as_bytes()).is_none(), "{bad}");
        }
    }
}
