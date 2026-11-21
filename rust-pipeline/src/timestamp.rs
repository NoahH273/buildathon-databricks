pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const GAP: i64 = 1_800_000;

pub fn decimal(value: &str) -> Option<i64> {
    if value.is_empty() {
        return None;
    }
    value.bytes().try_fold(0i64, |n, b| {
        if !b.is_ascii_digit() {
            return None;
        }
        n.checked_mul(10)?.checked_add(i64::from(b - b'0'))
    })
}

// Strict Gregorian calendar validation followed by days_from_civil (Hinnant).
pub fn parse(value: &str) -> Option<i64> {
    let b = value.as_bytes();
    if !b.is_empty() && b.iter().all(u8::is_ascii_digit) {
        return decimal(value);
    }
    if b.len() != 20 && b.len() != 25 {
        return None;
    }
    for (at, expected) in [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')] {
        if b[at] != expected {
            return None;
        }
    }
    let number = |start, end| decimal(std::str::from_utf8(&b[start..end]).ok()?);
    let year = number(0, 4)?;
    let month = number(5, 7)?;
    let day = number(8, 10)?;
    let hour = number(11, 13)?;
    let minute = number(14, 16)?;
    let second = number(17, 19)?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        _ => return None,
    };
    if !(1..=max_day).contains(&day) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let offset = if b.len() == 20 {
        if b[19] != b'Z' {
            return None;
        }
        0
    } else {
        if !matches!(b[19], b'+' | b'-') || b[22] != b':' {
            return None;
        }
        let h = number(20, 22)?;
        let m = number(23, 25)?;
        if h > 23 || m > 59 {
            return None;
        }
        (h * 60 + m) * if b[19] == b'-' { -1 } else { 1 }
    };
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    Some(days * DAY + ((hour * 60 + minute - offset) * 60 + second) * 1000)
}

pub fn in_window(value: &str, start: i64, end: i64) -> Option<i64> {
    // Huge decimal tokens cannot fit in any supported manifest window. No wrapping/coercion.
    parse(value).filter(|&ms| start <= ms && ms < end)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_forms() {
        assert_eq!(parse("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse("1969-12-31T23:59:59Z"), Some(-1000));
        assert_eq!(
            parse("2028-03-01T00:00:00+01:00"),
            parse("2028-02-29T23:00:00Z")
        );
        assert_eq!(
            parse("2026-10-03T19:58:10-04:00"),
            parse("2026-10-03T23:58:10Z")
        );
        assert_eq!(parse("1970-01-01T00:00:00-00:00"), Some(0));
        assert_eq!(parse("00000000000000000000086400000"), Some(DAY));
        for value in [
            "",
            "2027-02-29T00:00:00Z",
            "1900-02-29T00:00:00Z",
            "2028-02-30T00:00:00Z",
            "2028-13-01T00:00:00Z",
            "2028-01-01T24:00:00Z",
            "2028-01-01T00:00:60Z",
            "2028-01-01t00:00:00Z",
            "2028-01-01T00:00:00z",
            "2028-01-01T00:00:00Z\r",
            "2028-01-01T00:00:00+24:00",
            "2028-01-01T00:00:00+05:60",
            "2028-01-01T00:00:00+0200",
            "2028-01-01T00:00:00.0Z",
            "1.0",
            "1e12",
            "-5",
            "٢٥٠",
            "２５０",
            "99999999999999999999999999999",
        ] {
            assert_eq!(parse(value), None, "{value}");
        }
        assert!(parse("2000-02-29T00:00:00Z").is_some());
        assert_eq!(in_window("100", 100, 101), Some(100));
        assert_eq!(in_window("101", 100, 101), None);
    }
}
