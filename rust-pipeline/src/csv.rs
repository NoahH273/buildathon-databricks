//! The challenge dialect is deliberately narrower than RFC CSV. Only LF ends a row.
use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
};

pub fn parse<const N: usize>(line: &[u8]) -> Option<[&str; N]> {
    let text = std::str::from_utf8(line).ok()?;
    if text.is_empty() {
        return None;
    }
    let mut fields = [""; N];
    let mut count = 0;
    for field in text.split(',') {
        if count == N {
            return None;
        }
        let value = if field.starts_with('"') && field.ends_with('"') && field.len() >= 2 {
            &field[1..field.len() - 1]
        } else {
            field
        };
        if value.contains(['"', '\n']) {
            return None;
        }
        fields[count] = value;
        count += 1;
    }
    (count == N).then_some(fields)
}

pub fn files(directory: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.path().extension().is_some_and(|x| x == "csv") {
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}

pub fn read<const N: usize>(
    path: &Path,
    mut accept: impl FnMut([&str; N], &[u8]),
) -> io::Result<()> {
    let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?; // Exactly one header per file, including header-only parts.
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if let Some(fields) = parse(&line) {
            accept(fields, &line);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_dialect() {
        assert_eq!(parse::<3>(b"\"\", u1,\"x\""), Some(["", " u1", "x"]));
        assert_eq!(parse::<2>(b"u1,100\r"), Some(["u1", "100\r"]));
        for row in [
            b"".as_slice(),
            b"a",
            b"a,b,c",
            b"a,\"b",
            b"a,b\"",
            b"a,\"b\"x",
            b"a,\"b\"\"",
            b"a,\xff",
            b"a,b\n",
        ] {
            assert_eq!(parse::<2>(row), None, "{row:?}");
        }
    }
}
