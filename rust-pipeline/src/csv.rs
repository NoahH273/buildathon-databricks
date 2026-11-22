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

/// Iterate LF-delimited records, including a final unterminated record.
pub fn lines(mut data: &[u8]) -> impl Iterator<Item = &[u8]> {
    std::iter::from_fn(move || {
        if data.is_empty() {
            return None;
        }
        if let Some(end) = memchr::memchr(b'\n', data) {
            let row = &data[..end];
            data = &data[end + 1..];
            Some(row)
        } else {
            Some(std::mem::take(&mut data))
        }
    })
}

/// Remove each file's header once and share complete rows between workers.
pub fn chunks(files: &[Vec<u8>]) -> Vec<&[u8]> {
    const TARGET: usize = 512 * 1024;
    let mut chunks = Vec::new();
    for file in files {
        let Some(header) = memchr::memchr(b'\n', file) else {
            continue;
        };
        let mut data = &file[header + 1..];
        while !data.is_empty() {
            let end = data
                .get(TARGET..)
                .and_then(|tail| memchr::memchr(b'\n', tail))
                .map_or(data.len(), |i| TARGET + i + 1);
            let (chunk, rest) = data.split_at(end);
            chunks.push(chunk);
            data = rest;
        }
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chunk_boundaries_preserve_long_and_unterminated_rows() {
        let long = vec![b'x'; 512 * 1024 + 17];
        let mut part = b"header\nfirst\n\n".to_vec();
        part.extend_from_slice(&long);
        part.extend_from_slice(b"\nlast\r");
        let files = [
            b"header".to_vec(),
            b"header\n".to_vec(),
            part,
            b"h\nnext\n".to_vec(),
        ];
        let pieces = chunks(&files);
        assert!(pieces.len() > 2);
        let actual: Vec<_> = pieces.iter().flat_map(|piece| lines(piece)).collect();
        assert_eq!(
            actual,
            [b"first".as_slice(), b"", &long, b"last\r", b"next"]
        );
    }
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
