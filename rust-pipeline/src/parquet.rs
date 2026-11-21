//! Minimal Parquet writer: one row group, one PLAIN uncompressed data page (v1) per column.

use std::io::Write;

pub const T_INT32: i32 = 1;
pub const T_INT64: i32 = 2;
pub const T_BYTE_ARRAY: i32 = 6;

pub enum Logical {
    None,
    String,
    Date,
}

pub struct Column {
    pub name: &'static str,
    pub ptype: i32,
    pub logical: Logical,
    pub optional: bool,
    /// Page body: for optional columns, the 4-byte-length-prefixed definition levels, then values.
    pub body: Vec<u8>,
}

impl Column {
    pub fn i64s(name: &'static str, v: &[i64]) -> Column {
        let mut body = Vec::with_capacity(v.len() * 8);
        for x in v {
            body.extend_from_slice(&x.to_le_bytes());
        }
        Column { name, ptype: T_INT64, logical: Logical::None, optional: false, body }
    }
    pub fn dates(name: &'static str, v: &[i32]) -> Column {
        let mut body = Vec::with_capacity(v.len() * 4);
        for x in v {
            body.extend_from_slice(&x.to_le_bytes());
        }
        Column { name, ptype: T_INT32, logical: Logical::Date, optional: false, body }
    }
    /// `plain` is already PLAIN-encoded (u32 length prefix + bytes per value).
    pub fn strings(name: &'static str, plain: Vec<u8>) -> Column {
        Column { name, ptype: T_BYTE_ARRAY, logical: Logical::String, optional: false, body: plain }
    }
    /// Optional strings: `defs[i]` says whether row i is present; `plain` holds only present values.
    pub fn opt_strings(name: &'static str, defs: &[bool], plain: &[u8]) -> Column {
        let n = defs.len();
        let mut levels = Vec::new();
        if n > 0 {
            let groups = (n + 7) / 8;
            varint(&mut levels, ((groups as u64) << 1) | 1);
            let mut byte = 0u8;
            for (i, &d) in defs.iter().enumerate() {
                if d {
                    byte |= 1 << (i & 7);
                }
                if i & 7 == 7 {
                    levels.push(byte);
                    byte = 0;
                }
            }
            if n & 7 != 0 {
                levels.push(byte);
            }
        }
        let mut body = Vec::with_capacity(4 + levels.len() + plain.len());
        body.extend_from_slice(&(levels.len() as u32).to_le_bytes());
        body.extend_from_slice(&levels);
        body.extend_from_slice(plain);
        Column { name, ptype: T_BYTE_ARRAY, logical: Logical::String, optional: true, body }
    }
}

pub fn put_plain_str(buf: &mut Vec<u8>, s: &[u8]) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s);
}

fn varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn zz64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// Thrift compact protocol writer.
struct Tw {
    buf: Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BINARY: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

impl Tw {
    fn new() -> Tw {
        Tw { buf: Vec::new(), last: 0, stack: Vec::new() }
    }
    fn fh(&mut self, ty: u8, id: i16) {
        let d = id - self.last;
        if d > 0 && d <= 15 {
            self.buf.push(((d as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            varint(&mut self.buf, zz64(id as i64));
        }
        self.last = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.fh(CT_I32, id);
        varint(&mut self.buf, zz64(v as i64));
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.fh(CT_I64, id);
        varint(&mut self.buf, zz64(v));
    }
    fn binf(&mut self, id: i16, b: &[u8]) {
        self.fh(CT_BINARY, id);
        varint(&mut self.buf, b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn struct_begin(&mut self, id: i16) {
        self.fh(CT_STRUCT, id);
        self.stack.push(self.last);
        self.last = 0;
    }
    fn struct_end(&mut self) {
        self.buf.push(0);
        self.last = self.stack.pop().unwrap();
    }
    fn list_begin(&mut self, id: i16, elem: u8, size: usize) {
        self.fh(CT_LIST, id);
        if size < 15 {
            self.buf.push(((size as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            varint(&mut self.buf, size as u64);
        }
    }
    fn elem_struct_begin(&mut self) {
        self.stack.push(self.last);
        self.last = 0;
    }
    fn elem_i32(&mut self, v: i32) {
        varint(&mut self.buf, zz64(v as i64));
    }
    fn elem_bin(&mut self, b: &[u8]) {
        varint(&mut self.buf, b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn stop(&mut self) {
        self.buf.push(0);
    }
}

fn page_header(body_len: usize, num_values: usize) -> Vec<u8> {
    let mut t = Tw::new();
    t.i32f(1, 0); // DATA_PAGE
    t.i32f(2, body_len as i32);
    t.i32f(3, body_len as i32);
    t.struct_begin(5);
    t.i32f(1, num_values as i32);
    t.i32f(2, 0); // PLAIN
    t.i32f(3, 3); // RLE
    t.i32f(4, 3); // RLE
    t.struct_end();
    t.stop();
    t.buf
}

pub fn write_file(path: &std::path::Path, num_rows: usize, cols: &[Column]) -> std::io::Result<()> {
    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(b"PAR1");
    // (offset, total size) per column
    let mut chunks: Vec<(i64, i64)> = Vec::new();
    if num_rows > 0 {
        for c in cols {
            let off = out.len() as i64;
            let h = page_header(c.body.len(), num_rows);
            out.extend_from_slice(&h);
            out.extend_from_slice(&c.body);
            chunks.push((off, (h.len() + c.body.len()) as i64));
        }
    }
    let mut t = Tw::new();
    t.i32f(1, 1); // version
    t.list_begin(2, CT_STRUCT, cols.len() + 1);
    // root
    t.elem_struct_begin();
    t.binf(4, b"schema");
    t.i32f(5, cols.len() as i32);
    t.struct_end();
    for c in cols {
        t.elem_struct_begin();
        t.i32f(1, c.ptype);
        t.i32f(3, if c.optional { 1 } else { 0 });
        t.binf(4, c.name.as_bytes());
        match c.logical {
            Logical::None => {}
            Logical::String => {
                t.i32f(6, 0); // UTF8
                t.struct_begin(10);
                t.struct_begin(1);
                t.struct_end();
                t.struct_end();
            }
            Logical::Date => {
                t.i32f(6, 6); // DATE
                t.struct_begin(10);
                t.struct_begin(6);
                t.struct_end();
                t.struct_end();
            }
        }
        t.struct_end();
    }
    t.i64f(3, num_rows as i64);
    if num_rows > 0 {
        t.list_begin(4, CT_STRUCT, 1);
        t.elem_struct_begin();
        t.list_begin(1, CT_STRUCT, cols.len());
        let mut total = 0i64;
        for (c, &(off, size)) in cols.iter().zip(chunks.iter()) {
            total += size;
            t.elem_struct_begin();
            t.i64f(2, off);
            t.struct_begin(3);
            t.i32f(1, c.ptype);
            t.list_begin(2, CT_I32, 2);
            t.elem_i32(0);
            t.elem_i32(3);
            t.list_begin(3, CT_BINARY, 1);
            t.elem_bin(c.name.as_bytes());
            t.i32f(4, 0); // UNCOMPRESSED
            t.i64f(5, num_rows as i64);
            t.i64f(6, size);
            t.i64f(7, size);
            t.i64f(9, off);
            t.struct_end();
            t.struct_end();
        }
        t.i64f(2, total);
        t.i64f(3, num_rows as i64);
        t.struct_end();
    } else {
        t.list_begin(4, CT_STRUCT, 0);
    }
    t.binf(6, b"shopping-sessions-rust");
    t.stop();
    out.extend_from_slice(&t.buf);
    out.extend_from_slice(&(t.buf.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path)?;
    f.write_all(&out)?;
    Ok(())
}
