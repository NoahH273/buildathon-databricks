//! Linked IDs: parallel union-find over hashed IDs; each person is named by its byte-wise
//! smallest ID. The result maps a user-ID hash to a dense component index.

use crate::parse::{comp_person, U64Map};
use std::time::Instant;

pub const LB: u32 = 6; // user-hash partitions for building and probing
const NP: usize = 1 << LB;

#[derive(Clone, Copy)]
pub struct LinkRec {
    pub ha: u64,
    pub hb: u64,
    pub ra: u64,
    pub rb: u64,
}

/// user hash -> component index, partitioned by the top LB bits of the hash.
pub struct LinkMap {
    slots: Vec<[u64; 2]>,
    meta: [(u32, u32); NP], // (offset, mask)
}

impl LinkMap {
    #[inline(always)]
    fn nz(k: u64) -> u64 {
        if k == 0 {
            1
        } else {
            k
        }
    }
    #[inline(always)]
    fn home(k: u64) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 24) as usize
    }
    #[inline(always)]
    fn slot_index(&self, k: u64) -> (usize, usize, usize) {
        let (off, mask) = self.meta[(k >> (64 - LB)) as usize];
        (off as usize, mask as usize, Self::home(k) & mask as usize)
    }
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        let k = Self::nz(k);
        let (off, _, i) = self.slot_index(k);
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                self.slots.as_ptr().add(off + i) as *const i8,
                std::arch::x86_64::_MM_HINT_T0,
            );
        }
    }
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<u64> {
        let k = Self::nz(k);
        let (off, mask, mut i) = self.slot_index(k);
        loop {
            let s = unsafe { *self.slots.get_unchecked(off + i) };
            if s[0] == k {
                return Some(s[1]);
            }
            if s[0] == 0 {
                return None;
            }
            i = (i + 1) & mask;
        }
    }
    /// Person key and name word for a user hash and that user's own name ref.
    #[inline(always)]
    pub fn resolve(&self, user: u64, own_name: u64) -> (u64, u64) {
        match self.get(user) {
            Some(ci) => (comp_person(ci), crate::LINKED_BIT | ci),
            None => (user, own_name),
        }
    }
}

pub struct Linked {
    pub map: LinkMap,
    pub names: Vec<u64>, // component index -> ref of its smallest ID
}

#[derive(Clone, Copy)]
struct Entry {
    h: u64,
    r: u64,
    slot: u32,
}

/// Big-endian first 8 bytes, zero padded: orders like the bytes up to ties.
fn prefix8(s: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    let n = s.len().min(8);
    b[..n].copy_from_slice(&s[..n]);
    u64::from_be_bytes(b)
}

struct SyncPtr<T>(*mut T);
unsafe impl<T> Sync for SyncPtr<T> {}
unsafe impl<T> Send for SyncPtr<T> {}

pub fn build<'a, F>(per_thread: Vec<Vec<LinkRec>>, nthreads: usize, ref_str: F) -> Linked
where
    F: Fn(u64) -> &'a [u8] + Sync,
{
    let t = Instant::now();
    let nrec: usize = per_thread.iter().map(|v| v.len()).sum();
    let mut rec_off = Vec::with_capacity(per_thread.len());
    let mut acc = 0usize;
    for v in &per_thread {
        rec_off.push(acc);
        acc += v.len();
    }
    // L2: scatter endpoints by hash partition (one task per input vector)
    let ents: Vec<Vec<Vec<Entry>>> = std::thread::scope(|s| {
        let hs: Vec<_> = per_thread
            .iter()
            .zip(rec_off.iter())
            .map(|(v, &off)| {
                s.spawn(move || {
                    let mut out: Vec<Vec<Entry>> = (0..NP).map(|_| Vec::with_capacity(v.len() * 2 / NP + 16)).collect();
                    for (j, r) in v.iter().enumerate() {
                        let g = (off + j) as u32;
                        out[(r.ha >> (64 - LB)) as usize].push(Entry { h: r.ha, r: r.ra, slot: 2 * g });
                        out[(r.hb >> (64 - LB)) as usize].push(Entry { h: r.hb, r: r.rb, slot: 2 * g + 1 });
                    }
                    out
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    // L3a: per partition, dense local ids
    struct Part {
        hashes: Vec<u64>,
        keys: Vec<(u64, u64)>, // (prefix8, ref)
        ent_ids: Vec<(u32, u32)>, // (slot, local id)
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let parts: Vec<Part> = {
        let mut res: Vec<Option<Part>> = (0..NP).map(|_| None).collect();
        let res_ptr = SyncPtr(res.as_mut_ptr());
        std::thread::scope(|s| {
            for _ in 0..nthreads {
                let ents = &ents;
                let next = &next;
                let ref_str = &ref_str;
                let res_ptr = &res_ptr;
                s.spawn(move || loop {
                    let p = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if p >= NP {
                        break;
                    }
                    let cnt: usize = ents.iter().map(|e| e[p].len()).sum();
                    let mut map = U64Map::with_capacity(cnt);
                    let mut part = Part { hashes: Vec::new(), keys: Vec::new(), ent_ids: Vec::with_capacity(cnt) };
                    for e in ents.iter() {
                        for en in &e[p] {
                            let n = part.hashes.len() as u32;
                            let id = map.insert(en.h, n);
                            if id == n {
                                part.hashes.push(en.h);
                                part.keys.push((prefix8(ref_str(en.r)), en.r));
                            }
                            part.ent_ids.push((en.slot, id));
                        }
                    }
                    unsafe { *res_ptr.0.add(p) = Some(part) };
                });
            }
        });
        res.into_iter().map(|p| p.unwrap()).collect()
    };
    drop(ents);
    let mut node_off = vec![0u32; NP + 1];
    for p in 0..NP {
        node_off[p + 1] = node_off[p] + parts[p].hashes.len() as u32;
    }
    let n = node_off[NP] as usize;
    // L3b: endpoint ids
    let mut ids = vec![0u32; 2 * nrec];
    {
        let ids_ptr = SyncPtr(ids.as_mut_ptr());
        let next = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..nthreads {
                let parts = &parts;
                let node_off = &node_off;
                let next = &next;
                let ids_ptr = &ids_ptr;
                s.spawn(move || loop {
                    let p = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if p >= NP {
                        break;
                    }
                    let base = node_off[p];
                    for &(slot, local) in &parts[p].ent_ids {
                        unsafe { *ids_ptr.0.add(slot as usize) = base + local };
                    }
                });
            }
        });
    }
    let t_ids = t.elapsed();
    // L4: union-find
    let mut parent: Vec<u32> = (0..n as u32).collect();
    #[inline(always)]
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        unsafe {
            while *parent.get_unchecked(x as usize) != x {
                let p = *parent.get_unchecked(x as usize);
                let gp = *parent.get_unchecked(p as usize);
                *parent.get_unchecked_mut(x as usize) = gp;
                x = gp;
            }
        }
        x
    }
    for e in 0..nrec {
        let a = find(&mut parent, ids[2 * e]);
        let b = find(&mut parent, ids[2 * e + 1]);
        if a != b {
            if a < b {
                parent[b as usize] = a;
            } else {
                parent[a as usize] = b;
            }
        }
    }
    let t_uf = t.elapsed();
    // L5-7: roots, smallest name per root, component numbering
    let mut best: Vec<u32> = vec![u32::MAX; n];
    let mut keys: Vec<(u64, u64)> = Vec::with_capacity(n);
    for p in 0..NP {
        keys.extend_from_slice(&parts[p].keys);
    }
    let mut root: Vec<u32> = vec![0; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        root[i] = r;
        let b = best[r as usize];
        if b == u32::MAX {
            best[r as usize] = i as u32;
        } else {
            let (pi, ri) = keys[i];
            let (pb, rb) = keys[b as usize];
            if pi < pb || (pi == pb && ref_str(ri) < ref_str(rb)) {
                best[r as usize] = i as u32;
            }
        }
    }
    let mut comp_of_root: Vec<u32> = vec![u32::MAX; n];
    let mut names: Vec<u64> = Vec::new();
    let mut comp_of_node: Vec<u32> = vec![0; n];
    for i in 0..n {
        let r = root[i] as usize;
        if comp_of_root[r] == u32::MAX {
            comp_of_root[r] = names.len() as u32;
            names.push(keys[best[r] as usize].1);
        }
        comp_of_node[i] = comp_of_root[r];
    }
    let t_names = t.elapsed();
    // L8: partitioned probe table
    let mut meta = [(0u32, 0u32); NP];
    let mut total = 0usize;
    for p in 0..NP {
        let cap = ((parts[p].hashes.len().max(4) * 7) / 4).next_power_of_two();
        meta[p] = (total as u32, (cap - 1) as u32);
        total += cap;
    }
    let mut slots: Vec<[u64; 2]> = vec![[0, 0]; total];
    {
        let sp = SyncPtr(slots.as_mut_ptr());
        let next = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..nthreads {
                let parts = &parts;
                let node_off = &node_off;
                let comp_of_node = &comp_of_node;
                let meta = &meta;
                let next = &next;
                let sp = &sp;
                s.spawn(move || loop {
                    let p = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if p >= NP {
                        break;
                    }
                    let (off, mask) = meta[p];
                    for (local, &h) in parts[p].hashes.iter().enumerate() {
                        let k = LinkMap::nz(h);
                        let mut i = LinkMap::home(k) & mask as usize;
                        let v = comp_of_node[node_off[p] as usize + local] as u64;
                        loop {
                            let slot = unsafe { &mut *sp.0.add(off as usize + i) };
                            if slot[0] == 0 {
                                *slot = [k, v];
                                break;
                            }
                            i = (i + 1) & mask as usize;
                        }
                    }
                });
            }
        });
    }
    eprintln!(
        "links: {} recs, {} ids, {} comps; ids {:?} uf {:?} names {:?} total {:?}",
        nrec,
        n,
        names.len(),
        t_ids,
        t_uf,
        t_names,
        t.elapsed()
    );
    Linked { map: LinkMap { slots, meta }, names }
}
