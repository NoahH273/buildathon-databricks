//! Partitioned append with small write-combining staging buffers: records for one partition are
//! collected B at a time in a compact (cache-resident) staging area and copied out together, so a
//! destination page is touched once per B records instead of once per record.

pub struct Scatter<T: Copy, const B: usize> {
    stage: Vec<T>,
    fill: Vec<u32>,
    out: Vec<Vec<T>>,
}

impl<T: Copy, const B: usize> Scatter<T, B> {
    pub fn new(parts: usize, cap_each: usize) -> Self {
        let zero: T = unsafe { std::mem::zeroed() };
        let out: Vec<Vec<T>> = (0..parts).map(|_| Vec::with_capacity(cap_each)).collect();
        Scatter {
            stage: vec![zero; parts * B],
            fill: vec![0; parts],
            out,
        }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
        unsafe {
            let f = self.fill.get_unchecked_mut(p);
            let base = p * B;
            *self.stage.get_unchecked_mut(base + *f as usize) = x;
            *f += 1;
            if *f as usize == B {
                *f = 0;
                let src = self.stage.as_ptr().add(base);
                let o = self.out.get_unchecked_mut(p);
                o.reserve(B);
                std::ptr::copy_nonoverlapping(src, o.as_mut_ptr().add(o.len()), B);
                o.set_len(o.len() + B);
            }
        }
    }

    pub fn finish(mut self) -> Vec<Vec<T>> {
        for p in 0..self.fill.len() {
            let f = self.fill[p] as usize;
            let base = p * B;
            self.out[p].extend_from_slice(&self.stage[base..base + f]);
        }
        self.out
    }
}
