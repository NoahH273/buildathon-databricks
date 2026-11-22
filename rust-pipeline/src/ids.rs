use ahash::AHashMap;
use std::sync::Arc;
pub type Id = usize;
#[derive(Default)]
pub struct Interner {
    index: AHashMap<Arc<str>, Id>,
    pub values: Vec<Arc<str>>,
}
impl Interner {
    pub fn lookup(&self, value: &str) -> Option<Id> {
        self.index.get(value).copied()
    }
    pub fn intern(&mut self, value: &str) -> Id {
        if let Some(&id) = self.index.get(value) {
            return id;
        }
        let id = self.values.len();
        let value: Arc<str> = value.into();
        self.values.push(value.clone());
        self.index.insert(value, id);
        id
    }
}
#[derive(Default)]
pub struct People {
    pub names: Interner,
    parent: Vec<Id>,
    size: Vec<usize>,
    minimum: Vec<Id>,
}
impl People {
    pub fn intern(&mut self, value: &str) -> Id {
        let id = self.names.intern(value);
        if id == self.parent.len() {
            self.parent.push(id);
            self.size.push(1);
            self.minimum.push(id);
        }
        id
    }
    fn root(&mut self, mut id: Id) -> Id {
        while self.parent[id] != id {
            self.parent[id] = self.parent[self.parent[id]];
            id = self.parent[id];
        }
        id
    }
    pub fn union(&mut self, a: &str, b: &str) {
        let a = self.intern(a);
        let b = self.intern(b);
        let mut a = self.root(a);
        let mut b = self.root(b);
        if a == b {
            return;
        }
        if self.size[a] < self.size[b] {
            std::mem::swap(&mut a, &mut b);
        }
        self.parent[b] = a;
        self.size[a] += self.size[b];
        if self.names.values[self.minimum[b]].as_bytes()
            < self.names.values[self.minimum[a]].as_bytes()
        {
            self.minimum[a] = self.minimum[b];
        }
    }
    /// Consume the link forest and rewrite each exact alias lookup to its
    /// final byte-minimum ID. New event-only names are singleton components.
    pub fn resolve(mut self) -> PersonIds {
        let canonical: Vec<_> = (0..self.parent.len())
            .map(|id| {
                let root = self.root(id);
                self.minimum[root]
            })
            .collect();
        for id in self.names.index.values_mut() {
            *id = canonical[*id];
        }
        PersonIds(self.names)
    }
    #[cfg(test)]
    pub fn person(&mut self, value: &str) -> Id {
        let id = self.intern(value);
        let root = self.root(id);
        self.minimum[root]
    }
}
/// The table includes every linked spelling; multiple names may return one ID.
pub struct PersonIds(Interner);
impl PersonIds {
    pub fn lookup(&self, name: &str) -> Option<Id> {
        self.0.lookup(name)
    }
    pub fn person(&mut self, name: &str) -> Id {
        self.0.intern(name)
    }
    pub fn names(&self) -> &Interner {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transitive_minima_and_shared_device() {
        let mut p = People::default();
        for i in (1..1000).rev() {
            p.union(&format!("u{i}"), &format!("u{}", i - 1));
        }
        p.union("u999", "A_unused");
        p.union("u1", "u0");
        p.union("u0", "u1");
        p.union("u0", "u0");
        for i in 0..1000 {
            let id = p.person(&format!("u{i}"));
            assert_eq!(&*p.names.values[id], "A_unused");
        }
        assert_ne!(p.person("U1"), p.person("u1"));
        assert_ne!(p.person(" u1"), p.person("u1"));
        for i in 0..1000 {
            p.union(&format!("shared{i}"), "device");
        }
        assert_eq!(p.person("shared999"), p.person("device"));
        let mut resolved = p.resolve();
        assert_eq!(resolved.lookup("u1"), resolved.lookup("A_unused"));
        let singleton = resolved.person("new singleton");
        assert_eq!(resolved.lookup("new singleton"), Some(singleton));
        assert_eq!(&*resolved.names().values[singleton], "new singleton");
    }
}
