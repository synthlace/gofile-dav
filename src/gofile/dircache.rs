use std::collections::HashMap;

pub const ROOT_DIR: &str = "";

pub struct DirCache<T> {
    cache: HashMap<String, T>,
}

impl<T> DirCache<T> {
    pub fn new(root_id: T) -> Self {
        let mut cache = HashMap::new();
        cache.insert(ROOT_DIR.to_string(), root_id);
        Self { cache }
    }

    pub fn find_dir(&self, path: impl AsRef<str>) -> Option<&T> {
        self.cache.get(path.as_ref())
    }

    pub fn insert_dir(&mut self, path: impl Into<String>, value: T) -> Option<T> {
        self.cache.insert(path.into(), value)
    }
}
