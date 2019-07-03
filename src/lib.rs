#![deny(missing_docs)]

//! KvStore implementation. A wrapper around HashMap

use std::collections::HashMap;
use std::string::String;


/// The KvStore type.
///
///A wrapper around a HashMap.
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String,String>,
}

impl KvStore {
    /// Create a new KvStore
    /// ```rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// ```
    pub fn new() -> Self {
        KvStore { store: Default::default() }
    }

    /// Add a key/value pair
    ///
    /// If the key already exists, this will overwrite the value
    /// already stored for that key.
    /// ```rust
    /// # use kvs::KvStore;
    /// # let mut store = KvStore::new();
    /// store.set("key".to_owned(), "value".to_owned());
    /// ```
    pub fn set(&mut self, key:String, value:String) {
        self.store.insert(key, value);
    }

    /// Get the value for a key
    ///
    /// If the key exists it will return the value, otherwise
    /// it will return None
    /// ```rust
    /// # use kvs::KvStore;
    /// # let mut store = KvStore::new();
    /// # store.set("key".to_owned(), "value".to_owned());
    /// let optVal = store.get("key".to_owned()); // Some(val)
    /// let optVal2 = store.get("notPresent".to_owned()); // None()
    /// ```
    pub fn get(&self, key:String) -> Option<String> {
        match self.store.get(&key) {
            Some(value) => Some(value.to_owned()),
            None        => None,
        }
    }

    /// Remove a key/value pair
    /// ```rust
    /// # use kvs::KvStore;
    /// # let mut store = KvStore::new();
    /// # store.set("key".to_owned(), "value".to_owned());
    /// store.remove("key".to_owned());
    /// let val = store.get("key".to_owned()); // None()
    /// ```
    pub fn remove(&mut self, key:String) {
        self.store.remove(&key);
    }
}
