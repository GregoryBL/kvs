#![deny(missing_docs)]

//! KvStore implementation. A wrapper around HashMap

#[macro_use] extern crate failure;
// #[macro_use] extern crate failure_derive;

use failure::Error;
// use failure::err_msg;

use serde::{Serialize, Deserialize};

use std::collections::HashMap;
use std::string::String;
use std::path::{Path,PathBuf};

mod storage; //::{KvStoreArchive, FileNum, FileOffset};
use storage::{KvStoreArchive, FileNum, FileOffset};

/// Wrapper for a result with IO error
pub type Result<T> = std::result::Result<T, Error>;

/// Serializable representation of a kvs command
#[derive(Debug, Serialize, Deserialize)]
pub enum KvStoreCmd {
    Set(KvStoreSet),
    Rm(KvStoreRm),
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct KvStoreSet {
    key: String,
    val: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct KvStoreRm {
    key: String,
}

/// Initialization process:
/// 1. Read $PATH/index.store to read in filenames and states (KvStoreStorage)
/// 2. Create the file index {idx: {KvStoreStorageState, Reader}}
/// 3. Read in all of the files in order
/// 4. Create the writer



/// The KvStore type.
///
///A wrapper around a HashMap.
pub struct KvStore {
    #[derive(Default)]
    index: HashMap<String, (FileNum, FileOffset)>,
    archive: KvStoreArchive,
}

const MAX_BYTES_IN_FILE : i64 = 1000;

impl KvStore {

    /// Open a serialized KvStore on disk
    ///
    /// Initialization process:
    /// 1. Read <path>/index.store to read in filenames and states (KvStoreStorage)
    /// 2. Create the file index {idx: {KvStoreStorageState, Reader}}
    /// 3. Read in all of the files in order
    /// 4. Create the writer
    pub fn open(path: &Path) -> Result<KvStore> {
        let archive = KvStoreArchive::open(PathBuf::from(path))?;
        let index = archive.generate_index()?;
        Ok(KvStore { index: index, archive: archive })
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
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let set = KvStoreSet { key: key.clone(), val: value };
        let cmd = KvStoreCmd::Set(set);
        let (fileno, offset) = self.archive.write_cmd(cmd)?;
        self.index.insert(key, (fileno, offset));
        Ok(())
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
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some((fileno, offset)) => {
                Ok(Some(self.archive.read_value_from_filenum_at_offset(*fileno, *offset)?))
            },
            None => Ok(None),
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
    pub fn remove(&mut self, key: String) -> Result<()> {
        let rm = KvStoreRm { key: key.clone() };
        let cmd = KvStoreCmd::Rm(rm);
        self.index.remove(&key)
            // We know this won't happen in the cli
            .ok_or(format_err!("Tried to remove nonexistent key"))
            .map(|_| ())?;
        let _ = self.archive.write_cmd(cmd)?;
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        Ok(())
    }
}
