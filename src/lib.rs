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

// fn fill_idx_from_reader(idx: &mut HashMap<String, (FileNo, u64)>,
//                         reader: KvStoreStorageReader,
//                         fileno:FileNo) -> Result<()> {
//     reader.reader.seek(SeekFrom::Start(0));
//     loop {
//         let current_spot = reader.reader.seek(SeekFrom::Current(0))?;
//         match bson::decode_document(&mut reader.reader) {
//             // TODO: This should only break on EndOfStream, otherwise it should rethrow
//             Err(_) => {
//                 break
//             }
//             Ok(doc) => {
//                 let bson = bson::Bson::from(doc);
//                 let deser : KvStoreCmd = bson::from_bson(bson)?;
//                 match deser {
//                     KvStoreCmd::Set(s) => { idx.insert(s.key, (fileno,current_spot)); },
//                     KvStoreCmd::Rm(r)  => { idx.remove(&(r.key)); },
//                 }
//             },
//         }
//     }
//     Ok(())
// }

const MAX_BYTES_IN_FILE : i64 = 1000;

impl KvStore {
    // /// Create a new KvStore
    // /// ```rust
    // /// use kvs::KvStore;
    // /// let mut store = KvStore::new();
    // /// ```
    // fn new() -> Result<Self> {
    //     let default_path = PathBuf::from("kvstore.store");
    //     KvStore::open(&default_path)
    // }

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
    //     loop {
    //         match bson::decode_document(&mut file_idx_file) {
    //             Err(_) => { break },
    //             Ok(doc) => {
    //                 let bson_file = bson::Bson::from(doc);
    //                 let storage : KvStoreStorage = bson::from_bson(bson_file)?;
    //                 let reader = match storage.state {
    //                     KvStoreStorageState::Writable => {
    //                         let mut app = OpenOptions::new()
    //                             .append(true)
    //                             .open(PathBuf::from(storage.path))?;
    //                         writer = Some(app);
    //                         create_reader_from_path(PathBuf::from(storage.path))
    //                     }
    //                     KvStoreStorageState::ReadOnly =>
    //                         create_reader_from_path(PathBuf::from(storage.path)),
    //                     _ => Err(format_err!("Error deserializing files from file index")),
    //                 }?;
    //                 file_idx[&storage.index] = &reader;
    //                 if storage.index > max_idx { max_idx = storage.index };
    //                 fill_idx_from_reader(&mut idx, reader, storage.index)?;
    //             }
    //         }
    //     }
    //     if writer.is_none() {
    //         writer = Some(OpenOptions::new()
    //             .append(true)
    //             .create(true)
    //             .open(path.join(format!("{}.data",max_idx + 1)))?);
    //     }
    //     let write_pos = writer.unwrap().seek(SeekFrom::Current(0))?;
    //     Ok(KvStore {
    //         index: idx,
    //         file_index: file_idx,
    //         writer: writer.unwrap(),
    //         write_pos: write_pos
    //     })
    // }

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
        let (fileno, offset) = self.put_cmd_to_file(cmd)?;
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
                Ok(Some(self.get_value_from_file(fileno, offset)?))
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
        self.put_cmd_to_file(cmd)
    }

    fn compact(&mut self) -> Result<()> {
        Ok(())
    }
}
