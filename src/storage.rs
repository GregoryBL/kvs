#![deny(missing_docs)]

//! KvStore file storage layer implementation

use serde::{Serialize, Deserialize};

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use crate::{Result, KvStoreCmd};

/// TypeAlias for a file index
pub type FileNum = u64;
/// TypeAlias for an offset in a file
pub type FileOffset = u64;

#[derive(Debug, Serialize, Deserialize)]
enum KvStoreFileState {
    Writable, // Not full
    ReadOnly, // Full
    // Compacting, // Being written to another file
    // Initializing, // Being copied to from another file
}

/// Stored on disk
#[derive(Debug, Serialize, Deserialize)]
struct KvStoreStorage {
    file_num: FileNum,
    path: String,
    // state: KvStoreStorageState,
}

/// Private API to read and write to specific files
#[derive(Debug)]
struct KvStoreFile {
    path: PathBuf,
    reader: File,
    writer: File,
}

impl KvStoreFile {
    /// Open and initialize given a filepath
    fn open(path: PathBuf) -> Result<Self> {
        let mut reader = OpenOptions::new().read(true).create(true)
            .open(path.as_path())?;
        let mut writer = OpenOptions::new().append(true)
            .open(path.as_path())?;
        Ok(KvStoreFile { path: path, reader: reader, writer: writer })
    }

    fn read_value_at_offset(&self, offset: FileOffset) -> Result<String> {
        self.reader.seek(SeekFrom::Start(offset))?;
        let bson = bson::Bson::from(bson::decode_document(&mut self.reader)?);
        let cmd : KvStoreCmd = bson::from_bson(bson)?;
        match cmd {
            KvStoreCmd::Set(s) => Ok(s.val),
            _                  => Err(format_err!("Cmd at offset was not set command")),
        }
    }

    fn record_command(&mut self, cmd: KvStoreCmd) -> Result<FileOffset> {
        let bson_val = bson::to_bson(&cmd)?;
        let doc = bson_val.as_document()
            .ok_or(format_err!("Couldn't form bson doc."))?;
        bson::encode_document(&mut self.writer, doc)?;
        Ok(self.writer.seek(SeekFrom::Current(0))?)
    }
}

impl IntoIterator for KvStoreFile {
    type Item = KvStoreCmd;
    type IntoIter = KvStoreFileCommandIterator;
    fn into_iter(self) -> Self::IntoIter {
        KvStoreFileCommandIterator::new(self.path).expect("Failure making iterator")
    }
}


/// An iterator for commands in a file
///
/// We make a separate iterator so we don't need to worry about
/// the position in the file for concurrent access.
///
/// Since it's append-only we're not worried about anything mutating
/// or disappearing from the file.
struct KvStoreFileCommandIterator {
    // Initialized at 0
    file: File,
}

impl KvStoreFileCommandIterator {
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut reader = OpenOptions::new().read(true).open(path.as_path())?;
        Ok(KvStoreFileCommandIterator { file: reader })
    }
}

impl Iterator for KvStoreFileCommandIterator {
    type Item = KvStoreCmd;
    fn next(&mut self) -> Option<KvStoreCmd> {
        fn get_next_item(file: File) -> Result<KvStoreCmd> {
            let bson = bson::Bson::from(bson::decode_document(&mut file)?);
            let cmd : KvStoreCmd = bson::from_bson(bson)?;
            Ok(cmd)
        }
        match get_next_item(self.file) {
            Ok(_cmd) => Some(_cmd),
            Err(err) => None
        }
    }
}

/// Public API to read and write to the KvStore disk representation
#[derive(Debug)]
pub struct KvStoreArchive {
    path: PathBuf,
    // The paths of all of the data files in order
    paths: Vec<PathBuf>,
    files: HashMap<FileNum, KvStoreFile>,
}

impl KvStoreArchive {
    /// Open all files and construct an archive object
    ///
    /// Read the file at $Path/index.store to find the files to use
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut file_idx_file = OpenOptions::new()
            .read(true)
            .create(true)
            .open(path.join("index.store").to_owned())?;
        let paths: Vec<PathBuf> = Vec::new();
        let files: HashMap<FileNum, KvStoreFile> = HashMap::new();
        loop {
            match bson::decode_document(&mut file_idx_file) {
                Err(_) => { break },
                Ok(doc) => {
                    let bson_file = bson::Bson::from(doc);
                    let storage : KvStoreStorage = bson::from_bson(bson_file)?;
                    let kv_file = KvStoreFile::open(PathBuf::from(storage.path))?;

                    paths.push(PathBuf::from(storage.path));
                    files.insert(storage.file_num, kv_file);
                }
            }
        }
        Ok(KvStoreArchive { path: path, paths: paths, files: files})
    }

    pub fn read_value_from_filenum_at_offset(
        &self,
        file_num: FileNum,
        offset: FileOffset,
    ) -> Result<String> {
        let file = self.files[&file_num];
        file.read_value_at_offset(offset)
    }

    //////// NEED TO MAKE THIS KNOW WHAT FILE IT WRITES TO //////////
    pub fn write_cmd(&mut self,
                     cmd: KvStoreCmd) -> Result<(FileNum, FileOffset)> {
        let file = self.files.last()?;
        file.record_command(cmd)
    }

    pub fn generate_index(&self) -> Result<HashMap<String, (FileNum, FileOffset)>> {
        let index: HashMap<String, (FileNum, FileOffset)> = HashMap::new();
        for (file_num, path) in self.paths.enumerate() {
            let it = KvStoreFileCommandIterator { path: path}?;
            let mut position: FileNum = 0;
            for cmd in it {
                match cmd {
                    KvStoreCmd::KvStoreSet(s) => {index.insert(s.key (file_num, position))},
                    KvStoreCmd::KvStoreRm(r) => {index.remove(&r.key)},
                }
            }
        }
        Ok(index)
    }
}
