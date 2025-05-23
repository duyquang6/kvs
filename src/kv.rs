use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

use failure::format_err;
use serde::{Deserialize, Serialize};

use crate::Result;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Creates a `KvStore`.
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        let path = path.as_path();

        Err(format_err!("abcxyz"))
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "cmd", content = "params")]
enum Command {
    Set(String, String),
    Rm(String),
}

#[test]
fn test_serialize() {}

struct LogFile {
    inner: File,
}

impl LogFile {
    fn new(file_path: &str) -> Self {
        let f = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(file_path)
            .expect("cannot open log file");

        Self { inner: f }
    }

    fn write_append(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.seek(std::io::SeekFrom::End(0))?;
        self.inner.write(buf)?;

        Ok(buf.len())
    }

    fn read_until(&mut self, delimiter: char, buf: &mut [u8]) -> Result<usize> {
        const CHUNK_SIZE: usize = 8;
        let mut offset = 0;
        'outer: loop {
            let n: usize = self.inner.read(&mut buf[offset..offset + CHUNK_SIZE])?;
            if n == 0 {
                break;
            }
            for index in offset..offset + n {
                if buf[index] == delimiter as u8 {
                    // Rewind to index delimiter + 1
                    self.inner
                        .seek_relative((index - offset + 1) as i64 - n as i64)?;
                    offset = index + 1;
                    break 'outer;
                }
            }
            offset += n;
        }
        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::LogFile;

    #[test]
    fn test_read_until() {
        let path = format!(
            "{}/{}",
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/test_log_file"
        );
        let mut log_file = LogFile::new(&path);
        let mut buf = [0; 1000];
        let n = log_file
            .read_until('\n', &mut buf)
            .expect("read until failed");
        assert_eq!(n, 4);
        assert_eq!(
            "abc\n",
            str::from_utf8(&buf[0..n]).expect("convert string failed")
        );

        let n = log_file
            .read_until('\n', &mut buf)
            .expect("read until failed");
        assert_eq!(n, 4);
        assert_eq!(
            "xyz\n",
            str::from_utf8(&buf[0..n]).expect("convert string failed")
        );

        let n = log_file
            .read_until('\n', &mut buf)
            .expect("read until failed");
        assert_eq!(n, 3);
        assert_eq!(
            "def",
            str::from_utf8(&buf[0..n]).expect("convert string failed")
        )
    }
}

impl Drop for LogFile {
    fn drop(&mut self) {
        self.inner.flush().expect("flush WAL error");
    }
}

impl Default for LogFile {
    fn default() -> Self {
        Self::new("/tmp/kvs.wal")
    }
}

#[derive(Default)]
pub struct KvStoreLog {
    log_file: LogFile,
}

impl KvStoreLog {
    /// Creates a `KvStore`.
    pub fn new() -> KvStoreLog {
        KvStoreLog {
            log_file: LogFile::default(),
        }
    }

    fn open(path: impl Into<PathBuf>) -> Result<KvStoreLog> {
        let path: PathBuf = path.into();
        let Some(path) = path.as_path().to_str() else {
            return Err(format_err!("cannot convert path"));
        };

        let log_file = LogFile::new(path);

        Ok(KvStoreLog { log_file })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // self.map.insert(key, value);

        let cmd = Command::Set(key, value);
        //  = serde_json::to_string(value)
        // self.log_file.write_append(buf)

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Option<String> {
        // self.map.get(&key).cloned()
        None
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) {
        // self.map.remove(&key);
    }
}
