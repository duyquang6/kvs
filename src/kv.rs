use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::PathBuf,
};

use failure::format_err;
use serde::{Deserialize, Serialize};

use crate::Result;

#[derive(Serialize, Deserialize)]
#[serde(tag = "cmd", content = "params")]
enum Command {
    Set(String, String),
    Rm(String),
}

#[test]
fn test_serialize() {
    let set_cmd = Command::Set("key".to_string(), "value".to_string());
    let rm_cmd = Command::Rm("key".to_string());
    let json_data = serde_json::to_string(&set_cmd).expect("marshal failed");
    assert_eq!(json_data, r#"{"cmd":"Set","params":["key","value"]}"#);
    let json_data = serde_json::to_string(&rm_cmd).expect("marshal failed");
    assert_eq!(json_data, r#"{"cmd":"Rm","params":"key"}"#);
}

struct LogFile {
    head_log: File,
}

impl LogFile {
    fn new(dir_path: &str) -> Self {
        let f = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(format!("{}/head.log", dir_path))
            .expect("cannot open log file");

        Self { head_log: f }
    }

    fn insert(&mut self, buf: &[u8]) -> Result<usize> {
        let file_index = self.head_log.seek(std::io::SeekFrom::End(0))?;

        if file_index > 0 {
            self.head_log.write(b"\n")?;
        }
        self.head_log.write(buf)?;
        Ok(buf.len())
    }

    fn rewind_head(&mut self) -> Result<()> {
        self.head_log.rewind()?;
        Ok(())
    }

    fn read_until(&mut self, delimiter: char, buf: &mut [u8]) -> Result<usize> {
        const CHUNK_SIZE: usize = 8;
        let mut offset = 0;
        'outer: loop {
            let n: usize = self.head_log.read(&mut buf[offset..offset + CHUNK_SIZE])?;
            if n == 0 {
                break;
            }
            for index in offset..offset + n {
                if buf[index] == delimiter as u8 {
                    // Rewind to index delimiter + 1
                    self.head_log
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
    use super::{Command, LogFile};

    #[test]
    fn test_read_until() {
        let path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), "/tests/data");
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

    // #[test]
    // fn test_insert_commands() {
    //     let set_cmd = Command::Set("key".to_string(), "value".to_string());
    //     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    // }
}

impl Drop for LogFile {
    fn drop(&mut self) {
        self.head_log.flush().expect("flush WAL error");
    }
}

impl Default for LogFile {
    fn default() -> Self {
        Self::new(".")
    }
}

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
    log_file: LogFile,
}

impl KvStore {
    /// Creates a `KvStore`.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        let Some(path) = path.as_path().to_str() else {
            return Err(format_err!("cannot convert path"));
        };
        let log_file = LogFile::new(path);
        Ok(KvStore { log_file })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key, value);
        let serde_bytes = serde_json::to_vec(&cmd)?;
        self.log_file.insert(&serde_bytes)?;

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let mut buf = [0; 1000];
        self.log_file.rewind_head()?;
        let mut value = None;
        loop {
            let n = self.log_file.read_until('\n', &mut buf)?;
            if n == 0 {
                break;
            }

            let cmd: Command = serde_json::from_slice(&buf[0..n])?;
            match cmd {
                Command::Set(k, v) => {
                    if k == key {
                        value = Some(v)
                    }
                }

                Command::Rm(k) => {
                    if k == key {
                        value = None
                    }
                }
            }
        }

        Ok(value)
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let mut buf = [0; 1000];
        self.log_file.rewind_head()?;
        let mut value = None;
        loop {
            let n = self.log_file.read_until('\n', &mut buf)?;
            if n == 0 {
                break;
            }

            let cmd: Command = serde_json::from_slice(&buf[0..n])?;
            match cmd {
                Command::Set(k, v) => {
                    if k == key {
                        value = Some(v)
                    }
                }

                Command::Rm(k) => {
                    if k == key {
                        value = None
                    }
                }
            }
        }

        let Some(_) = value else {
            return Err(format_err!("Key not found"));
        };

        // Found key, insert to log
        let cmd = Command::Rm(key);
        let serde_data = serde_json::to_vec(&cmd)?;
        self.log_file.insert(&serde_data)?;

        Ok(())
    }
}
