use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use failure::format_err;
use serde_derive::{Deserialize, Serialize};

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
    dir_path: String,
    wal_path: String,
}

impl LogFile {
    fn new(dir_path: &str) -> Self {
        let wal_path = format!("{}/head.log", dir_path);
        let f = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&wal_path)
            .expect("cannot open log file");

        Self {
            head_log: f,
            wal_path,
            dir_path: dir_path.to_string(),
        }
    }

    fn append(&mut self, buf: &[u8]) -> Result<usize> {
        let file_index = self.head_log.seek(std::io::SeekFrom::End(0))?;
        let mut n = 0;
        if file_index > 0 {
            n += self.head_log.write(b"\n")?;
        }
        n += self.head_log.write(buf)?;
        Ok(n)
    }

    fn read_until(&mut self, delimiter: char, buf: &mut [u8]) -> Result<usize> {
        const CHUNK_SIZE: usize = 8;
        let mut offset = 0;
        'outer: loop {
            let n: usize = self.head_log.read(&mut buf[offset..offset + CHUNK_SIZE])?;
            if n == 0 {
                break;
            }
            for (index, &current_char) in buf.iter().enumerate().skip(offset).take(n) {
                if current_char == delimiter as u8 {
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

    fn current_file_offset(&mut self) -> Result<u64> {
        let offset = self.head_log.stream_position()?;
        Ok(offset)
    }

    fn read_until_from_offset(
        &mut self,
        delimiter: char,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize> {
        self.head_log.seek(SeekFrom::Start(offset))?;
        let n = self.read_until(delimiter, buf)?;
        Ok(n)
    }

    // drop this log file, return new log file
    fn compact(
        &mut self,
        retained_offsets: &[u64],
        mut on_write_fn: impl FnMut(&[u8], u64),
    ) -> Result<()> {
        // Read file from start
        self.head_log.flush()?;
        self.head_log.rewind()?;

        let temp_path = format!("{}/head.log.compact", self.dir_path);

        let mut new_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&temp_path)
            .expect("cannot open log file");

        // Write to new log file
        for &offset in retained_offsets {
            let mut buf = [0; 1000];
            let mut n = self.read_until_from_offset('\n', offset, &mut buf)?;
            let mut cur_offset = new_file.seek(SeekFrom::Current(0))?;
            if cur_offset > 0 {
                new_file.write(b"\n")?;
                cur_offset += 1;
            }
            on_write_fn(&buf[0..n], cur_offset);
            if buf[n - 1] == b'\n' {
                n -= 1;
            }
            new_file.write(&buf[0..n])?;
        }

        // replace original WAL
        fs::rename(temp_path, &self.wal_path)?;
        self.head_log = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&self.wal_path)
            .expect("cannot open log file");

        Ok(())
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
        assert_eq!(n, 39);
        assert_eq!(
            "{\"cmd\":\"Set\",\"params\":[\"key\",\"value\"]}\n",
            str::from_utf8(&buf[0..n]).expect("convert string failed")
        );

        let n = log_file
            .read_until('\n', &mut buf)
            .expect("read until failed");
        assert_eq!(n, 41);
        assert_eq!(
            "{\"cmd\":\"Set\",\"params\":[\"key2\",\"value2\"]}\n",
            str::from_utf8(&buf[0..n]).expect("convert string failed")
        );
    }
}

impl Drop for LogFile {
    fn drop(&mut self) {
        self.head_log.flush().expect("flush WAL error");
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
/// let mut store = KvStore::open(".").unwrap();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned()).unwrap();
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    log_file: LogFile,
    log_pointer_map: HashMap<String, u64>,
}

impl KvStore {
    /// Creates a `KvStore`.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        let Some(path) = path.as_path().to_str() else {
            return Err(format_err!("cannot convert path"));
        };
        let log_file = LogFile::new(path);
        let log_pointer_map = HashMap::new();
        let mut obj = KvStore {
            log_file,
            log_pointer_map,
        };

        let _ = obj.replay_log_file()?;

        Ok(obj)
    }

    fn replay_log_file(&mut self) -> Result<()> {
        let mut buf = [0; 1000];
        loop {
            let n = self.log_file.read_until('\n', &mut buf)?;
            if n == 0 {
                break;
            }

            let cmd: Command = serde_json::from_slice(&buf[0..n])?;
            match cmd {
                Command::Set(k, _) => {
                    let log_offset = self.log_file.current_file_offset()? - n as u64;
                    self.log_pointer_map
                        .entry(k)
                        .and_modify(|e| *e = log_offset)
                        .or_insert(log_offset);
                }

                Command::Rm(k) => {
                    self.log_pointer_map
                        .remove(&k)
                        .expect("WAL log invalid, remove key non existed");
                }
            }
        }
        Ok(())
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key.clone(), value);

        let serde_bytes = serde_json::to_vec(&cmd)?;
        self.log_file.append(&serde_bytes)?;
        let cur_offset = self.log_file.current_file_offset()?;
        let log_offset = cur_offset - serde_bytes.len() as u64;

        // Update in-mem map log pointer
        self.log_pointer_map
            .entry(key)
            .and_modify(|e| *e = log_offset)
            .or_insert(log_offset);

        // Do log compact
        let _ = self.log_compact()?;

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let Some(&offset) = self.log_pointer_map.get(&key) else {
            return Ok(None);
        };
        let mut buf = [0; 1000];
        let n = self
            .log_file
            .read_until_from_offset('\n', offset, &mut buf)?;
        let cmd: Command = serde_json::from_slice(&buf[0..n])?;
        match cmd {
            Command::Set(_, value) => Ok(Some(value)),
            _ => panic!("invalid write a head log offset"),
        }
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let offset = self.log_pointer_map.remove(&key);
        if offset.is_none() {
            return Err(format_err!("Key not found"));
        }

        // Found key, insert to log
        let cmd = Command::Rm(key);
        let serde_data = serde_json::to_vec(&cmd)?;
        self.log_file.append(&serde_data)?;

        // Do log compact
        let _ = self.log_compact()?;

        Ok(())
    }

    fn log_compact(&mut self) -> Result<bool> {
        const COMPACT_THRESHOLD: u64 = 16_000_000; // 16 MB

        let cur_offset = self.log_file.current_file_offset()?;
        if cur_offset < COMPACT_THRESHOLD {
            return Ok(false);
        }

        let mut new_log_pointer_map = HashMap::new();
        let mut retained_offsets: Vec<u64> = self.log_pointer_map.values().map(|&x| x).collect();
        retained_offsets.sort_unstable();

        self.log_file
            .compact(&retained_offsets, |buf, cur_offset| {
                let cmd: Command = serde_json::from_slice(buf).expect("no error");
                let Command::Set(key, _) = cmd else {
                    panic!("should not happen");
                };

                new_log_pointer_map.insert(key, cur_offset);
            })?;

        self.log_pointer_map = new_log_pointer_map;

        Ok(true)
    }
}
