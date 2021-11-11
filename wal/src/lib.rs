//! DONE WASTE HARDWARE

//! WAL is a apend only store to keep current going writes
//! we write the files here temporarily, which will be moved to
//! columnar store once we are done

#![allow(dead_code)]

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fs2::FileExt;
use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

/// WriteRecord is a unit entry in Wal
pub struct WriteRecord {
    crc: u32,
    len: u32,
    data: Vec<u8>,
}

impl WriteRecord {
    pub fn new(data: Vec<u8>) -> Result<Self> {
        let len = data.len().try_into()?;
        Ok(Self {
            crc: 0xdddd,
            len,
            data,
        })
    }
}

pub trait Store {
    fn open_file_for_read(&self, path: &str) -> Result<File>;
    fn open_file_for_append(&self, path: &str) -> Result<File>;
    fn root(&self) -> Option<&PathBuf>;
}

enum StoreType {
    FSBlobStore(FSBlobStore),
    MemStore(),
}

type FSBlob = File;
pub struct FSBlobStore {
    pub root: Option<PathBuf>,
    pub blobs: Vec<FSBlob>,
}

impl Store for FSBlobStore {
    fn open_file_for_read(&self, path: &str) -> Result<File> {
        Ok(std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .open(path)?)
    }

    fn open_file_for_append(&self, path: &str) -> Result<File> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(self.root.as_ref().unwrap().join(path))?;
        let thread = std::thread::current();
        let tname = thread.name().unwrap();
        // println!("{:?} :: opening file {}", tname, path);
        // println!("{:?} :: waiting for lock for append", tname);
        // file.lock_exclusive()?;
        // println!("{:?} :: lock aquired for append", tname);
        Ok(file)
    }

    fn root(&self) -> Option<&PathBuf> {
        self.root.as_ref()
    }
}

// impl Iterator for FSBlobStore {
//     type Item = FSBlob;
//     fn next(&mut self) -> Option<Self::Item> {
//         let current_file = self
//             .root
//             .read_dir()
//             .unwrap()
//             .filter(|f| {
//                 let path = f.unwrap().path();
//                 path.is_file() && path.extension().unwrap() == ""
//             })
//             .next();

//     }
// }

pub struct Wal<T: Store> {
    pub store: T,
    pub active_file: Option<File>,
}

impl<T: Store> Wal<T> {
    pub fn append(&mut self, payload: &WriteRecord) -> Result<u32> {
        let mut f = match self.active_file.take() {
            Some(f) => f,
            None => self.store.open_file_for_append("file.db")?,
        };
        //std::thread::sleep_ms(500);

        // std::thread::sleep_ms(10000);
        f.write_u32::<LittleEndian>(payload.len)
            // .and_then(|_| f.write_u16::<LittleEndian>(payload.crc))
            .and_then(|_| f.write_all(&payload.data))?;
        drop(payload);
        std::fs::remove_file(self.store.root().unwrap().join("file.db"))?;
        // self.active_file = Some(f);
        Ok(0)
    }

    pub fn fsync(&mut self) -> Result<()> {
        let f = self.active_file.take();

        if let Some(f) = f {
            f.sync_all()?;
        }
        Ok(())
    }

    pub fn new(t: T) -> Self {
        Wal {
            active_file: None,
            store: t,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn wal_write_test() {
    //     let mut wal = Wal::new(FSBlobStore { blobs: vec![] });
    //     let record = WriteRecord::new(Vec::from("Hello world!")).unwrap();
    //     for i in 0..100 {
    //         wal.append(&record).unwrap();
    //     }

    //     wal.fsync().unwrap();
    // }

    #[test]
    fn wal_write_concurrent_test() {
        std::thread::sleep_ms(20000);
        println!(
            "cpu count:: logical: {}, phys: {}",
            num_cpus::get(),
            num_cpus::get_physical()
        );
        let hndl = std::thread::Builder::new()
            .name("THREAD-1".into())
            .spawn(|| {
                println!("thread1 started");
                let mut wal = Wal::new(FSBlobStore {
                    root: Some("../db".into()),
                    blobs: vec![],
                });
                let record =
                    WriteRecord::new(vec![1u8; (52428800 * 2).try_into().unwrap()]).unwrap();
                for _ in 0..5000000 {
                    // println!("writing in thread1");
                    wal.append(&record).unwrap();
                }
                wal.fsync().unwrap();
            })
            .unwrap();

        /*
        let hndl2 = std::thread::Builder::new()
            .name("THREAD-2".into())
            .spawn(|| {
                println!("thread2 started");
                let mut wal2 = Wal::new(FSBlobStore {
                    root: "../db".into(),
                    blobs: vec![],
                });
                let record = WriteRecord::new(Vec::from("Secon Writer")).unwrap();
                for _ in 0..100 {
                    // println!("writing in thread2");
                    wal2.append(&record).unwrap();
                }
                wal2.fsync().unwrap();
            })
            .unwrap();
        */
        dbg!("waiting for both thread!");
        hndl.join().unwrap();
        //hndl2.join().unwrap();
    }
}
