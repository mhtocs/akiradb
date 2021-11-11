use anyhow::Result;
use fs2::FileExt;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

mod builder;
mod schema;
mod table;

pub trait Store {
    fn root(&self) -> &PathBuf;
    fn get<'a>(&self, key: &str, buf: &'a mut Vec<u8>) -> Result<&'a mut Vec<u8>>;
    fn put(&self, key: &str, data: Vec<u8>) -> Result<()>;
    fn delete(&self, key: &str) -> Result<()>;
    fn list(&self) -> Result<Vec<FSBlob>>;
    fn exist(&self, key: &str) -> bool;
    fn clean(&self, key: &str) -> Result<()>;
}

enum StoreType {
    FSBlobStore(FSBlobStore),
    MemStore(),
}

type FSBlob = File;
pub struct FSBlobStore {
    pub root: PathBuf,
    pub blobs: Vec<FSBlob>,
}

impl Store for FSBlobStore {
    fn root(&self) -> &PathBuf {
        &self.root
    }

    fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let path = self.root.join(key);
        let prefix = path.parent().unwrap();
        std::fs::create_dir_all(prefix)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)?;
        file.lock_exclusive()?;
        file.write_all(&data[..]).and_then(|_| file.sync_all())?;

        Ok(())
    }

    fn get<'a>(&self, key: &str, buf: &'a mut Vec<u8>) -> Result<&'a mut Vec<u8>> {
        let path = self.root.join(key);
        let mut file = std::fs::OpenOptions::new().read(true).open(path)?;
        file.lock_exclusive()?;
        file.read_to_end(buf)?;
        Ok(buf)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let mut path = self.root.join(key);
        std::fs::remove_file(&path)?;
        while path.pop()
            && &path != &self.root
            && path.is_dir()
            && path.read_dir()?.next().is_none()
        {
            std::fs::remove_dir(&path)?;
        }
        Ok(())
    }

    fn list(&self) -> Result<Vec<FSBlob>> {
        Ok(Vec::new())
    }

    fn exist(&self, key: &str) -> bool {
        let path = self.root.join(key);
        std::fs::metadata(path).is_ok()
    }

    fn clean(&self, key: &str) -> Result<()> {
        let path = self.root.join(key);
        if let Err(e) = std::fs::remove_dir_all(path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(e.into());
            }
        };
        Ok(())
    }
}

struct Path {
    root: PathBuf,
}

impl Path {
    fn push(&mut self, next: &str) -> &mut Path {
        self.root.push(next);
        self
    }

    fn build(&self) -> &PathBuf {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    // #[ignore]
    fn store_write_and_read_test() {
        let store = FSBlobStore {
            root: "./root".into(),
            blobs: vec![],
        };

        let path = "file.txt";

        store.put(path, vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]).unwrap();

        let mut buf = vec![];
        store
            .get(path, &mut buf)
            .and_then(|_| store.delete(path))
            .unwrap();

        assert_eq!(Vec::from("Hello"), vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    }

    #[test]
    // #[ignore]
    fn write_and_read_from_dir_test() {
        let store = FSBlobStore {
            root: "./root".into(),
            blobs: vec![],
        };

        let mut path = Path {
            root: PathBuf::from("wrd_root"),
        };

        let path = path
            .push("new")
            .push("temp")
            .push("temp2")
            .push("file.txt")
            .build()
            .to_str()
            .unwrap();

        store.put(path, vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]).unwrap();

        let mut buf = vec![];
        dbg!(path);
        store
            .get(path, &mut buf)
            .and_then(|_| store.delete(path))
            .unwrap();
        assert_eq!(Vec::from("Hello"), vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    }

    #[test]
    fn delete_test() {
        let store = FSBlobStore {
            root: "./root".into(),
            blobs: vec![],
        };

        let mut path = Path {
            root: "del_root".into(),
        };

        let path = path.push("new").push("temp12").build().to_str().unwrap();
        store.put(path, vec![0]).unwrap();
        dbg!(path);
        store.delete(path).unwrap();
        assert_eq!(store.exist(path), false);
    }

    #[test]
    // #[ignore]
    fn clean_root_test() {
        let store = FSBlobStore {
            root: "./root".into(),
            blobs: vec![],
        };

        store.clean("clean_root2").unwrap();
        assert_eq!(store.exist(store.root.to_str().unwrap()), false);
    }
}
