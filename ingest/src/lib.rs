pub mod fst;
pub mod tokenizer;
use anyhow::Result;
use std::path::PathBuf;
use store::{FSBlobStore, Store};

pub struct Ingester<'a> {
    store: &'a FSBlobStore,
}

impl<'a> Ingester<'a> {
    fn index(&self, path: &str) -> Result<String> {
        let mut buf = Vec::new();
        let buf = self.store.get(path, &mut buf)?;
        let tokens = vec!["12"]; // tokenizer::tokenize(std::str::from_utf8_mut(buf).unwrap());
        println!("tokens:: {:#?}", tokens);
        let path = PathBuf::from(path).to_str().unwrap().to_string() + ".token";
        let path = path.as_str();
        let data: Vec<u8> = tokens
            .iter()
            .map(|s| s.as_bytes().to_owned())
            .map(|mut s| {
                &s.push(0x0a);
                s
            })
            .flatten()
            .collect();
        self.store.put(path, data)?;
        Ok(path.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn index_test() {
        let store = FSBlobStore {
            root: "./root".into(),
            blobs: vec![],
        };

        let path = "file.txt";

        // store.put(path, vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]).unwrap();

        let ingester = Ingester { store: &store };

        let path = ingester.index(path).unwrap();
        let path = path.as_str();

        let mut buf = vec![];
        store.get(path, &mut buf).unwrap();

        println!("tokens:: {:#?}", buf);
        // assert_eq!(Vec::from("Hello"), vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    }
}
