//! This doesnt belong here
//! It should be in a crate which is responsible for everything related to
//! terms & posting list.
//! But i am too lazy to create a crate for this, So lets keep this here
//! for now

use anyhow::Result;
use std::io::Write;

// based on tantivy's fst & burntsushi's transducer blog

pub struct TermDictBuilder<W> {
    map_builder: fst::MapBuilder<W>,
    term_id: u64,
}

impl<W> TermDictBuilder<W>
where
    W: Write,
{
    pub fn new(w: W) -> Result<Self> {
        Ok(Self {
            map_builder: fst::MapBuilder::new(w)?,
            term_id: 0,
        })
    }

    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.map_builder.insert(key, self.term_id)?;
        self.term_id += 1;
        Ok(())
    }

    pub fn build(mut self) -> Result<W> {
        println!("BYTES WRITTEN:: {}", self.map_builder.bytes_written());
        Ok(self.map_builder.into_inner()?)
    }

    pub fn merge(&mut self) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    // use pretty_assertions::assert_eq;
    use crate::tokenizer::{NGramTokenizer, Token, Tokenizer};
    use fst;
    use fst::Streamer;

    use store::{FSBlobStore, Store};
    use TermDictBuilder;

    #[test]
    fn fst_setup_test() {
        let tokenizer = NGramTokenizer::new(3).unwrap();
        let sample_str = r#"Loaded Servicing Stack v6.1.7601.23505 with Core: C:\Windows\winsxs\amd64_microsoft-windows-servicingstack_31bf3856ad364e35_6.1.7601.23505_none_681aa442f6fed7f0\cbscore.dll"#;
        println!("BYTES LEN {}", sample_str.bytes().len());
        let mut tokens = tokenizer.tokenize(sample_str).collect::<Vec<Token>>();
        tokens.sort();
        let mut term_builder = TermDictBuilder::new(Vec::<u8>::new()).unwrap();
        for token in tokens {
            match term_builder.insert(token.as_ref()) {
                Ok(s) => println!("SUCESS:: {:#?}", token),
                Err(e) => println!("FAILED:: {:#?}", e),
            }
        }

        let term_vec = term_builder.build();

        let store = FSBlobStore {
            root: "./".into(),
            blobs: vec![],
        };

        let terms = term_vec.unwrap();
        store.put("text.term", terms.clone()).unwrap();

        let map = fst::Map::new(terms).unwrap();

        // let mut stream = map.stream();
        // while let Some(pair) = stream.next() {
        //     println!(
        //         "KEY:: {:?}, VALUE:: {:?}",
        //         std::str::from_utf8(pair.0),
        //         pair.1
        //     )
        // }
    }
    // assert_eq!(
    //     tokens,
    //     vec!["123", "234", "345", "456", "567", "678", "789"]
    // );
}
