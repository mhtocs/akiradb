//! READS DONT BLOCK WRITE
//! WRITES DONT BLOCK READ

use akiradb::util::config;
use akiradb::util::file;
use ingest::fst;
use ingest::tokenizer::{NGramTokenizer, Token, Tokenizer};
use serde_json::Value;
use std::collections::BTreeSet;
use store::{FSBlobStore, Store};

fn main() -> std::io::Result<()> {
    let cfg = config::Opt::from_args();
    let tokenizer = NGramTokenizer::new(3).unwrap();
    let block_size = 2usize.pow(32);

    cfg.files.iter().for_each(|filename| {
        if cfg.verbose {
            println!("parsing file {}", filename.display())
        }

        // Open the file in read-only mode (ignoring errors).
        let mut reader = file::reader(&filename, &cfg);

        let mut tokens = BTreeSet::<Token>::new();
        println!("Block size is :: {}", block_size);

        let mut buf = String::with_capacity(8 * 1024);
        let mut i = 0;
        while reader.read_line(&mut buf).unwrap_or(0) > 0 {
            let json: serde_json::Result<Value> = serde_json::from_str(&buf);
            match json {
                Ok(json) => {
                    for (k, _) in json.as_object().unwrap() {
                        tokens.extend(
                            tokenizer
                                .tokenize(json.get(k).unwrap().as_str().unwrap())
                                .collect::<Vec<Token>>(),
                        );
                    }
                }
                Err(err) => println!("PARSING ERROR:: {}", err),
            }
            i = i + 1;
            buf.clear();
        }

        // tokens.sort_unstable();
        println!("TOTAL READ:: {}", i);
        println!("TOTAL ELEMENTS:: {}", tokens.len());
        let mut unique = 0;
        let mut term_builder = fst::TermDictBuilder::new(Vec::<u8>::new()).unwrap();

        for token in tokens {
            //           println!("{:?}", token)
            match term_builder.insert(token.as_ref()) {
                Ok(s) => (unique += 1), //println!("SUCESS:: {:#?}", token),
                Err(e) => (),           //println!("FAILED:: {:#?}", e),
            }
        }

        println!("UNIQUE:: {}", unique);

        let term_vec = term_builder.build();

        let store = FSBlobStore {
            root: "./".into(),
            blobs: vec![],
        };

        let terms = term_vec.unwrap();
        store.put("final.term", terms.clone()).unwrap();
    });

    Ok(())
}
