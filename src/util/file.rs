use super::config::Opt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

pub fn reader(file: &PathBuf, opt: &Opt) -> Box<dyn BufRead> {
    if opt.verbose {
        println!("Opening log file:: {}", file.display())
    }

    // let file = Path::new(filename);
    let file = match File::open(file) {
        Err(e) => panic!("Failed to open file: {} {}", file.display(), e.to_string()),
        Ok(file) => file,
    };

    Box::new(BufReader::with_capacity(8 * 8 * 1024, file))
}
