use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "akiradb", about, author)]
pub struct Opt {
    ///Activate verbose mode
    #[structopt(short, long)]
    pub verbose: bool,

    /// Files to process
    #[structopt(name = "files", parse(from_os_str), required(true))]
    pub files: Vec<PathBuf>,

    /// Output file
    #[structopt(short, long, parse(from_os_str))]
    pub output: Option<PathBuf>,
}

impl Opt {
    pub fn from_args() -> Self {
        StructOpt::from_args()
    }
}
