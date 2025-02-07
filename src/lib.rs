mod specs {
    include!(concat!(env!("OUT_DIR"), "/epochfs.rs"));
}

mod fs;
pub use fs::Fs;
