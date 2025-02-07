use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["specs/epochfs.proto"], &["specs/"])?;
    Ok(())
}
