use std::{
    fs,
    io::{self, Read},
    path::Path,
};
use cloyster::pagecache::Snapshot;

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = |dir_entry: io::Result<fs::DirEntry>| {
        if let Ok(de) = dir_entry {
            let path_buf = de.path();
            let path = path_buf.as_path();
            let path_str = &*path.to_string_lossy();
            if path_str.starts_with("./cloyster.db/snap.") && !path_str.ends_with(".in___motion") {
                Some(path.to_path_buf())
            } else {
                None
            }
        } else {
            None
        }
    };

    let snap_dir = Path::new("./cloyster.db");

    if !snap_dir.exists() {
        fs::create_dir_all(snap_dir)?;
    }

    let snaps: Vec<_> = snap_dir.read_dir()?.filter_map(filter).collect();

    for snap in snaps {
        let mut file = std::fs::File::open(snap).unwrap();
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        let snapshot: Snapshot = bincode::deserialize(&buf).unwrap();
        println!("snapshot: {:#?}", snapshot);
    }

    Ok(())
}
