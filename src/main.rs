mod fs;

use std::env;
use std::path::Path;
use std::time::Duration;

const TTL: Duration = Duration::from_millis(500);

fn main() {
    let args = env::args().collect::<Vec<String>>();

    let (mp, rest) = match &args[1..] {
        [ref path, ref rest @ ..] => (Path::new(path), rest),
        _ => {
            println!("Usage: {} <MountPoint> <args>", args[0]);
            return;
        }
    };

    let merged_fs = fs::merged::MergedFS::new(rest.iter().map(|x| Path::new(x)));
    fuser::mount2(merged_fs, mp, &[]).unwrap();
}
