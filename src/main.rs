mod fs;
#[macro_use]
extern crate log;

use fuser::MountOption;
use std::env;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const TTL: Duration = Duration::from_millis(50000);

fn main() {
    env_logger::init();
    info!("MergeFS Started");

    let args = env::args().collect::<Vec<String>>();

    let (mp, rest) = match &args[1..] {
        [ref path, ref rest @ ..] => (Path::new(path), rest),
        _ => {
            println!("Usage: {} <MountPoint> <args>", args[0]);
            return;
        }
    };
    let fhm = Arc::new(fs::file_handle::FileHandleManager::new());
    let merged_fs = fs::merged::MergedFS::new(rest.iter().map(|x| Path::new(x)), fhm);
    fuser::mount2(
        merged_fs,
        mp,
        &[MountOption::AutoUnmount, MountOption::DefaultPermissions],
    )
    .unwrap();
}
