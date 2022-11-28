pub mod file_handle;
pub mod inode;
pub mod merged;
pub mod source;
pub mod stats;

cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {
        #[path = "fs/attrs_unix.rs"]
        pub mod attrs;
    } else if #[cfg(target_os = "macos")] {
        #[path = "fs/attrs_unix.rs"]
        pub mod attrs;
    } else if #[cfg(target_os = "windows")] {

    }
}
