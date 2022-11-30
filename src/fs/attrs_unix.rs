use crate::fs::stats::FSStats;
use fuser::{FileAttr, FileType};
use std::ffi::CString;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::{fs, io, mem, path};

pub fn fsstat<P: AsRef<path::Path>>(p: P) -> io::Result<FSStats> {
    let p = CString::new(p.as_ref().to_str().unwrap()).unwrap();
    let mut statvfs: libc::statvfs = unsafe { mem::zeroed() };
    let result = unsafe { libc::statvfs(p.as_ptr(), &mut statvfs) };

    if result != 0 {
        // error
        return Err(match result {
            libc::ENOENT => io::Error::new(io::ErrorKind::NotFound, "not found"),
            x => io::Error::new(io::ErrorKind::Other, format!("error {:#x}", x)),
        });
    }

    let out = FSStats {
        fsid: statvfs.f_fsid as u64,
        blocks: statvfs.f_blocks as u64,
        bfree: statvfs.f_bfree as u64,
        bavail: statvfs.f_bavail as u64,
        files: statvfs.f_files as u64,
        ffree: statvfs.f_ffree as u64,
        bsize: statvfs.f_bsize as u64,
        namelen: statvfs.f_namemax as u64,
    };

    debug!("fsstat of fs at {} is {:?}", p.to_string_lossy(), out);

    Ok(out)
}

pub fn fuse_attr<P: AsRef<path::Path>>(ino: u64, p: P) -> io::Result<FileAttr> {
    let p = p.as_ref();
    let md = fs::metadata(p)?;

    let out = FileAttr {
        ino,
        size: md.len(),
        blocks: md.blocks(),
        atime: md.accessed().unwrap_or(std::time::UNIX_EPOCH),
        mtime: md.modified().unwrap_or(std::time::UNIX_EPOCH),
        ctime: md.modified().unwrap_or(std::time::UNIX_EPOCH),
        crtime: md.created().unwrap_or(std::time::UNIX_EPOCH),
        kind: if md.file_type().is_dir() {
            FileType::Directory
        } else if md.file_type().is_symlink() {
            FileType::Symlink
        } else {
            FileType::RegularFile
        },
        perm: md.permissions().mode() as u16,
        nlink: md.nlink() as u32,
        uid: md.uid(),
        gid: md.gid(),
        rdev: md.rdev() as u32,
        blksize: md.blksize() as u32,
        flags: 0,
    };

    debug!("attr of file at {} is {:?}", p.display(), out);
    Ok(out)
}
