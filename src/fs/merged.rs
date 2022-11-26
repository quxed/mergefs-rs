use crate::fs::inode::INodeTable;
use crate::fs::source::Source;
use crate::{fs, TTL};

use crate::fs::attrs::fuse_attr;
use crate::fs::source;
use fuser::FileType::{Directory, RegularFile};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyStatfs, Request,
};
use libc::{c_int, ENOENT};
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{io, mem};

const SOURCE_INODE_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;
const IS_DIR_MASK: u64 = 0xA000_0000_0000_0000;
const MERGED_SOURCE_MASK: u64 = !(SOURCE_INODE_MASK | IS_DIR_MASK);

pub struct MergedFS {
    sources: Vec<Source>,
    directories: INodeTable, // directories are mapped across all disks.
}

impl MergedFS {
    pub fn new<'a, I>(paths: I) -> Self
    where
        I: Iterator<Item = &'a Path>,
    {
        debug!("MergedFS::new(...)");
        MergedFS {
            sources: paths.map(|root| Source::new(root.to_path_buf())).collect(),
            directories: INodeTable::new(),
        }
    }

    fn get_dir_info<P: AsRef<Path>>(&self, ino: u64, p: P) -> FileAttr {
        debug!(
            "MergedFS::get_dir_info(ino: {:#x}, p:{})",
            ino,
            p.as_ref().display()
        );

        let mut attr: FileAttr = unsafe { mem::zeroed() };
        attr.ino = ino;
        attr.kind = FileType::Directory;
        attr.perm = 0o777;
        for s in &self.sources {
            if let Ok(m) = s.metadata(p.as_ref()) {
                attr.atime = m.accessed().unwrap_or(SystemTime::UNIX_EPOCH);
                attr.ctime = m.created().unwrap_or(SystemTime::UNIX_EPOCH);
                attr.size = m.len();
            }
        }
        return attr;
    }

    pub fn read_dir<P: AsRef<Path>>(&self, path_ref: P) -> io::Result<ReadDir> {
        debug!("MergedFS::read_dir({})", path_ref.as_ref().display());
        let mut tmp = Vec::<(u16, source::ReadDir)>::with_capacity(self.sources.len());
        for i in 0..self.sources.len() {
            let path = path_ref.as_ref().clone();
            let read_dir = self.sources.get(i).unwrap().read_dir(path).unwrap();
            tmp.push((i as u16, read_dir))
        }

        return Ok(ReadDir {
            current: tmp.pop(),
            rd: tmp,
            src: self,
        });
    }

    fn get_attr_dir(&self, ino: u64) -> Option<fuser::FileAttr> {
        debug!("MergedFS::get_attr_dir({:#x})", ino);
        let ino = ino & SOURCE_INODE_MASK;
        let dir = self.directories.lookup_merged_path_from_inode(ino)?;
        for v in &self.sources {
            if let Ok(x) = v.fuse_attr_path(&dir) {
                return Some(x);
            }
        }
        return None;
    }

    fn fuse_attr(&self, ino: u64) -> Option<FileAttr> {
        if ino == 1 || (IS_DIR_MASK & ino) > 0 {
            // directory
            return self.get_attr_dir(ino);
        }
        // so it's a file (we assume!)
        let source = (ino & MERGED_SOURCE_MASK) >> 48;
        let src = self.sources.get(source as usize)?;
        let src_inode = ino & SOURCE_INODE_MASK;

        let attr = src.fuse_attr(src_inode);
        if let Err(e) = attr {
            if e.kind() != ErrorKind::NotFound {
                error!(
                    "error getting attributes for inode {:#x} -> ({}, {}): {}",
                    ino, source, src_inode, e
                );
                return None;
            }
            return None;
        }
        let mut attr = attr.unwrap();
        attr.ino = ino; // to make sure we're including the source bits in the inode.
        return Some(attr);
    }

    fn fuse_attr_from_path<P: AsRef<Path>>(&self, path: P) -> Option<FileAttr> {
        if path.as_ref() == OsString::from("/") {
            return self.fuse_attr(1);
        }

        if let Some(ino) = self
            .directories
            .lookup_inode_from_merged_path(&OsString::from(path.as_ref()))
        {
            return self.fuse_attr(ino);
        }
        // not a directory, and we don't have any inode, so this isn't going to be a fun lookup.
        for (idx, src) in (0..).zip(&self.sources) {
            if let Ok(mut attr) = src.fuse_attr_path(&path) {
                attr.ino = (attr.ino & SOURCE_INODE_MASK) | (idx << 48);
                return Some(attr);
            }
        }
        // no sources contained this file.
        None
    }
}

impl Filesystem for MergedFS {
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        todo!()
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("MergedFS::lookup(parent:{:#x}, name:{:?})", parent, name);

        // look up the parent directory
        let parent_path = self.directories.lookup_merged_path_from_inode(parent);
        if let None = parent_path {
            reply.error(ENOENT);
            return;
        }

        let mut parent_path = PathBuf::from(parent_path.unwrap());
        // special case for looking at the parent...
        let target_path = if name == "._." {
            let grandparent = parent_path.parent();
            if let None = grandparent {
                reply.error(ENOENT);
                return;
            }
            grandparent.unwrap()
        } else {
            parent_path.push(name);
            parent_path.as_path()
        };

        if let Some(attr) = self.fuse_attr_from_path(target_path) {
            reply.entry(&TTL, &attr, 1);
        } else {
            reply.error(ENOENT);
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyStatfs) {
        debug!("MergedFS::statfs({:#x})", ino);

        let mut totals: fs::stats::FSStats = unsafe { mem::zeroed() };
        let mut seen = BTreeSet::<u64>::new();

        for (idx, v) in (0..).zip(&self.sources) {
            let tmp = v.statfs();
            if let Err(e) = tmp {
                error!("error running statfs over {}: {}", idx, e);
                continue;
            }
            let tmp = tmp.unwrap();
            if seen.contains(&tmp.fsid) {
                debug!("skipping fs {} as we've already seen it", tmp.fsid);
                continue;
            }
            seen.insert(tmp.fsid);
            totals.namelen = if totals.namelen == 0 || totals.namelen > tmp.namelen {
                tmp.namelen
            } else {
                totals.namelen
            };

            totals.bsize = tmp.bsize;
            totals.bfree += tmp.bfree;
            totals.bavail += tmp.bavail;
            totals.blocks += tmp.blocks;
            totals.files += tmp.files;
            totals.ffree += tmp.ffree;
        }

        debug!("replying with fs {:?}", totals);
        reply.statfs(
            totals.blocks,
            totals.bfree,
            totals.bavail,
            totals.files,
            totals.ffree,
            totals.bsize as u32,
            totals.namelen as u32,
            totals.bsize as u32,
        );
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        debug!("MergedFS::access(ino:{:#x}, mask:{:#x})", ino, mask);
        reply.ok();
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        debug!("MergedFS::getattr({:#x})", ino);

        if ino == 1 || (IS_DIR_MASK & ino) > 0 {
            // directory
            let attr = self.get_attr_dir(ino);
            if let None = attr {
                debug!("directory inode {:#x} not found", ino);
                reply.error(libc::ENOENT);
                return;
            }

            let attr = attr.unwrap();
            debug!("returning attributes {:?}", attr);
            reply.attr(&TTL, &attr);
            return;
        }

        let source = (ino & MERGED_SOURCE_MASK) >> 48;

        let src = self.sources.get(source as usize);
        if let None = src {
            debug!("file inode {:#x} not found", ino);
            reply.error(libc::ENOENT);
            return;
        }
        let src = src.unwrap();
        let src_inode = ino & SOURCE_INODE_MASK;
        let attr = src.fuse_attr(src_inode);
        if let Err(e) = attr {
            if e.kind() == ErrorKind::NotFound {
                reply.error(libc::ENOENT);
            } else {
                error!(
                    "error getting attributes for inode {:#x} -> ({}, {}): {}",
                    ino, source, src_inode, e
                );
                reply.error(libc::EFAULT);
            }
            return;
        }
        let attr = attr.unwrap();
        reply.attr(&TTL, &attr);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("MergedFS::readdir(ino:{:#x}, offset: {})", ino, offset);
        let read_dir = if ino == 1 {
            debug!("MergedFS::readdir -- readdir of root");
            // root
            Ok(self.read_dir("/"))
        } else if (ino & IS_DIR_MASK) == 0 {
            error!(
                "MergedFS::readdir -- readdir on {:#x} is not a directory",
                ino
            );
            // not a directory
            Err(libc::EINVAL)
        } else {
            debug!("MergedFS::readdir -- readdir of non-root directory");
            // look up the directory path from its inode
            match self
                .directories
                .lookup_merged_path_from_inode(ino & SOURCE_INODE_MASK)
            {
                None => Err(ENOENT),
                Some(x) => Ok(self.read_dir(PathBuf::from(x))),
            }
        };

        if let Err(i) = read_dir {
            reply.error(i);
            return;
        }

        let read_dir = read_dir.unwrap().unwrap(); // directly unwrapping result as there's no way for it to return an error.
        let mut i = offset + 1;

        for dir in read_dir.into_iter().skip(offset as usize) {
            if let Err(x) = dir {
                error!("error reading directory {}", x);
                continue;
            }
            let dir = dir.unwrap();

            let file_type = if dir.d.is_dir().unwrap_or(false) {
                Directory
            } else {
                RegularFile
            };

            debug!(
                "MergedFS::readdir -- adding {:?} inode {:#x} offset {} filename {}",
                file_type,
                dir.ino(),
                i,
                PathBuf::from(dir.d.file_name()).display()
            );

            if reply.add(dir.inode, i, file_type, dir.d.file_name()) {
                debug!("buffer is full skipping at offset {}", i);
                break;
            }
            i = i + 1
        }
        reply.ok();
    }
}

pub struct ReadDir<'a> {
    current: Option<(u16, source::ReadDir<'a>)>,
    rd: Vec<(u16, source::ReadDir<'a>)>,
    src: &'a MergedFS,
}

impl<'a> Iterator for ReadDir<'a> {
    type Item = Result<DirEntry<'a>, std::io::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        debug!("MergedFS::ReadDir::next");
        while let Some((idx, read_dir)) = &mut self.current {
            let val = read_dir.next();
            debug!(
                "MergedFS::ReadDir::next -- next val on source {} is {:?}",
                idx, val
            );
            match val {
                Some(Ok(d)) => {
                    debug!(
                        "MergedFS::ReadDir::next -- looking at {}",
                        d.path_within_source().display()
                    );
                    if d.is_dir().unwrap() {
                        // because we want dirs to appear as though they exist on all disks,
                        // we have a separate inode table for just directories within the Merged FS.
                        // the inodes assigned to directories within the merged FS are unrelated to
                        // the inodes assigned by their sources.
                        //
                        // however, for files, we use the source inodes.
                        let os_str = d.path_within_source().into_os_string();
                        let dir_inode = self
                            .src
                            .directories
                            .lookup_insert_inode_from_merged_path(&os_str);
                        let inode = (dir_inode & SOURCE_INODE_MASK) | IS_DIR_MASK;
                        debug!(
                            "MergedFS::ReadDir::next -- come across dir {} inode {:#x}",
                            PathBuf::from(&os_str).display(),
                            inode
                        );

                        return Some(Ok(DirEntry { inode, d }));
                    }

                    let inode = (MERGED_SOURCE_MASK & ((*idx as u64) << 48)) // store the source index in the first two bytes of the inode number.
                    | (SOURCE_INODE_MASK & d.ino()); // and the source inode in the rest...
                    debug!(
                        "MergedFS::ReadDir::next -- come across file {} in source {:#x} inode {:#x}",
                        PathBuf::from(d.path_within_source().into_os_string()).display(),
                        idx,
                        inode,
                    );

                    return Some(Ok(DirEntry { inode, d }));
                }
                v => {
                    debug!("MergedFS::ReadDir::next -- Skipping due to {:?}", v);
                    // most likely the directory doesn't exist on this filesystem.
                    self.current = self.rd.pop();
                }
            }
        }
        None
    }
}

pub struct DirEntry<'a> {
    inode: u64,
    d: source::DirEntry<'a>,
}

impl<'a> DirEntry<'a> {
    fn from_dir_entry(d: source::DirEntry<'a>, inode: u64) -> Self {
        DirEntry { inode, d }
    }

    fn ino(&self) -> u64 {
        self.inode
    }
}
