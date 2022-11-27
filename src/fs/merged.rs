use crate::fs::inode::{INode, INodeTable};
use crate::fs::source::Source;
use crate::{fs, TTL};

use crate::fs::attrs::fuse_attr;
use crate::fs::source;
use fuser::FileType::{Directory, RegularFile};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyStatfs, Request,
};
use libc::{c_int, EINVAL, EIO, ENOENT};
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{io, mem};

const FILE_HANDLE_READ_BIT: u64 = 1 << 63;

pub struct MergedFS {
    sources: Vec<Source>,
    directories: INodeTable<u64>, // directories are mapped across all disks.
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

    fn get_attr_dir(&self, ino: INode) -> Option<fuser::FileAttr> {
        debug!("MergedFS::get_attr_dir({:#x})", ino);
        match ino {
            INode::Dir(ino) => {
                let dir = self.directories.lookup_merged_path_from_inode(ino)?;
                for v in &self.sources {
                    if let Ok(x) = v.fuse_attr_path(&dir) {
                        return Some(x);
                    }
                }
                return None;
            }
            _ => return None,
        }
    }

    fn fuse_attr(&self, ino: u64) -> Option<FileAttr> {
        let ino = INode::from(ino);
        match ino {
            d @ INode::Dir(x) => self.get_attr_dir(d),
            f @ INode::File(source_idx, src_inode) => {
                // so it's a file (we assume!)
                let src = self.sources.get(source_idx as usize)?;
                let attr = src.fuse_attr(src_inode);
                if let Err(e) = attr {
                    if e.kind() != ErrorKind::NotFound {
                        error!(
                            "error getting attributes for inode {:#x} -> ({}, {}): {}",
                            ino, source_idx, src_inode, e
                        );
                        return None;
                    }
                    return None;
                }
                let mut attr = attr.unwrap();
                attr.ino = f.into(); // to make sure we're including the source bits in the inode.
                return Some(attr);
            }
        }
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
                attr.ino = INode::File(idx, attr.ino).into();
                return Some(attr);
            }
        }
        // no sources contained this file.
        None
    }
}

fn inject_source_index(n: INode, s: u16) -> INode {
    match n {
        d @ INode::Dir(_) => d,
        INode::File(_, ino) => INode::File(s, ino),
    }
}
fn check_file_handle_read(file_handle: u64) -> bool {
    (file_handle & FILE_HANDLE_READ_BIT) != 0
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
        let mut buf = vec![0; size as usize];
        // if !check_file_handle_read(fh) {
        //     reply.error(libc::EACCES);
        //     return;
        // }
        let ino = INode::from(ino);
        match ino {
            INode::Dir(_) => {
                warn!("MergedFS::read attempted to read a directory. {:?}", ino);
                reply.error(EINVAL);
                return;
            }
            INode::File(src, src_ino) => {
                let source = self.sources.get(src as usize);
                if let None = source {
                    warn!("MergedFS::read attempted to fetch an inode {:?} from a non-existent source {}", ino, src);
                    reply.error(ENOENT);
                    return;
                }
                let source = source.unwrap();
                let res = source.read(src_ino, offset, size, &mut buf);
                if let Err(e) = res {
                    error!("MergedFS::read error reading form inode {:?}: {}", ino, e);
                    reply.error(EIO);
                    return;
                }
                let amount_read = res.unwrap();
                reply.data(&buf[0..amount_read]);
            }
        }
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
        let ino = INode::from(ino);
        let attr = match ino {
            d @ INode::Dir(ino) => {
                // directory
                let attr = self.get_attr_dir(d);
                if let None = attr {
                    debug!("MergedFS::getattr -- directory inode {:#x} not found", ino);
                    reply.error(libc::ENOENT);
                    return;
                }

                attr.unwrap()
            }
            INode::File(src_idx, ino) => {
                let src = self.sources.get(src_idx as usize);
                if let None = src {
                    debug!("MergedFS::getattr -- file inode {:#x} not found", ino);
                    reply.error(libc::ENOENT);
                    return;
                }
                let src = src.unwrap();
                let attr = src.fuse_attr(ino);
                if let Err(e) = attr {
                    if e.kind() == ErrorKind::NotFound {
                        reply.error(libc::ENOENT);
                    } else {
                        error!(
                            "error getting attributes for inode {:#x} -> ({}, {}): {}",
                            ino, src_idx, ino, e
                        );
                        reply.error(libc::EFAULT);
                    }
                    return;
                }
                attr.unwrap()
            }
        };

        debug!("returning attributes {:?}", attr);
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
        } else {
            let ino = INode::from(ino);
            match ino {
                INode::File(_, _) => {
                    error!(
                        "MergedFS::readdir -- readdir on {:?} is not a directory",
                        ino
                    );
                    // not a directory
                    Err(libc::EINVAL)
                }
                INode::Dir(dir_ino) => {
                    debug!("MergedFS::readdir -- readdir of non-root directory");
                    // look up the directory path from its inode
                    match self.directories.lookup_merged_path_from_inode(dir_ino) {
                        None => Err(ENOENT),
                        Some(x) => Ok(self.read_dir(PathBuf::from(x))),
                    }
                }
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

            if reply.add(dir.inode.into(), i, file_type, dir.d.file_name()) {
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
        while let Some((source_index, read_dir)) = &mut self.current {
            let val = read_dir.next();
            debug!(
                "MergedFS::ReadDir::next -- next val on source {} is {:?}",
                source_index, val
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
                        let inode = INode::Dir(dir_inode);
                        debug!(
                            "MergedFS::ReadDir::next -- come across dir {} inode {:#x}",
                            PathBuf::from(&os_str).display(),
                            inode
                        );

                        return Some(Ok(DirEntry { inode, d }));
                    }

                    let inode = INode::File(*source_index, d.ino());
                    debug!(
                        "MergedFS::ReadDir::next -- come across file {} in source {:#x} inode {:#x}",
                        PathBuf::from(d.path_within_source().into_os_string()).display(),
                        source_index,
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
    inode: INode,
    d: source::DirEntry<'a>,
}

impl<'a> DirEntry<'a> {
    fn from_dir_entry(d: source::DirEntry<'a>, inode: INode) -> Self {
        DirEntry { inode, d }
    }

    fn ino(&self) -> INode {
        self.inode
    }
}
