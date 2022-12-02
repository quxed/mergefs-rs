use crate::fs::inode::{INode, INodeTable};
use crate::fs::source::Source;
use crate::{fs, TTL};

use crate::fs::file_handle::{FileHandleManager, Mode};
use crate::fs::source;
use fuser::FileType::{Directory, RegularFile};
use fuser::{
    FileAttr, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use libc::{EACCES, EINVAL, EIO, ENOENT, O_RDONLY, O_TRUNC};
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use std::{io, mem};

pub struct MergedFS {
    sources: Vec<Source>,
    directories: INodeTable<u64>, // directories are mapped across all disks.
    fhm: Arc<FileHandleManager>,
}

impl MergedFS {
    pub fn new<'a, I>(paths: I, fhm: Arc<FileHandleManager>) -> Self
    where
        I: Iterator<Item = &'a Path>,
    {
        debug!("MergedFS::new(...)");
        let paths: Vec<PathBuf> = paths.map(|x| PathBuf::from(x)).collect();
        for p in &paths {
            if p.is_relative() {
                panic!("paths must be absolute. {} is relative", p.display());
            }
        }

        MergedFS {
            sources: paths
                .iter()
                .map(|root| Source::new(PathBuf::from(root), fhm.clone()))
                .collect(),
            directories: INodeTable::new(),
            fhm,
        }
    }

    fn nominate_source_for_creation(&self) -> io::Result<&Source> {
        // find the source that has the most space.
        if self.sources.len() == 0 {
            return Err(io::Error::new(
                ErrorKind::Other,
                "no space, as there are no sources",
            ));
        }
        let mut nominee = self.sources.get(0).unwrap();
        let stats = nominee.statfs()?;
        let mut nominee_free = stats.bavail * stats.bsize;
        for v in &self.sources[1..] {
            if let Ok(stats) = v.statfs() {
                let tmp = stats.bavail * stats.bsize;
                if tmp > nominee_free {
                    nominee = v;
                    nominee_free = tmp;
                }
            }
        }
        Ok(nominee)
    }

    fn get_dir_info<P: AsRef<Path>>(&self, ino: u64, p: P) -> FileAttr {
        debug!(
            "MergedFS::get_dir_info(ino: {:#x}, p:{})",
            ino,
            p.as_ref().display()
        );

        let mut attr: FileAttr = unsafe { mem::zeroed() };
        attr.ino = ino;
        attr.kind = Directory;
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
            if let Ok(read_dir) = self.sources.get(i).unwrap().read_dir(path) {
                tmp.push((i as u16, read_dir))
            }
        }

        return Ok(ReadDir {
            current: tmp.pop(),
            rd: tmp,
            src: self,
        });
    }

    fn get_attr_dir(&self, ino: INode) -> Option<FileAttr> {
        debug!("MergedFS::get_attr_dir({:#x})", ino);
        return match ino {
            INode::Dir(ino) => {
                let dir = self.directories.lookup_merged_path_from_inode(ino)?;
                for v in &self.sources {
                    if let Ok(x) = v.fuse_attr_path(&dir) {
                        return Some(x);
                    }
                }
                None
            }
            _ => None,
        };
    }

    fn fuse_attr(&self, ino: u64) -> Option<FileAttr> {
        let ino = INode::from(ino);
        match ino {
            d @ INode::Dir(_) => self.get_attr_dir(d),
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

    fn create_dir_all<P: AsRef<Path>>(&self, path: P, source: &Source) -> io::Result<INode> {
        let path = path.as_ref();
        source.create_dir_all(path)?;
        match self
            .directories
            .lookup_inode_from_merged_path(&path.to_path_buf().into_os_string())
        {
            Some(x) => Ok(INode::Dir(x)),
            None => Err(io::Error::new(
                ErrorKind::Other,
                format!("unable to assign inode for path {}", path.display()),
            )),
        }
    }

    fn extend_dir_path_by_inode(&self, parent: u64, name: &OsStr) -> io::Result<PathBuf> {
        let parent_path = self.directories.lookup_merged_path_from_inode(parent);
        if let None = parent_path {
            error!(
                "MergedFS::extend_dir_path_by_inode(parent:{}, name: {}) -- parent inode does not exist",
                parent,
                PathBuf::from(name).display()
            );
            debug!(
                "MergedFS::extend_dir_path_by_inode(parent:{}, name: {}) -- directory inodes {:?}",
                parent,
                PathBuf::from(name).display(),
                self.directories,
            );
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "unable to find path for parent directory with given inode",
            ));
        }

        let mut new_path = PathBuf::from(parent_path.unwrap());
        new_path.push(name);
        Ok(new_path)
    }
}

fn inject_source_index(n: INode, s: u16) -> INode {
    match n {
        d @ INode::Dir(_) => d,
        INode::File(_, ino) => INode::File(s, ino),
    }
}

impl Filesystem for MergedFS {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("MergedFS::lookup(parent:{:#x}, name:{:?})", parent, name);

        // look up the parent directory
        let parent_path = self.directories.lookup_merged_path_from_inode(parent);
        if let None = parent_path {
            reply.error(ENOENT);
            return;
        }

        let mut target_path = PathBuf::from(parent_path.unwrap());
        target_path.push(name);
        let target_path = target_path.as_path();

        if let Some(mut attr) = self.fuse_attr_from_path(&target_path) {
            // if the file is a directory, return our directory inode, rather than the source's inode.
            if attr.kind == Directory {
                let ino = self.directories.lookup_insert_inode_from_merged_path(
                    &PathBuf::from(&target_path).into_os_string(),
                );
                attr.ino = ino;
            }
            reply.entry(&TTL, &attr, 1);
        } else {
            reply.error(ENOENT);
        }
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
                    reply.error(ENOENT);
                    return;
                }

                attr.unwrap()
            }
            INode::File(src_idx, ino) => {
                let src = self.sources.get(src_idx as usize);
                if let None = src {
                    debug!("MergedFS::getattr -- file inode {:#x} not found", ino);
                    reply.error(ENOENT);
                    return;
                }
                let src = src.unwrap();
                let attr = src.fuse_attr(ino);
                if let Err(e) = attr {
                    if e.kind() == ErrorKind::NotFound {
                        reply.error(ENOENT);
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

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        debug!(
            "MergedFS::mkdir(parent:{}, name: {})",
            parent,
            PathBuf::from(name).display()
        );
        let new_path = self.extend_dir_path_by_inode(parent, name);
        if let Err(e) = new_path {
            error!(
                "MergedFS::mkdir(parent:{}, name: {}) -- error building path with name and parent inode: {}",
                parent,
                PathBuf::from(name).display(),
                e
            );
            reply.error(EINVAL);
            return;
        }
        let new_path = new_path.unwrap();
        let source = self.nominate_source_for_creation();
        if let Err(x) = source {
            error!(
                "MergedFS::mkdir(parent:{}, name: {}) -- error nominating source: {}",
                parent,
                PathBuf::from(name).display(),
                x
            );
            reply.error(EIO);
            return;
        }
        let source = source.unwrap();
        match source.create_dir_all(&new_path) {
            Err(x) => {
                error!(
                    "MergedFS::mkdir(parent:{}, name: {}) -- error creating directory: {}",
                    parent,
                    PathBuf::from(name).display(),
                    x,
                );
                reply.error(EIO);
            }
            Ok(mut attr) => {
                let new_ino = self
                    .directories
                    .lookup_insert_inode_from_merged_path(&new_path.into_os_string());
                attr.ino = new_ino;
                reply.entry(&TTL, &attr, 1);
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("MergedFS::unlink(parent:{:#x}, name:{:?})", parent, name);
        let path = self.extend_dir_path_by_inode(parent, name);
        if let Err(e) = path {
            debug!(
                "MergedFS::unlink(parent:{:#x}, name:{:?}) -- cannot extend path {}",
                parent, name, e
            );
            reply.error(EIO);
            return;
        }
        let path = path.unwrap();
        let attrs = self.fuse_attr_from_path(&path);
        if let None = attrs {
            reply.error(ENOENT);
            return;
        }
        let attrs = attrs.unwrap();
        match INode::from(attrs.ino) {
            INode::Dir(_) => {
                reply.error(EINVAL);
                return;
            }
            INode::File(src, _) => {
                let source = self.sources.get(src as usize);
                if let None = source {
                    warn!(
                        "MergedFS::unlink(parent:{:#x}, name:{:?}) -- no source with id {:#x}",
                        parent, name, src
                    );
                    reply.error(EIO);
                    return;
                }
                let source = source.unwrap();
                match source.unlink(path) {
                    Err(e) => {
                        warn!(
                            "MergedFS::unlink(parent:{:#x}, name:{:?}) -- error unlinking file {}",
                            parent, name, e
                        );
                        reply.error(EIO);
                    }
                    Ok(_) => reply.ok(),
                }
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("MergedFS::rmdir(parent:{:#x}, name:{:?})", parent, name);
        let path = self.extend_dir_path_by_inode(parent, name);
        if let Err(e) = path {
            debug!(
                "MergedFS::rmdir(parent:{:#x}, name:{:?}) -- cannot extend path {}",
                parent, name, e
            );
            reply.error(EIO);
            return;
        }
        let path = path.unwrap();
        for src in &self.sources {
            if let Err(e) = src.rmdir(&path) {
                if e.kind() != ErrorKind::NotFound {
                    debug!(
                        "MergedFS::rmdir(parent:{:#x}, name:{:?}) -- error deleting from source {}",
                        parent, name, e
                    );
                    // ignore not founds...
                    reply.error(EIO);
                    return;
                }
            }
        }
        reply.ok();
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        old_parent: u64,
        old_name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        debug!(
            "MergedFS::rename(old_parent:{}, old_name: {},new_parent:{}, new_name: {})",
            old_parent,
            PathBuf::from(old_name).display(),
            new_parent,
            PathBuf::from(new_name).display(),
        );
        let old_path = self.extend_dir_path_by_inode(old_parent, old_name);
        if let Err(e) = old_path {
            error!(
                "MergedFS::rename(old_parent:{}, old_name: {},new_parent:{}, new_name: {}) -- unable to extend old path: {}",
                old_parent,
                PathBuf::from(old_name).display(),
                new_parent,
                PathBuf::from(new_name).display(),
                e,
            );
            reply.error(EIO);
            return;
        }
        let old_path = old_path.unwrap();
        let new_path = self.extend_dir_path_by_inode(new_parent, new_name);
        if let Err(e) = new_path {
            error!(
                "MergedFS::rename(old_parent:{}, old_name: {},new_parent:{}, new_name: {}) -- unable to extend new path: {}",
                old_parent,
                PathBuf::from(old_name).display(),
                new_parent,
                PathBuf::from(new_name).display(),
                e,
            );
            reply.error(EIO);
            return;
        }
        let new_path = new_path.unwrap();
        // find which source the old file is on, and move it within the source.
        // annoyingly, again it seems we don't have the inode, so we're going to have to do a lookup.
        let old_attrs = self.fuse_attr_from_path(&old_path);
        if let None = old_attrs {
            warn!(
                "MergedFS::rename(old_parent:{}, old_name: {},new_parent:{}, new_name: {}) -- no source contains file {}",
                old_parent,
                PathBuf::from(old_name).display(),
                new_parent,
                PathBuf::from(new_name).display(),
                old_path.display()
            );
            reply.error(ENOENT);
            return;
        }
        let old_attrs = old_attrs.unwrap();
        let res = match INode::from(old_attrs.ino) {
            INode::File(src, _) => {
                let source = self.sources.get(src as usize);
                if let None = source {
                    warn!(
                        "MergedFS::rename(old_parent:{}, old_name: {},new_parent:{}, new_name: {}) -- no source with id {}",
                        old_parent,
                        PathBuf::from(old_name).display(),
                        new_parent,
                        PathBuf::from(new_name).display(),
                        src
                    );
                    reply.error(EINVAL);
                    return;
                }
                let source = source.unwrap();
                source.rename(old_path, new_path)
            }
            INode::Dir(_) => {
                self.directories
                    .drop_paths_with_prefix(&old_path.clone().into_os_string());
                // rename across all sources...
                for src in &self.sources {
                    if let Err(e) = src.rename(&old_path, &new_path) {
                        if e.kind() != ErrorKind::NotFound {
                            warn!(
                                "MergedFS::rename(old_parent:{}, old_name: {},new_parent:{}, new_name: {}) -- error moving directory {}",
                                old_parent,
                                PathBuf::from(old_name).display(),
                                new_parent,
                                PathBuf::from(new_name).display(),
                                e
                            );
                            reply.error(EIO);
                            return;
                        }
                    }
                }
                Ok(())
            }
        };
        match res {
            Ok(()) => reply.ok(),
            Err(e) => {
                warn!(
                    "MergedFS::rename(old_parent:{}, old_name: {},new_parent:{}, new_name: {}) -- error moving file {}",
                    old_parent,
                    PathBuf::from(old_name).display(),
                    new_parent,
                    PathBuf::from(new_name).display(),
                    e
                );
                reply.error(EIO);
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        debug!("MergedFS::open(ino: {:#x}, flags:{:#x})", ino, flags,);
        if let INode::File(src, ino) = INode::from(ino) {
            let src = self.sources.get(src as usize);
            if let None = src {
                error!("MergedFS::open(ino: {:#x}, flags:{:#x}) -- inode references a source that does not exist", ino, flags);
                reply.error(EINVAL);
                return;
            }
            let src = src.unwrap();
            let mode = match flags & libc::O_ACCMODE {
                O_RDONLY => {
                    if flags & O_TRUNC != 0 {
                        error!("MergedFS::open(ino: {:#x}, flags:{:#x}) -- O_TRUNC ({:#x}) on a O_RDONLY ({:#x}) open", ino, flags, O_TRUNC, O_RDONLY);
                        reply.error(EACCES);
                        return;
                    }
                    Mode::Read
                }
                libc::O_WRONLY => {
                    if flags & O_TRUNC == 0 {
                        Mode::Append(flags & libc::O_CREAT != 0)
                    } else {
                        Mode::Trunc(flags & libc::O_CREAT != 0)
                    }
                }
                libc::O_RDWR => {
                    if flags & O_TRUNC == 0 {
                        Mode::Append(flags & libc::O_CREAT != 0)
                    } else {
                        Mode::Trunc(flags & libc::O_CREAT != 0)
                    }
                }
                _ => {
                    error!(
                        "MergedFS::open(ino: {:#x}, flags:{:#x}) -- unrecognised mode",
                        ino, flags
                    );
                    reply.error(EINVAL);
                    return;
                }
            };
            match src.open(ino, mode) {
                Err(e) => {
                    let err = match e.kind() {
                        ErrorKind::NotFound => ENOENT,
                        _ => EIO,
                    };
                    error!(
                        "MergedFS::open(ino: {:#x}, flags:{:#x}) -- error opening file {}",
                        ino, flags, err
                    );
                    reply.error(err);
                    return;
                }
                Ok(x) => {
                    reply.opened(x, 0);
                }
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        requested_read_size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        debug!(
            "MergedFS::read(fh:{}, offset:{}, requested_read:{})",
            fh, offset, requested_read_size
        );
        let file_size = match self.fhm.size(fh) {
            Err(x) => {
                warn!("MergedFS::read(fh:{}, offset:{}, requested_read:{}) -- error getting handle {} during read. {}", fh, offset, requested_read_size, fh, x);
                reply.error(EIO);
                return;
            }
            Ok(x) => x,
        };
        let read_amount = if (requested_read_size as i64) + offset > file_size as i64 {
            // we'd read off the end of the file , so reduce the requested_read_size to the
            // remaining number of bytes.
            file_size - offset as u64
        } else {
            requested_read_size as u64
        };
        let mut buf = vec![0 as u8; read_amount as usize];
        match self.fhm.read(fh, offset as u64, buf.as_mut_slice()) {
            Err(e) => {
                warn!(
                    "MergedFS::read(fh:{}, offset:{}, requested_read:{}) -- error reading {} bytes of handle {}: {}",
                    fh, offset, requested_read_size, read_amount, fh, e
                );
                reply.error(EIO);
            }
            Ok(x) => {
                reply.data(&buf[0..x]);
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        debug!(
            "MergedFS::write(fh:{}, offset:{}, data_len:{})",
            fh,
            offset,
            data.len(),
        );

        match self.fhm.write(fh, offset as u64, data) {
            Err(e) => {
                debug!(
                    "MergedFS::write(fh:{}, offset:{}, data_len:{}) -- error writing data {}",
                    fh,
                    offset,
                    data.len(),
                    e,
                );
                reply.error(EIO);
            }
            Ok(x) => {
                reply.written(x as u32);
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("MergedFS::release(fh: {:#x})", fh,);
        match self.fhm.close(fh) {
            Ok(()) => reply.ok(),
            Err(x) => {
                error!(
                    "MergedFS::release(fh: {:#x}) -- error releasing handle: {}",
                    fh, x
                );
                reply.error(EIO)
            }
        }
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
                    Err(EINVAL)
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

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        debug!(
            "MergedFS::create(parent:{}, name: {})",
            parent,
            PathBuf::from(name).display()
        );

        let parent_path = self.directories.lookup_merged_path_from_inode(parent);
        if let None = parent_path {
            error!(
                "MergedFS::create(parent:{}, name: {}) -- parent inode does not exist",
                parent,
                PathBuf::from(name).display()
            );
            reply.error(ENOENT);
            return;
        }
        let parent_path = PathBuf::from(parent_path.unwrap());
        let source = self.nominate_source_for_creation();
        if let Err(e) = source {
            error!(
                "MergedFS::create(parent:{}, name: {}) -- unable to nominate a source for creation: {}",
                parent,
                PathBuf::from(name).display(),
                e,
            );
            reply.error(EIO);
            return;
        }
        let source = source.unwrap();
        let create_parent_result = self.create_dir_all(&parent_path, source);
        if let Err(e) = create_parent_result {
            error!("MergedFS::create(parent:{}, name: {}) -- unable to create parent ({}) in source: {}", parent, PathBuf::from(name).display(), parent_path.display(), e);
            reply.error(EIO);
            return;
        }
        let mut parent_path = parent_path;
        parent_path.push(name);
        let created = source.create(&parent_path, mode);
        if let Err(e) = created {
            error!(
                "MergedFS::create(parent:{}, name: {}) -- unable to create file ({}) in source: {}",
                parent,
                PathBuf::from(name).display(),
                parent_path.display(),
                e
            );
            reply.error(EIO);
            return;
        }
        let (attr, fh) = created.unwrap();
        reply.created(&TTL, &attr, 1, fh, 0);
    }
}

pub struct ReadDir<'a> {
    current: Option<(u16, source::ReadDir<'a>)>,
    rd: Vec<(u16, source::ReadDir<'a>)>,
    src: &'a MergedFS,
}

impl<'a> Iterator for ReadDir<'a> {
    type Item = Result<DirEntry<'a>, io::Error>;
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
