use crate::fs::inode::INodeTable;
use crate::fs::source::Source;
use crate::TTL;

use crate::fs::source;
use fuser::FileType::{Directory, RegularFile};
use fuser::{FileAttr, FileType, Filesystem, ReplyAttr, ReplyDirectory, Request};
use libc::ENOENT;
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
        MergedFS {
            sources: paths.map(|root| Source::new(root.to_path_buf())).collect(),
            directories: INodeTable::new(),
        }
    }

    fn get_dir_info<P: AsRef<Path>>(&self, ino: u64, p: P) -> FileAttr {
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
}

impl Filesystem for MergedFS {
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", ino);
        if ino == 1 {
            // get the root information.
            let mut attr: FileAttr = unsafe { mem::zeroed() };
            attr.ino = 1;
            attr.kind = FileType::Directory;
            attr.perm = 0o755;
            reply.attr(&TTL, &attr)
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let read_dir = if ino == 1 {
            // root
            Ok(self.read_dir("/"))
        } else if (ino & IS_DIR_MASK) == 0 {
            // not a directory
            Err(libc::EINVAL)
        } else {
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
        for dir in read_dir {
            if let Err(x) = dir {
                println!("error reading dir {}", x);
                continue;
            }
            let dir = dir.unwrap();
            let file_type = if dir.d.is_dir().unwrap_or(false) {
                Directory
            } else {
                RegularFile
            };

            reply.add(dir.inode, 0, file_type, dir.d.dir_entry().file_name());
        }
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
        while let Some((idx, read_dir)) = &mut self.current {
            match read_dir.next() {
                Some(Ok(v)) => {
                    if v.is_dir().unwrap() {
                        // because we want dirs to appear as though they exist on all disks,
                        // we have a separate inode table for just directories within the Merged FS.
                        // the inodes assigned to directories within the merged FS are unrelated to
                        // the inodes assigned by their sources.
                        //
                        // however, for files, we use the source inodes.
                        let os_str = v.path_within_source().into_os_string();
                        let dir_inode = self.src.directories.lookup_inode_from_merged_path(&os_str);
                        return Some(Ok(DirEntry {
                            inode: (dir_inode & SOURCE_INODE_MASK) | IS_DIR_MASK,
                            d: v,
                        }));
                    }
                    // for files...
                    return Some(Ok(DirEntry {
                        // store the source index in the first two bytes of the inode number.
                        inode: (MERGED_SOURCE_MASK & ((*idx as u64) << 56))
                            | (SOURCE_INODE_MASK & v.ino()), // and the source inode in the rest...
                        d: v,
                    }));
                }
                _ => {
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

    fn dir_entry(self) -> source::DirEntry<'a> {
        self.d
    }
}
