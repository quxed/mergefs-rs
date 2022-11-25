use crate::fs::inode::INodeTable;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::result::Result;

pub struct Source {
    root: PathBuf,
    inodes: INodeTable,
}

impl<'a> Source {
    pub fn new(root: PathBuf) -> Self {
        Source {
            root,
            inodes: INodeTable::new(),
        }
    }

    fn extend_root<P: AsRef<Path>>(&self, path_ref: P) -> PathBuf {
        let root = self.root.clone();
        let path = path_ref.as_ref();
        let rel_path = if path.is_absolute() {
            path.into_iter().skip(1).collect()
        } else {
            path.to_path_buf()
        };
        root.join(rel_path)
    }

    pub fn read_dir<P: AsRef<Path>>(&'a self, path_ref: P) -> io::Result<ReadDir> {
        match fs::read_dir(self.extend_root(path_ref).as_path()) {
            Ok(rd) => Ok(ReadDir::<'a> { rd, src: self }),
            Err(x) => Err(x),
        }
    }

    pub fn create_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        fs::create_dir(self.extend_root(path))
    }

    pub fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        fs::create_dir_all(self.extend_root(path))
    }

    pub fn metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<fs::Metadata> {
        fs::metadata(self.extend_root(path))
    }
}

pub struct ReadDir<'a> {
    rd: fs::ReadDir,
    src: &'a Source,
}

impl<'a> Iterator for ReadDir<'a> {
    type Item = Result<DirEntry<'a>, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rd.next() {
            None => None,
            Some(Ok(item)) => {
                let os_path = item.path().into_os_string();
                let inode = self.src.inodes.lookup_inode_from_merged_path(&os_path);
                Some(Ok(DirEntry::from_dir_entry(self.src, item, inode)))
            }
            Some(Err(x)) => Some(Err(x)),
        }
    }
}

pub struct DirEntry<'a> {
    inode: u64,
    d: fs::DirEntry,
    src: &'a Source,
}

impl<'a> DirEntry<'a> {
    fn from_dir_entry(src: &'a Source, d: fs::DirEntry, inode: u64) -> Self {
        DirEntry::<'a> { inode, d, src }
    }

    pub fn ino(&self) -> u64 {
        self.inode
    }

    pub fn dir_entry(&'a self) -> &'a fs::DirEntry {
        &self.d
    }

    pub fn path_within_source(&self) -> PathBuf {
        self.d
            .path()
            .clone()
            .strip_prefix(self.src.root.clone())
            .unwrap()
            .to_path_buf()
    }

    pub fn is_dir(&self) -> Option<bool> {
        if let Ok(m) = self.d.metadata() {
            return Some(m.is_dir());
        }
        None
    }
}
