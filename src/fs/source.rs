use crate::fs::inode::INodeTable;
use fuser::FileType::Directory;
use fuser::{FileAttr, FileType};
use std::cmp::min;
use std::ffi::OsString;
use std::fs;
use std::fs::{create_dir_all, File};
use std::io;
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::result::Result;
use std::sync::Arc;

use crate::fs::attrs;
use crate::fs::attrs::fuse_attr;
use crate::fs::file_handle::{FileHandleManager, Mode};

#[derive(Debug)]
pub struct Source {
    root: PathBuf,
    inodes: INodeTable<u64>,
    fhm: Arc<FileHandleManager>,
}

impl<'a> Source {
    pub fn new(root: PathBuf, fhm: Arc<FileHandleManager>) -> Self {
        debug!("Source::new({})", root.display());
        Source {
            root,
            inodes: INodeTable::new(),
            fhm,
        }
    }

    pub fn open(&self, ino: u64, mode: Mode) -> io::Result<u64> {
        let path = self.merged_path_from_inode(ino)?;
        let path = self.extend_root(path);
        self.fhm.open(path, mode)
    }

    pub fn close(&self, ino: u64) -> io::Result<()> {
        self.fhm.close(ino)
    }

    fn merged_path_from_inode(&self, ino: u64) -> io::Result<PathBuf> {
        let path = self.inodes.lookup_merged_path_from_inode(ino);
        if let None = path {
            Err(io::Error::new(ErrorKind::NotFound, "not found"))
        } else {
            Ok(PathBuf::from(path.unwrap()))
        }
    }

    pub fn read(
        &self,
        ino: u64,
        offset: i64,
        amount_to_read: u32,
        buf: &mut [u8],
    ) -> io::Result<usize> {
        let path = self.merged_path_from_inode(ino)?;
        let fh = File::open(&self.extend_root(path));

        if let Err(e) = fh {
            return Err(e);
        }
        let fh = fh.unwrap();

        let remaining_file_size = fh.metadata().unwrap().len() as i64 - offset;
        let amount_to_read = min(
            amount_to_read as usize,
            min(remaining_file_size as usize, buf.len() as usize),
        );

        return fh.read_at(&mut buf[0..amount_to_read], offset as u64);
    }

    pub fn statfs(&self) -> io::Result<crate::fs::stats::FSStats> {
        debug!("Source::{}::statfs", self.root.display());
        attrs::fsstat(&self.root)
    }

    fn extend_root<P: AsRef<Path>>(&self, path_ref: P) -> PathBuf {
        let root = self.root.clone();
        let path = path_ref.as_ref();
        let rel_path = if path.is_absolute() {
            path.into_iter().skip(1).collect()
        } else {
            path.to_path_buf()
        };
        let got = root.join(rel_path);
        debug!(
            "extending root, {}, onto {} --> {}",
            self.root.display(),
            path.display(),
            got.display()
        );
        got
    }

    pub fn read_dir<P: AsRef<Path>>(&'a self, path_ref: P) -> io::Result<ReadDir> {
        debug!(
            "Source::{}::readdir({})",
            self.root.display(),
            path_ref.as_ref().display()
        );
        match fs::read_dir(self.extend_root(path_ref).as_path()) {
            Ok(rd) => Ok(ReadDir::<'a> { rd, src: self }),
            Err(x) => Err(x),
        }
    }

    pub fn create_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<FileAttr> {
        debug!(
            "Source::{}::create_dir({})",
            self.root.display(),
            path.as_ref().display()
        );
        let real_path = self.extend_root(&path);
        if let Err(x) = fs::create_dir(&real_path) {
            return Err(x);
        }
        fuse_attr(
            self.inodes.lookup_insert_inode_from_merged_path(
                &PathBuf::from(path.as_ref()).into_os_string(),
            ),
            &real_path,
        )
    }

    pub fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<FileAttr> {
        debug!(
            "Source::{}::create_dir_all({})",
            self.root.display(),
            path.as_ref().display()
        );
        let real_path = self.extend_root(&path);
        if let Err(x) = create_dir_all(&real_path) {
            return Err(x);
        }
        fuse_attr(
            self.inodes.lookup_insert_inode_from_merged_path(
                &PathBuf::from(path.as_ref()).into_os_string(),
            ),
            &real_path,
        )
    }

    pub fn unlink<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let real_path = self.extend_root(&path);
        let path = PathBuf::from(path.as_ref()).into_os_string();
        let attrs = fuse_attr(
            self.inodes.lookup_insert_inode_from_merged_path(&path),
            &real_path,
        )?;
        if attrs.kind != FileType::RegularFile {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "cannot unlink a non-regular file",
            ));
        }
        fs::remove_file(real_path)?;
        self.inodes.drop_path(&path);
        Ok(())
    }

    pub fn rmdir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let real_path = self.extend_root(&path);
        let path = PathBuf::from(path.as_ref()).into_os_string();
        let attrs = fuse_attr(
            self.inodes.lookup_insert_inode_from_merged_path(&path),
            &real_path,
        )?;
        if attrs.kind != Directory {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "cannot unlink a non-regular file",
            ));
        }
        if self.read_dir(&path)?.count() > 0 {
            return Err(io::Error::new(ErrorKind::Other, "directory is not empty"));
        }
        fs::remove_dir(real_path)?;
        self.inodes.drop_paths_with_prefix(&path);
        return Ok(());
    }

    pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> io::Result<()> {
        debug!(
            "Source::{}::rename(from: {}, to: {})",
            self.root.display(),
            from.as_ref().display(),
            to.as_ref().display(),
        );

        let attrs = self.fuse_attr_path(&from)?;
        if attrs.kind == Directory {
            debug!(
                "Source::{}::rename(from: {}, to: {}) -- directory",
                self.root.display(),
                from.as_ref().display(),
                to.as_ref().display(),
            );
            self.inodes
                .drop_paths_with_prefix(&PathBuf::from(from.as_ref()).into_os_string());
        } else {
            debug!(
                "Source::{}::rename(from: {}, to: {}) -- regular file",
                self.root.display(),
                from.as_ref().display(),
                to.as_ref().display(),
            );
            self.inodes
                .drop_path(&PathBuf::from(from.as_ref()).into_os_string());
        }

        let full_from = self.extend_root(from);
        let full_to = self.extend_root(to);

        if let Some(new_parent) = full_to.parent() {
            create_dir_all(new_parent)?
        }

        fs::rename(full_from, full_to)
    }

    pub fn create<P: AsRef<Path>>(&self, merged_path: P, mode: u32) -> io::Result<(FileAttr, u64)> {
        debug!(
            "Source::{}::create(path:{}, mode:{})",
            self.root.display(),
            merged_path.as_ref().display(),
            mode,
        );

        let merged_path = merged_path.as_ref().to_path_buf();
        let full_path = self.extend_root(&merged_path);
        let file_handle = self.fhm.create(&full_path)?;
        let ino = self
            .inodes
            .lookup_insert_inode_from_merged_path(&merged_path.into_os_string());
        let file_attrs = fuse_attr(ino, full_path)?;
        Ok((file_attrs, file_handle))
    }

    pub fn metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<fs::Metadata> {
        debug!(
            "Source::{}::metadata({})",
            self.root.display(),
            path.as_ref().display()
        );
        fs::metadata(self.extend_root(path))
    }

    pub fn fuse_attr_path<P: AsRef<Path>>(&self, path: P) -> io::Result<FileAttr> {
        debug!(
            "Source::{}::fuse_attr_path({})",
            self.root.display(),
            path.as_ref().display()
        );
        let pb = path.as_ref().to_path_buf().into_os_string();

        if let Some(ino) = self.inodes.lookup_inode_from_merged_path(&pb) {
            fuse_attr(ino, self.extend_root(path))
        } else {
            Err(io::Error::new(ErrorKind::NotFound, "not found"))
        }
    }

    pub fn fuse_attr(&self, ino: u64) -> io::Result<FileAttr> {
        debug!("Source::{}::fuse_attr({})", self.root.display(), ino);
        let path = self.inodes.lookup_merged_path_from_inode(ino);
        if let None = path {
            return Err(io::Error::new(ErrorKind::NotFound, "not found"));
        }
        let path = path.unwrap();
        fuse_attr(ino, self.extend_root(path))
    }
}

pub struct ReadDir<'a> {
    rd: fs::ReadDir,
    src: &'a Source,
}

impl<'a> Iterator for ReadDir<'a> {
    type Item = Result<DirEntry<'a>, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("Source::{}::Iterator::next", self.src.root.display());
        let val = match self.rd.next() {
            None => None,
            Some(Ok(item)) => {
                let item_path = item.path();
                let merged_path = strip_root(item_path, &self.src.root);
                let inode = self
                    .src
                    .inodes
                    .lookup_insert_inode_from_merged_path(&merged_path.into_os_string());
                Some(Ok(DirEntry::from_dir_entry(self.src, item, inode)))
            }
            Some(Err(x)) => Some(Err(x)),
        };
        debug!("next is {:?}", val);
        val
    }
}

#[derive(Debug)]
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
        self.inode.into()
    }

    pub fn file_name(&self) -> OsString {
        self.d.file_name()
    }

    pub fn path_within_source(&self) -> PathBuf {
        let out = strip_root(&self.d.path(), &self.src.root);
        debug!("DirEntry::path_within_source -- source root is {}, full path is {}, path within source is {}", self.src.root.display(), self.d.path().display(), out.display());
        out
    }

    pub fn is_dir(&self) -> Option<bool> {
        if let Ok(m) = self.d.metadata() {
            return Some(m.is_dir());
        }
        None
    }
}

fn strip_root<P: AsRef<Path>, Q: AsRef<Path>>(path: P, root: Q) -> PathBuf {
    let mut out = PathBuf::from("/");
    out.push(path.as_ref().strip_prefix(root.as_ref()).unwrap());
    out
}
