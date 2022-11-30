use std::collections::BTreeMap;
use std::fs::File;
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::{fs, io};

use crate::fs::file_handle::FileHandle::{ReadOnly, ReadWrite};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug)]
pub enum Mode {
    Read,
    Trunc(bool),
    Append(bool),
}

impl Mode {
    pub fn create(&self) -> bool {
        match self {
            Mode::Read => false,
            Mode::Trunc(x) => *x,
            Mode::Append(x) => *x,
        }
    }
}

#[derive(Debug)]
pub enum FileHandle {
    ReadOnly(fs::File),
    ReadWrite(fs::File),
}

impl FileHandle {
    fn file(&self) -> &fs::File {
        match self {
            ReadOnly(x) => x,
            ReadWrite(x) => x,
        }
    }
}

#[derive(Debug)]
struct FileHandleManagerFields {
    position: u64,
    handles: BTreeMap<u64, FileHandle>,
}

#[derive(Debug)]
pub struct FileHandleManager {
    fhs: RwLock<FileHandleManagerFields>,
}

impl FileHandleManager {
    pub fn new() -> Self {
        FileHandleManager {
            fhs: RwLock::new(FileHandleManagerFields {
                position: 1,
                handles: BTreeMap::new(),
            }),
        }
    }

    pub fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<u64> {
        self.open(path, Mode::Trunc(true))
    }

    pub fn open<P: AsRef<Path>>(&self, path: P, mode: Mode) -> io::Result<u64> {
        debug!(
            "FileHandleManager::open(path: {}, mode: {:?})",
            path.as_ref().display(),
            mode
        );
        let mut fields = self.obtain_write_lock()?;
        let mut opts = &mut fs::OpenOptions::new();
        opts = match &mode {
            Mode::Read => opts.read(true),
            Mode::Trunc(_) => opts.write(true).truncate(true).create(mode.create()),
            Mode::Append(_) => opts.write(true).append(true).create(mode.create()),
        };
        let f = opts.open(path);
        if let Err(e) = f {
            return Err(e);
        }

        let f = match &mode {
            Mode::Read => ReadOnly(f.unwrap()),
            Mode::Trunc(_) => ReadWrite(f.unwrap()),
            Mode::Append(_) => ReadWrite(f.unwrap()),
        };
        let idx = fields.position;
        fields.position = fields.position + 1;
        fields.handles.insert(idx, f);
        return Ok(idx);
    }

    pub fn read(&self, fh: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let fields = self.obtain_read_lock()?;
        let fh = match fields.handles.get(&fh) {
            Some(fh) => Ok(fh),
            None => Err(io::Error::new(
                ErrorKind::InvalidInput,
                "invalid file handle",
            )),
        }?;
        fh.file().read_at(buf, offset)
    }

    pub fn write(&self, fh: u64, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let fields = self.obtain_read_lock()?;
        let fh = match fields.handles.get(&fh) {
            Some(fh) => Ok(fh),
            None => Err(io::Error::new(
                ErrorKind::InvalidInput,
                "invalid file handle",
            )),
        }?;
        fh.file().write_at(buf, offset)
    }

    pub fn close(&self, fh: u64) -> io::Result<()> {
        let mut fields = self.obtain_write_lock()?;
        match fields.handles.remove(&fh) {
            None => Err(io::Error::new(
                ErrorKind::InvalidInput,
                "invalid file handle",
            )),
            Some(_) => Ok(()),
        }
    }

    pub fn size(&self, fh: u64) -> io::Result<u64> {
        let fields = self.obtain_read_lock()?;
        let mut fh = match fields.handles.get(&fh) {
            Some(fh) => Ok(fh),
            None => Err(io::Error::new(
                ErrorKind::InvalidInput,
                "invalid file handle",
            )),
        }?;
        Ok(fh.file().metadata()?.len())
    }

    fn obtain_write_lock(&self) -> io::Result<RwLockWriteGuard<FileHandleManagerFields>> {
        let fields = self.fhs.write();
        if let Err(e) = fields {
            error!("file handle lock is poisoned: {}", e);
            return Err(io::Error::new(
                ErrorKind::Other,
                "file handle lock is poisoned",
            ));
        }
        return Ok(fields.unwrap());
    }

    fn obtain_read_lock(&self) -> io::Result<RwLockReadGuard<FileHandleManagerFields>> {
        let fields = self.fhs.read();
        if let Err(e) = fields {
            error!("file handle lock is poisoned: {}", e);
            return Err(io::Error::new(
                ErrorKind::Other,
                "file handle lock is poisoned",
            ));
        }
        return Ok(fields.unwrap());
    }
}
