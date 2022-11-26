#[derive(Debug)]
pub struct FSStats {
    pub fsid: u64,
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u64,
    pub namelen: u64,
}
