use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt::{Debug, Formatter, LowerHex};
use std::ops::Add;
use std::sync::RwLock;

const SOURCE_INODE_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;
const IS_FILE_MASK: u64 = 0xA000_0000_0000_0000;
const MERGED_SOURCE_MASK: u64 = !(SOURCE_INODE_MASK | IS_FILE_MASK);

#[derive(Debug)]
pub enum INode {
    Dir(u64),
    File(u16, u64),
}

impl Clone for INode {
    fn clone(&self) -> Self {
        match self {
            INode::Dir(nod) => INode::Dir(*nod),
            INode::File(src, nod) => INode::File(*src, *nod),
        }
    }
}

impl Add<u64> for INode {
    type Output = INode;

    fn add(self, rhs: u64) -> Self::Output {
        match self {
            INode::Dir(x) => INode::Dir(x + rhs),
            INode::File(s, x) => INode::File(s, x + rhs),
        }
    }
}

impl Eq for INode {}

impl PartialEq<Self> for INode {
    fn eq(&self, other: &Self) -> bool {
        self.to_u64() == other.to_u64()
    }
}

impl PartialOrd<Self> for INode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.to_u64().partial_cmp(&other.to_u64())
    }
}

impl Ord for INode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_u64().cmp(&other.to_u64())
    }
}
impl Copy for INode {}

impl LowerHex for INode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let tmp: u64 = self.into();
        f.write_fmt(format_args!("{:#016x}", tmp))
    }
}

impl INode {
    pub fn is_root(&self) -> bool {
        match self {
            INode::Dir(x) => *x == 1,
            INode::File(_, _) => false,
        }
    }

    fn to_u64(&self) -> u64 {
        match self {
            INode::Dir(v) => (*v & SOURCE_INODE_MASK) & !IS_FILE_MASK,
            INode::File(s, v) => (((*s as u64) << 48) | *v) | IS_FILE_MASK,
        }
    }
}

impl From<&INode> for u64 {
    fn from(val: &INode) -> Self {
        val.to_u64()
    }
}

impl From<INode> for u64 {
    fn from(val: INode) -> Self {
        val.to_u64()
    }
}

impl From<u64> for INode {
    fn from(val: u64) -> Self {
        if (val & IS_FILE_MASK) > 0 {
            let source = (val & MERGED_SOURCE_MASK) >> 48;
            INode::File(source as u16, val & SOURCE_INODE_MASK)
        } else {
            INode::Dir(val & SOURCE_INODE_MASK)
        }
    }
}

#[derive(Debug)]
pub struct INodeTable<INodeType> {
    fields: RwLock<INodeTableFields<INodeType>>,
}

#[derive(Debug)]
struct INodeTableFields<INodeType> {
    position: INodeType, // keeps track of the highest inode currently assigned.
    merged_path_to_inode: BTreeMap<OsString, INodeType>,
    inode_to_merged_path: BTreeMap<INodeType, OsString>,
}

impl<'a, T> INodeTable<T>
where
    T: Debug,
    T: Copy,
    T: Add<u64, Output = T>,
    T: Ord,
    T: Default,
{
    pub fn new() -> Self {
        debug!("INodeTable::new");
        let item = INodeTable {
            fields: RwLock::new(INodeTableFields::<T> {
                position: Default::default(),
                merged_path_to_inode: BTreeMap::<OsString, T>::new(),
                inode_to_merged_path: BTreeMap::<T, OsString>::new(),
            }),
        };

        // insert root.
        item.add_inode_for_merged_path_safe(OsString::from("/"));
        return item;
    }

    pub fn lookup_inode_from_merged_path(&self, str: &OsString) -> Option<T> {
        debug!(
            "INodeTable::lookup_inode_from_merged_path({:?})",
            str.clone()
        );
        let res = self.lookup_inode_from_merged_path_cache_safe(str);
        debug!(
            "INodeTable::lookup_inode_from_merged_path({:?}) -- returning {:?}",
            str.clone(),
            res
        );
        res
    }

    pub fn lookup_insert_inode_from_merged_path(&self, str: &OsString) -> T {
        debug!(
            "INodeTable::lookup_insert_inode_from_merged_path({:?})",
            str.clone()
        );
        if let Some(i) = self.lookup_inode_from_merged_path_cache_safe(str) {
            debug!(
                "INodeTable::lookup_insert_inode_from_merged_path({:?}) -- found {:?}",
                str.clone(),
                i
            );
            return i;
        }
        debug!(
            "INodeTable::lookup_insert_inode_from_merged_path({:?}) -- not found",
            str.clone(),
        );
        self.add_inode_for_merged_path_safe(str.to_os_string())
    }

    pub fn lookup_merged_path_from_inode(&self, ino: T) -> Option<OsString> {
        debug!("INodeTable::lookup_merged_path_from_inode({:?})", ino);
        let fields = self.fields.read().unwrap();
        fields
            .inode_to_merged_path
            .get(&ino)
            .and_then(|x| Some(OsString::from(x)))
    }

    fn lookup_inode_from_merged_path_cache_safe(&self, str: &OsString) -> Option<T> {
        let fields = self.fields.read().unwrap();
        fields.merged_path_to_inode.get(str).and_then(|x| Some(*x))
    }

    fn add_inode_for_merged_path_safe(&self, str: OsString) -> T {
        debug!(
            "INodeTable::add_inode_for_merged_path_safe({})",
            str.clone().to_str().unwrap_or("invalid characters")
        );

        let mut fields = self.fields.write().unwrap();
        debug!(
            "INodeTable::add_inode_for_merged_path_safe({}) -- lock obtained",
            str.clone().to_str().unwrap_or("invalid characters")
        );

        // check whether we've had something inserted while we were waiting for the lock.
        if let Some(i) = fields.merged_path_to_inode.get(&str) {
            // yup, just return it.
            debug!(
                "INodeTable::add_inode_for_merged_path_safe({}) -- inode inserted while waiting for lock: {:?}",
                str.clone().to_str().unwrap_or("invalid characters"),
                *i
            );
            return *i;
        }

        let position_idx = fields.position + 1;
        fields.position = position_idx;

        fields
            .merged_path_to_inode
            .insert(str.clone(), position_idx);
        fields
            .inode_to_merged_path
            .insert(position_idx, str.clone());

        debug!(
            "add_inode_for_merged_path_safe({}) -- inode inserted by this request: {:?}",
            str.clone().to_str().unwrap_or("invalid characters"),
            fields.position
        );
        return fields.position;
    }
}
