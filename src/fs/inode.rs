use std::collections::BTreeMap;
use std::ffi::OsString;
use std::sync::RwLock;

// #[derive(Debug)]
// enum INode {
//     Dir(u64),
//     File(u16, u64),
// }
//
// impl From<INode> for u64 {
//     fn from(val: INode) -> Self {
//         match val {
//             INode::Dir(v) => v & SOURCE_INODE_MASK,
//             INode::File(s, v) => ((s as u64) << 48) | v,
//         }
//     }
// }
//
// impl From<u64> for INode {
//     fn from(val: u64) -> Self {
//         if (val & IS_DIR_MASK) > 0 {
//             INode::Dir(val & SOURCE_INODE_MASK)
//         } else {
//             let source = (val & MERGED_SOURCE_MASK) >> 48;
//             INode::File(source as u16, val & SOURCE_INODE_MASK)
//         }
//     }
// }

#[derive(Debug)]
pub struct INodeTable {
    fields: RwLock<INodeTableFields>,
}

#[derive(Debug)]
struct INodeTableFields {
    position: u64, // keeps track of the highest inode currently assigned.
    merged_path_to_inode: BTreeMap<OsString, u64>,
    inode_to_merged_path: BTreeMap<u64, OsString>,
}

impl<'a> INodeTable {
    pub fn new() -> Self {
        debug!("INodeTable::new");
        let item = INodeTable {
            fields: RwLock::new(INodeTableFields {
                position: 0,
                merged_path_to_inode: BTreeMap::<OsString, u64>::new(),
                inode_to_merged_path: BTreeMap::<u64, OsString>::new(),
            }),
        };

        // insert root.
        item.add_inode_for_merged_path_safe(OsString::from("/"));
        return item;
    }

    pub fn lookup_inode_from_merged_path(&self, str: &OsString) -> Option<u64> {
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

    pub fn lookup_insert_inode_from_merged_path(&self, str: &OsString) -> u64 {
        debug!(
            "INodeTable::lookup_insert_inode_from_merged_path({:?})",
            str.clone()
        );
        if let Some(i) = self.lookup_inode_from_merged_path_cache_safe(str) {
            debug!(
                "INodeTable::lookup_insert_inode_from_merged_path({:?}) -- found {}",
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

    pub fn lookup_merged_path_from_inode(&self, ino: u64) -> Option<OsString> {
        debug!("INodeTable::lookup_merged_path_from_inode({:#x})", ino);
        let fields = self.fields.read().unwrap();
        fields
            .inode_to_merged_path
            .get(&ino)
            .and_then(|x| Some(OsString::from(x)))
    }

    fn lookup_inode_from_merged_path_cache_safe(&self, str: &OsString) -> Option<u64> {
        let fields = self.fields.read().unwrap();
        fields.merged_path_to_inode.get(str).and_then(|x| Some(*x))
    }

    fn add_inode_for_merged_path_safe(&self, str: OsString) -> u64 {
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
                "INodeTable::add_inode_for_merged_path_safe({}) -- inode inserted while waiting for lock: {}",
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
            "add_inode_for_merged_path_safe({}) -- inode inserted by this request: {}",
            str.clone().to_str().unwrap_or("invalid characters"),
            position_idx
        );
        return position_idx;
    }
}
