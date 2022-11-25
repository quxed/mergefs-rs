use std::collections::BTreeMap;
use std::ffi::OsString;
use std::sync::RwLock;

pub struct INodeTable {
    fields: RwLock<INodeTableFields>,
}

struct INodeTableFields {
    position: u64, // keeps track of the highest inode currently assigned.
    merged_path_to_inode: BTreeMap<OsString, u64>,
    inode_to_merged_path: BTreeMap<u64, OsString>,
}

impl<'a> INodeTable {
    pub fn new() -> Self {
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

    pub fn lookup_inode_from_merged_path(&self, str: &OsString) -> u64 {
        if let Some(i) = self.lookup_inode_from_merged_path_cache_safe(str) {
            return i;
        }

        self.add_inode_for_merged_path_safe(str.to_os_string())
    }

    pub fn lookup_merged_path_from_inode(&self, ino: u64) -> Option<OsString> {
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
        let mut fields = self.fields.write().unwrap();

        // check whether we've had something inserted while we were waiting for the lock.
        if let Some(i) = fields.merged_path_to_inode.get(&str) {
            // yup, just return it.
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
        return position_idx;
    }
}
