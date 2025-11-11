#[derive(Debug)]
pub struct IndexMeta {
    pub table: String,
    pub index_no: usize,
    pub attr_len: usize,
    pub root_page: u32,
}

pub struct CatalogManager {
    pub indexes: Vec<IndexMeta>,
}

impl CatalogManager {
    pub fn new() -> Self {
        Self { indexes: vec![] }
    }

    pub fn register_index(&mut self, meta: IndexMeta) {
        self.indexes.push(meta);
    }

    pub fn get_index(&self, table: &str, index_no: usize) -> Option<&IndexMeta> {
        self.indexes.iter().find(|i| i.table == table && i.index_no == index_no)
    }
}
