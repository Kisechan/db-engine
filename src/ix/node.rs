use crate::common::types::PageId;

#[derive(Debug, Clone)]
pub struct BPTreeNode {
    pub page_id: PageId,
    pub is_leaf: bool,
    pub keys: Vec<Vec<u8>>,      // key 二进制存储
    pub rids: Vec<(u32, u16)>,   // 叶子节点存 RID (page, slot)
    pub children: Vec<PageId>,   // 内部节点子指针
    pub next_leaf: Option<PageId>,
}

impl BPTreeNode {
    pub fn new(page_id: PageId, is_leaf: bool) -> Self {
        Self {
            page_id,
            is_leaf,
            keys: vec![],
            rids: vec![],
            children: vec![],
            next_leaf: None,
        }
    }

    // 序列化 BPTreeNode 到字节数组
    // 
    // 格式（字节偏移）：
    // [0] is_leaf (1字节，bool)
    // [1-4] key_count (4字节，u32)
    // [5-8] next_leaf (4字节，u32，Option<PageId>，0表示None)
    // [9+] 数据部分（变长）
    //   对于叶子节点：
    //     - 每个 key 存储：key_len(2字节) + key_data(变长) + rid(8字节=4+4)
    //   对于内部节点：
    //     - 每个 key 存储：key_len(2字节) + key_data(变长)
    //     - children: key_count+1 个 PageId，每个4字节
    pub fn serialize(&self) -> Vec<u8> {
        use std::io::Write;
        
        let mut buf = Vec::new();
        
        // [0] is_leaf
        buf.write_all(&[if self.is_leaf { 1 } else { 0 }]).unwrap();
        
        // [1-4] key_count
        let key_count = self.keys.len() as u32;
        buf.write_all(&key_count.to_le_bytes()).unwrap();
        
        // [5-8] next_leaf
        let next_leaf_val = self.next_leaf.unwrap_or(0);
        buf.write_all(&next_leaf_val.to_le_bytes()).unwrap();
        
        // [9+] 数据部分
        if self.is_leaf {
            // 叶子节点：key + rid
            for i in 0..key_count as usize {
                // key_len (2字节)
                let key_len = self.keys[i].len() as u16;
                buf.write_all(&key_len.to_le_bytes()).unwrap();
                
                // key_data (变长)
                buf.write_all(&self.keys[i]).unwrap();
                
                // rid (8字节: page_id 4字节 + slot_id 4字节)
                let (page, slot) = self.rids[i];
                buf.write_all(&page.to_le_bytes()).unwrap();
                buf.write_all(&(slot as u32).to_le_bytes()).unwrap();
            }
        } else {
            // 内部节点：key + children
            for i in 0..key_count as usize {
                // key_len (2字节)
                let key_len = self.keys[i].len() as u16;
                buf.write_all(&key_len.to_le_bytes()).unwrap();
                
                // key_data (变长)
                buf.write_all(&self.keys[i]).unwrap();
            }
            
            // children: key_count+1 个 PageId
            for child_page in &self.children {
                buf.write_all(&child_page.to_le_bytes()).unwrap();
            }
        }
        
        buf
    }

    // 反序列化字节数组到 BPTreeNode
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        use std::io::Read;
        
        if data.len() < 9 {
            return Err("Data too short for BPTreeNode header".to_string());
        }
        
        let mut cursor = std::io::Cursor::new(data);
        
        // [0] is_leaf
        let mut is_leaf_byte = [0u8; 1];
        cursor.read_exact(&mut is_leaf_byte)
            .map_err(|e| format!("Failed to read is_leaf: {}", e))?;
        let is_leaf = is_leaf_byte[0] != 0;
        
        // [1-4] key_count
        let mut key_count_bytes = [0u8; 4];
        cursor.read_exact(&mut key_count_bytes)
            .map_err(|e| format!("Failed to read key_count: {}", e))?;
        let key_count = u32::from_le_bytes(key_count_bytes) as usize;
        
        // [5-8] next_leaf
        let mut next_leaf_bytes = [0u8; 4];
        cursor.read_exact(&mut next_leaf_bytes)
            .map_err(|e| format!("Failed to read next_leaf: {}", e))?;
        let next_leaf_val = u32::from_le_bytes(next_leaf_bytes);
        let next_leaf = if next_leaf_val == 0 { None } else { Some(next_leaf_val) };
        
        let mut keys = Vec::new();
        let mut rids = Vec::new();
        let mut children = Vec::new();
        
        if is_leaf {
            // 叶子节点：读取 key + rid
            for _ in 0..key_count {
                // key_len (2字节)
                let mut key_len_bytes = [0u8; 2];
                cursor.read_exact(&mut key_len_bytes)
                    .map_err(|e| format!("Failed to read key_len: {}", e))?;
                let key_len = u16::from_le_bytes(key_len_bytes) as usize;
                
                // key_data (变长)
                let mut key_data = vec![0u8; key_len];
                cursor.read_exact(&mut key_data)
                    .map_err(|e| format!("Failed to read key_data: {}", e))?;
                keys.push(key_data);
                
                // rid (8字节: page_id 4字节 + slot_id 4字节)
                let mut page_bytes = [0u8; 4];
                cursor.read_exact(&mut page_bytes)
                    .map_err(|e| format!("Failed to read rid page: {}", e))?;
                let page = u32::from_le_bytes(page_bytes);
                
                let mut slot_bytes = [0u8; 4];
                cursor.read_exact(&mut slot_bytes)
                    .map_err(|e| format!("Failed to read rid slot: {}", e))?;
                let slot = u32::from_le_bytes(slot_bytes) as u16;
                
                rids.push((page, slot));
            }
        } else {
            // 内部节点：读取 key + children
            for _ in 0..key_count {
                // key_len (2字节)
                let mut key_len_bytes = [0u8; 2];
                cursor.read_exact(&mut key_len_bytes)
                    .map_err(|e| format!("Failed to read key_len: {}", e))?;
                let key_len = u16::from_le_bytes(key_len_bytes) as usize;
                
                // key_data (变长)
                let mut key_data = vec![0u8; key_len];
                cursor.read_exact(&mut key_data)
                    .map_err(|e| format!("Failed to read key_data: {}", e))?;
                keys.push(key_data);
            }
            
            // children: key_count+1 个 PageId
            for _ in 0..=key_count {
                let mut child_bytes = [0u8; 4];
                cursor.read_exact(&mut child_bytes)
                    .map_err(|e| format!("Failed to read child page: {}", e))?;
                let child_page = u32::from_le_bytes(child_bytes);
                children.push(child_page);
            }
        }
        
        // page_id 从外部传入时获取，这里暂时设为 0，应该由调用者设置
        Ok(Self {
            page_id: 0,
            is_leaf,
            keys,
            rids,
            children,
            next_leaf,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_serialize_deserialize() {
        // 创建叶子节点
        let mut node = BPTreeNode::new(1, true);
        node.keys = vec![
            vec![0x01, 0x02],
            vec![0x03, 0x04, 0x05],
        ];
        node.rids = vec![
            (10, 1),
            (20, 2),
        ];
        node.next_leaf = Some(2);
        
        // 序列化
        let serialized = node.serialize();
        println!("Serialized leaf node: {:?} (size: {})", serialized, serialized.len());
        
        // 反序列化
        let deserialized = BPTreeNode::deserialize(&serialized)
            .expect("Failed to deserialize leaf node");
        
        // 验证
        assert_eq!(deserialized.is_leaf, true);
        assert_eq!(deserialized.keys.len(), 2);
        assert_eq!(deserialized.keys[0], vec![0x01, 0x02]);
        assert_eq!(deserialized.keys[1], vec![0x03, 0x04, 0x05]);
        assert_eq!(deserialized.rids[0], (10, 1));
        assert_eq!(deserialized.rids[1], (20, 2));
        assert_eq!(deserialized.next_leaf, Some(2));
        
        println!("✓ Leaf node serialize/deserialize test passed");
    }

    #[test]
    fn test_internal_node_serialize_deserialize() {
        // 创建内部节点
        let mut node = BPTreeNode::new(5, false);
        node.keys = vec![
            vec![0x10, 0x20],
            vec![0x30, 0x40],
        ];
        node.children = vec![1, 2, 3];  // key_count + 1 = 3
        node.next_leaf = None;
        
        // 序列化
        let serialized = node.serialize();
        println!("Serialized internal node: {:?} (size: {})", serialized, serialized.len());
        
        // 反序列化
        let deserialized = BPTreeNode::deserialize(&serialized)
            .expect("Failed to deserialize internal node");
        
        // 验证
        assert_eq!(deserialized.is_leaf, false);
        assert_eq!(deserialized.keys.len(), 2);
        assert_eq!(deserialized.keys[0], vec![0x10, 0x20]);
        assert_eq!(deserialized.keys[1], vec![0x30, 0x40]);
        assert_eq!(deserialized.children.len(), 3);
        assert_eq!(deserialized.children, vec![1, 2, 3]);
        assert_eq!(deserialized.next_leaf, None);
        
        println!("✓ Internal node serialize/deserialize test passed");
    }

    #[test]
    fn test_empty_node_serialize_deserialize() {
        // 创建空的叶子节点
        let node = BPTreeNode::new(100, true);
        
        // 序列化
        let serialized = node.serialize();
        println!("Serialized empty node: {:?} (size: {})", serialized, serialized.len());
        
        // 反序列化
        let deserialized = BPTreeNode::deserialize(&serialized)
            .expect("Failed to deserialize empty node");
        
        // 验证
        assert_eq!(deserialized.is_leaf, true);
        assert_eq!(deserialized.keys.len(), 0);
        assert_eq!(deserialized.rids.len(), 0);
        assert_eq!(deserialized.next_leaf, None);
        
        println!("✓ Empty node serialize/deserialize test passed");
    }
}
