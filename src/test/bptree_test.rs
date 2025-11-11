// 快速验证 BPTreeNode serialize/deserialize 的测试程序
use std::io::{Read, Write, Cursor};

// 从 src/common/types.rs 复制的定义
pub type PageId = u32;

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
    pub fn serialize(&self) -> Vec<u8> {
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
        if data.len() < 9 {
            return Err("Data too short for BPTreeNode header".to_string());
        }
        
        let mut cursor = Cursor::new(data);
        
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

fn main() {
    println!("=== BPTreeNode Serialize/Deserialize Test ===\n");
    
    // 测试 1: 叶子节点
    println!("Test 1: Leaf Node");
    let mut leaf_node = BPTreeNode::new(1, true);
    leaf_node.keys = vec![
        vec![0x01, 0x02],
        vec![0x03, 0x04, 0x05],
    ];
    leaf_node.rids = vec![
        (10, 1),
        (20, 2),
    ];
    leaf_node.next_leaf = Some(2);
    
    let serialized_leaf = leaf_node.serialize();
    println!("  Serialized size: {} bytes", serialized_leaf.len());
    
    let deserialized_leaf = BPTreeNode::deserialize(&serialized_leaf).unwrap();
    println!("  is_leaf: {}", deserialized_leaf.is_leaf);
    println!("  key_count: {}", deserialized_leaf.keys.len());
    println!("  keys: {:?}", deserialized_leaf.keys);
    println!("  rids: {:?}", deserialized_leaf.rids);
    println!("  next_leaf: {:?}", deserialized_leaf.next_leaf);
    
    assert_eq!(deserialized_leaf.is_leaf, true);
    assert_eq!(deserialized_leaf.keys.len(), 2);
    assert_eq!(deserialized_leaf.rids[0], (10, 1));
    assert_eq!(deserialized_leaf.rids[1], (20, 2));
    assert_eq!(deserialized_leaf.next_leaf, Some(2));
    println!("  ✓ PASSED\n");
    
    // 测试 2: 内部节点
    println!("Test 2: Internal Node");
    let mut internal_node = BPTreeNode::new(5, false);
    internal_node.keys = vec![
        vec![0x10, 0x20],
        vec![0x30, 0x40],
    ];
    internal_node.children = vec![1, 2, 3];
    internal_node.next_leaf = None;
    
    let serialized_internal = internal_node.serialize();
    println!("  Serialized size: {} bytes", serialized_internal.len());
    
    let deserialized_internal = BPTreeNode::deserialize(&serialized_internal).unwrap();
    println!("  is_leaf: {}", deserialized_internal.is_leaf);
    println!("  key_count: {}", deserialized_internal.keys.len());
    println!("  keys: {:?}", deserialized_internal.keys);
    println!("  children: {:?}", deserialized_internal.children);
    println!("  next_leaf: {:?}", deserialized_internal.next_leaf);
    
    assert_eq!(deserialized_internal.is_leaf, false);
    assert_eq!(deserialized_internal.keys.len(), 2);
    assert_eq!(deserialized_internal.children, vec![1, 2, 3]);
    assert_eq!(deserialized_internal.next_leaf, None);
    println!("  ✓ PASSED\n");
    
    // 测试 3: 空节点
    println!("Test 3: Empty Node");
    let empty_node = BPTreeNode::new(100, true);
    
    let serialized_empty = empty_node.serialize();
    println!("  Serialized size: {} bytes", serialized_empty.len());
    
    let deserialized_empty = BPTreeNode::deserialize(&serialized_empty).unwrap();
    println!("  is_leaf: {}", deserialized_empty.is_leaf);
    println!("  key_count: {}", deserialized_empty.keys.len());
    
    assert_eq!(deserialized_empty.is_leaf, true);
    assert_eq!(deserialized_empty.keys.len(), 0);
    println!("  ✓ PASSED\n");
    
    // 测试 4: 单 key 内部节点
    println!("Test 4: Single Key Internal Node");
    let mut single_key_node = BPTreeNode::new(10, false);
    single_key_node.keys = vec![vec![0xFF]];
    single_key_node.children = vec![100, 200];
    
    let serialized_single = single_key_node.serialize();
    println!("  Serialized size: {} bytes", serialized_single.len());
    
    let deserialized_single = BPTreeNode::deserialize(&serialized_single).unwrap();
    assert_eq!(deserialized_single.keys.len(), 1);
    assert_eq!(deserialized_single.keys[0], vec![0xFF]);
    assert_eq!(deserialized_single.children, vec![100, 200]);
    println!("  ✓ PASSED\n");
    
    println!("=== All Tests Passed! ===");
}
