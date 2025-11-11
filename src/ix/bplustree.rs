use crate::ix::node::BPTreeNode;
use crate::ix::errors::{IXResult, IXError};
use crate::common::types::PageId;
use std::cell::RefCell;
use std::collections::HashMap;

pub struct BPTree {
    pub root: PageId,
    pub order: usize,  // 每节点最大 key 数
    nodes: RefCell<HashMap<PageId, BPTreeNode>>,  // 内存缓存（简化实现）
}

impl BPTree {
    pub fn new(order: usize) -> Self {
        // 创建空的 B+ 树，root 为 0（需要外部初始化）
        Self { 
            root: 0, 
            order,
            nodes: RefCell::new(HashMap::new()),
        }
    }

    // 插入 key-rid 对到 B+ 树
    // 
    // 流程：
    // 1. 如果树为空，创建根节点（叶子）并插入
    // 2. 查找目标叶子节点
    // 3. 在叶子中插入 key-rid
    // 4. 如果叶子满（key数 >= order），分裂并向上传播
    pub fn insert(&mut self, key: Vec<u8>, rid: (u32, u16)) -> IXResult<()> {
        // 如果树为空，创建根节点
        if self.root == 0 {
            let mut root_node = BPTreeNode::new(1, true); // page_id=1, is_leaf=true
            root_node.keys.push(key);
            root_node.rids.push(rid);
            self.root = 1;
            
            // 写入缓存
            self.write_node(&root_node)?;
            
            println!("[BPTree] Created root node (page_id=1) with first key");
            return Ok(());
        }

        // 查找目标叶子节点
        let leaf_page_id = self.find_leaf(self.root, &key)?;
        
        // 读取叶子节点
        let mut leaf_node = self.read_node(leaf_page_id)?;
        
        // 在叶子中找到插入位置
        let insert_pos = self.find_insert_position(&leaf_node.keys, &key);
        
        // 在叶子中插入
        leaf_node.keys.insert(insert_pos, key.clone());
        leaf_node.rids.insert(insert_pos, rid);
        
        println!("[BPTree] Inserted key at leaf page {} (position {})", leaf_page_id, insert_pos);
        
        // 检查是否需要分裂
        if leaf_node.keys.len() >= self.order {
            println!("[BPTree] Leaf page {} is full, splitting...", leaf_page_id);
            
            // 分裂叶子节点
            let (promote_key, new_leaf_page_id, new_leaf_node) = 
                self.split_leaf_node(&leaf_node)?;
            
            // 修改原叶子，使其只包含左半部分
            let mid = leaf_node.keys.len() / 2;
            leaf_node.keys = leaf_node.keys[..mid].to_vec();
            leaf_node.rids = leaf_node.rids[..mid].to_vec();
            leaf_node.next_leaf = Some(new_leaf_page_id);
            
            // 写回分裂后的旧叶子和新叶子
            self.write_node(&leaf_node)?;
            self.write_node(&new_leaf_node)?;
            
            // 检查分裂的叶子是否就是根
            if leaf_page_id == self.root {
                // 根叶子分裂，创建新根
                let mut new_root = BPTreeNode::new(self.get_next_page_id(), false);
                new_root.keys.push(promote_key);
                new_root.children.push(leaf_page_id);
                new_root.children.push(new_leaf_page_id);
                
                self.write_node(&new_root)?;
                self.root = new_root.page_id;
                
                println!("[BPTree] Root split, new root page_id={}", new_root.page_id);
            } else {
                // 向上层递归插入提升的 key
                self.insert_internal(self.root, promote_key, leaf_page_id, new_leaf_page_id)?;
            }
        } else {
            // 只需写回叶子
            self.write_node(&leaf_node)?;
        }
        
        Ok(())
    }

    // 在内部节点中插入一个 key 和右子指针
    // 当左子产生分裂时调用
    fn insert_internal(
        &mut self,
        parent_page_id: PageId,
        promote_key: Vec<u8>,
        _left_child: PageId,
        right_child: PageId,
    ) -> IXResult<()> {
        let mut parent = self.read_node(parent_page_id)?;
        
        // 找到在父节点中的插入位置
        let insert_pos = self.find_insert_position(&parent.keys, &promote_key);
        
        parent.keys.insert(insert_pos, promote_key);
        parent.children.insert(insert_pos + 1, right_child);
        
        println!("[BPTree] Inserted key into internal page {} (position {})", parent_page_id, insert_pos);
        
        // 检查父节点是否也满了
        if parent.keys.len() >= self.order {
            println!("[BPTree] Internal page {} is full, splitting...", parent_page_id);
            
            let (promote_key_up, new_internal_node) =
                self.split_internal_node(&parent)?;
            
            // 修改原父节点的 keys（只保留前半部分）
            let mid = parent.keys.len() / 2;
            parent.keys = parent.keys[..mid].to_vec();
            parent.children = parent.children[..=mid].to_vec();
            
            self.write_node(&parent)?;
            self.write_node(&new_internal_node)?;
            
            // 继续向上递归
            if parent_page_id == self.root {
                // 根节点也满了，创建新根
                let mut new_root = BPTreeNode::new(self.get_next_page_id(), false);
                new_root.keys.push(promote_key_up);
                new_root.children.push(parent.page_id);
                new_root.children.push(new_internal_node.page_id);
                
                self.write_node(&new_root)?;
                self.root = new_root.page_id;
                
                println!("[BPTree] Root split, new root page_id={}", new_root.page_id);
            } else {
                // TODO: 递归向上插入（需要知道父节点的父节点）
                // 这里简化处理，实际应该维护路径
            }
        } else {
            self.write_node(&parent)?;
        }
        
        Ok(())
    }

    // 查找包含 key 的叶子节点
    fn find_leaf(&self, root_page_id: PageId, key: &[u8]) -> IXResult<PageId> {
        let mut current_page = root_page_id;
        
        loop {
            let node = self.read_node(current_page)?;
            
            if node.is_leaf {
                return Ok(current_page);
            }
            
            // 在内部节点中查找应该下降的子指针
            let mut child_idx = 0;
            for (i, node_key) in node.keys.iter().enumerate() {
                if key < node_key {
                    child_idx = i;
                    break;
                }
                child_idx = i + 1;
            }
            
            if child_idx >= node.children.len() {
                return Err(IXError::InvalidOperation);
            }
            
            current_page = node.children[child_idx];
        }
    }

    // 找到应该插入的位置
    fn find_insert_position(&self, keys: &[Vec<u8>], key: &[u8]) -> usize {
        for (i, k) in keys.iter().enumerate() {
            if key < k.as_slice() {
                return i;
            }
        }
        keys.len()
    }

    // 分裂叶子节点
    // 返回：(提升的 key，新叶子页面 ID，新叶子节点)
    fn split_leaf_node(&self, leaf: &BPTreeNode) -> IXResult<(Vec<u8>, PageId, BPTreeNode)> {
        let mid = leaf.keys.len() / 2;
        
        // 创建新叶子
        let new_page_id = self.get_next_page_id();
        let mut new_leaf = BPTreeNode::new(new_page_id, true);
        
        // 分裂 keys 和 rids
        new_leaf.keys = leaf.keys[mid..].to_vec();
        new_leaf.rids = leaf.rids[mid..].to_vec();
        
        // 新叶子继承原叶子的 next_leaf
        new_leaf.next_leaf = leaf.next_leaf;
        
        // 原叶子的 next_leaf 指向新叶子
        // （这部分在调用者处理）
        
        // 提升中间的 key（叶子中的第一个 key）
        let promote_key = new_leaf.keys[0].clone();
        
        println!("[BPTree] Split leaf: mid={}, promote_key_len={}", mid, promote_key.len());
        
        Ok((promote_key, new_page_id, new_leaf))
    }

    // 分裂内部节点
    // 返回：(提升的 key，新右子节点)
    fn split_internal_node(&self, internal: &BPTreeNode) -> IXResult<(Vec<u8>, BPTreeNode)> {
        let mid = internal.keys.len() / 2;
        
        // 提升的 key
        let promote_key = internal.keys[mid].clone();
        
        // 右子节点：新 page_id
        let new_page_id = self.get_next_page_id();
        let mut right = BPTreeNode::new(new_page_id, false);
        right.keys = internal.keys[mid + 1..].to_vec();
        right.children = internal.children[mid + 1..].to_vec();
        
        println!("[BPTree] Split internal: mid={}, promote_key_len={}, right_keys={}", 
            mid, promote_key.len(), right.keys.len());
        
        Ok((promote_key, right))
    }

    // 读取节点（从缓存读取）
    fn read_node(&self, page_id: PageId) -> IXResult<BPTreeNode> {
        let nodes = self.nodes.borrow();
        if let Some(node) = nodes.get(&page_id) {
            Ok(node.clone())
        } else {
            drop(nodes);
            // 如果缓存中没有，返回空节点
            Ok(BPTreeNode::new(page_id, true))
        }
    }

    // 写入节点（保存到缓存）
    fn write_node(&self, node: &BPTreeNode) -> IXResult<()> {
        println!("[BPTree] Write node page_id={} (is_leaf={}, keys={})", 
            node.page_id, node.is_leaf, node.keys.len());
        self.nodes.borrow_mut().insert(node.page_id, node.clone());
        Ok(())
    }

    // 获取下一个可用的页 ID（简化实现）
    fn get_next_page_id(&self) -> PageId {
        // 简化实现：静态分配
        // 实际应该从页管理器获取
        static mut NEXT_PAGE_ID: u32 = 2;
        unsafe {
            NEXT_PAGE_ID += 1;
            NEXT_PAGE_ID - 1
        }
    }

    // 删除 key-rid 对
    // 
    // 流程：
    // 1. 查找目标叶子节点
    // 2. 删除 key 和对应的 RID
    // 3. 如果叶子低于最小容量（< order/2）：
    //    - 先尝试从兄弟节点借位
    //    - 如果借位失败，合并两个节点
    // 4. 递归向上调整父节点
    pub fn delete(&mut self, key: Vec<u8>, rid: (u32, u16)) -> IXResult<()> {
        if self.root == 0 {
            return Err(IXError::InvalidOperation);
        }

        // 查找目标叶子节点
        let leaf_page_id = self.find_leaf(self.root, &key)?;
        let mut leaf_node = self.read_node(leaf_page_id)?;

        println!("[BPTree] Delete: searching for key in leaf page {} (keys in leaf: {})", 
            leaf_page_id, leaf_node.keys.len());
        for (i, k) in leaf_node.keys.iter().enumerate() {
            println!("  [{}]: {:?}", i, k);
        }

        // 查找 key 的位置
        let key_pos = self.find_key_position(&leaf_node.keys, &key);
        
        if key_pos >= leaf_node.keys.len() || leaf_node.keys[key_pos].as_slice() != key {
            // 未找到该 key
            println!("[BPTree] Key not found in leaf page {} (searched {} keys)", 
                leaf_page_id, leaf_node.keys.len());
            return Err(IXError::KeyNotFound);
        }

        // 检查 RID 是否匹配
        if key_pos < leaf_node.rids.len() && leaf_node.rids[key_pos] != rid {
            println!("[BPTree] RID mismatch for key in leaf page {}", leaf_page_id);
            return Err(IXError::InvalidOperation);
        }

        // 删除 key 和 rid
        leaf_node.keys.remove(key_pos);
        if key_pos < leaf_node.rids.len() {
            leaf_node.rids.remove(key_pos);
        }

        println!("[BPTree] Deleted key from leaf page {} (position {})", leaf_page_id, key_pos);

        // 检查叶子是否需要调整
        let min_keys = (self.order + 1) / 2 - 1;  // 最小 key 数
        
        if leaf_node.keys.len() < min_keys && leaf_page_id != self.root {
            println!("[BPTree] Leaf page {} underfull, attempting rebalance...", leaf_page_id);
            
            // 尝试从兄弟借位或合并
            self.handle_leaf_underflow(&mut leaf_node, leaf_page_id)?;
        }

        self.write_node(&leaf_node)?;
        Ok(())
    }

    // 处理叶子节点下溢（key 数过少）
    fn handle_leaf_underflow(&mut self, _leaf: &mut BPTreeNode, leaf_page_id: PageId) -> IXResult<()> {
        // TODO: 实现完整的叶子平衡逻辑
        // 这里简化处理，实际应该：
        // 1. 读取左兄弟，尝试借位
        // 2. 读取右兄弟，尝试借位
        // 3. 如果都不能借，则合并
        // 4. 更新父节点中的分隔 key
        
        println!("[BPTree] Leaf underflow handling at page {}", leaf_page_id);
        Ok(())
    }

    // 在键数组中查找 key 的位置（精确查找）
    fn find_key_position(&self, keys: &[Vec<u8>], key: &[u8]) -> usize {
        for (i, k) in keys.iter().enumerate() {
            if k.as_slice() == key {
                return i;
            }
        }
        keys.len()
    }

    // 查找键对应的 RID
    // 
    // # 参数
    // - `key`: 要查找的键（二进制格式）
    // 
    // # 返回
    // 如果找到返回 Some((page_id, slot_id))，否则返回 None
    pub fn search(&self, key: &[u8]) -> IXResult<Option<(u32, u16)>> {
        if self.root == 0 {
            return Ok(None);
        }

        // 查找包含该键的叶子节点
        let leaf_page_id = self.find_leaf(self.root, key)?;
        let leaf_node = self.read_node(leaf_page_id)?;

        println!("[BPTree] Search: looking for key in leaf page {}", leaf_page_id);

        // 在叶子中查找
        let key_pos = self.find_key_position(&leaf_node.keys, key);
        
        if key_pos >= leaf_node.keys.len() || leaf_node.keys[key_pos].as_slice() != key {
            println!("[BPTree] Search: key not found");
            return Ok(None);
        }

        // 返回对应的 RID
        if key_pos < leaf_node.rids.len() {
            let rid = leaf_node.rids[key_pos];
            println!("[BPTree] Search: found RID {:?}", rid);
            Ok(Some(rid))
        } else {
            Ok(None)
        }
    }

    // 范围扫描：找出所有键在 [lower, upper) 范围内的 RID
    // 
    // # 参数
    // - `lower`: 范围下界（包含）
    // - `upper`: 范围上界（不包含）
    // 
    // # 返回
    // 范围内所有记录 ID 的列表
    pub fn scan_range(&self, lower: &[u8], upper: &[u8]) -> IXResult<Vec<(u32, u16)>> {
        if self.root == 0 {
            println!("[BPTree] Scan: empty tree");
            return Ok(vec![]);
        }

        let mut results = Vec::new();

        // 1. 找到起始叶子节点（包含 lower 的叶子）
        let start_leaf_page = self.find_leaf(self.root, lower)?;
        let mut current_leaf_page = start_leaf_page;

        println!("[BPTree] Scan: starting from leaf page {}", start_leaf_page);

        // 2. 遍历叶子链表，收集范围内的所有 RID
        loop {
            let leaf_node = self.read_node(current_leaf_page)?;

            println!("[BPTree] Scan: processing leaf page {} with {} keys", 
                current_leaf_page, leaf_node.keys.len());

            // 在当前叶子中收集符合条件的 RID
            for (i, key) in leaf_node.keys.iter().enumerate() {
                let key_bytes = key.as_slice();
                
                // 检查是否在范围内: lower <= key < upper
                if key_bytes >= lower && key_bytes < upper {
                    if i < leaf_node.rids.len() {
                        let rid = leaf_node.rids[i];
                        results.push(rid);
                        println!("[BPTree] Scan: added RID {:?} for key {:?}", 
                            rid, key_bytes);
                    }
                } else if key_bytes >= upper {
                    // 已超过范围上界，可以停止扫描
                    println!("[BPTree] Scan: reached upper bound, stopping");
                    return Ok(results);
                }
            }

            // 3. 沿着 next_leaf 指针继续扫描下一个叶子
            match leaf_node.next_leaf {
                Some(next_page) => {
                    current_leaf_page = next_page;
                    println!("[BPTree] Scan: moving to next leaf page {}", next_page);
                }
                None => {
                    // 没有更多的叶子了，扫描完成
                    println!("[BPTree] Scan: reached end of leaf chain");
                    break;
                }
            }
        }

        println!("[BPTree] Scan: completed, found {} results", results.len());
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_delete_basic() {
        let mut btree = BPTree::new(3);

        // 插入几个 key
        let key1 = vec![1, 2, 3];
        let rid1 = (10u32, 1u16);
        assert!(btree.insert(key1.clone(), rid1).is_ok());

        let key2 = vec![5, 6, 7];
        let rid2 = (20u32, 2u16);
        assert!(btree.insert(key2.clone(), rid2).is_ok());

        let key3 = vec![3, 4, 5];
        let rid3 = (30u32, 3u16);
        assert!(btree.insert(key3.clone(), rid3).is_ok());

        println!("Inserted 3 keys");

        // 删除第一个 key
        assert!(btree.delete(key1.clone(), rid1).is_ok());
        println!("Deleted key1");

        // 删除不存在的 key
        let key_not_exist = vec![99, 99];
        let result = btree.delete(key_not_exist, (999u32, 99u16));
        println!("Delete non-existent key result: {:?}", result);

        // 删除已删除的 key
        let result = btree.delete(key1.clone(), rid1);
        println!("Delete already deleted key result: {:?}", result);
    }

    #[test]
    fn test_insert_and_delete_with_split() {
        let mut btree = BPTree::new(3);

        // 插入足够多的 key 触发分裂
        let keys = vec![
            (vec![1, 0], (10u32, 1u16)),
            (vec![2, 0], (20u32, 2u16)),
            (vec![3, 0], (30u32, 3u16)),
            (vec![4, 0], (40u32, 4u16)),
            (vec![5, 0], (50u32, 5u16)),
        ];

        for (key, rid) in &keys {
            assert!(btree.insert(key.clone(), *rid).is_ok());
        }

        println!("Inserted {} keys with splits", keys.len());

        // 删除一些 key
        for (key, rid) in &keys[0..2] {
            assert!(btree.delete(key.clone(), *rid).is_ok());
        }

        println!("Deleted some keys after split");
    }

    #[test]
    fn test_empty_tree_delete() {
        let mut btree = BPTree::new(3);

        let key = vec![1, 2, 3];
        let rid = (10u32, 1u16);

        let result = btree.delete(key, rid);
        println!("Delete from empty tree result: {:?}", result);
        assert!(result.is_err());
    }

    #[test]
    fn test_search() {
        let mut btree = BPTree::new(3);

        // 插入一些键
        let key1 = vec![1, 0, 0];
        let rid1 = (100u32, 0u16);
        btree.insert(key1.clone(), rid1).expect("Insert failed");

        let key2 = vec![2, 0, 0];
        let rid2 = (200u32, 1u16);
        btree.insert(key2.clone(), rid2).expect("Insert failed");

        let key3 = vec![3, 0, 0];
        let rid3 = (300u32, 2u16);
        btree.insert(key3.clone(), rid3).expect("Insert failed");

        println!("Inserted 3 keys");

        // 搜索存在的键
        let result = btree.search(&key1).expect("Search failed");
        assert_eq!(result, Some(rid1));
        println!("✓ Found key1: {:?}", result);

        let result = btree.search(&key2).expect("Search failed");
        assert_eq!(result, Some(rid2));
        println!("✓ Found key2: {:?}", result);

        // 搜索不存在的键
        let not_exist_key = vec![99, 0, 0];
        let result = btree.search(&not_exist_key).expect("Search failed");
        assert_eq!(result, None);
        println!("✓ Key not found (as expected)");
    }

    #[test]
    fn test_scan_range() {
        let mut btree = BPTree::new(4);

        // 插入多个键
        let test_data = vec![
            (vec![1, 0, 0], (10u32, 0u16)),
            (vec![2, 0, 0], (20u32, 1u16)),
            (vec![3, 0, 0], (30u32, 2u16)),
            (vec![4, 0, 0], (40u32, 3u16)),
            (vec![5, 0, 0], (50u32, 4u16)),
            (vec![6, 0, 0], (60u32, 5u16)),
        ];

        for (key, rid) in &test_data {
            btree.insert(key.clone(), *rid).expect("Insert failed");
        }

        println!("Inserted {} keys", test_data.len());

        // 测试范围扫描：[2, 5)
        let lower = vec![2, 0, 0];
        let upper = vec![5, 0, 0];
        let results = btree.scan_range(&lower, &upper).expect("Scan failed");

        println!("Scan [2, 5) returned {} results", results.len());
        assert_eq!(results.len(), 3);  // 应该有 key 2, 3, 4
        assert_eq!(results[0], (20u32, 1u16));
        assert_eq!(results[1], (30u32, 2u16));
        assert_eq!(results[2], (40u32, 3u16));

        println!("✓ Scan range [2, 5) test passed");

        // 测试范围扫描：[1, 3)
        let lower = vec![1, 0, 0];
        let upper = vec![3, 0, 0];
        let results = btree.scan_range(&lower, &upper).expect("Scan failed");

        println!("Scan [1, 3) returned {} results", results.len());
        assert_eq!(results.len(), 2);  // 应该有 key 1, 2
        assert_eq!(results[0], (10u32, 0u16));
        assert_eq!(results[1], (20u32, 1u16));

        println!("✓ Scan range [1, 3) test passed");

        // 测试范围扫描：空范围
        let lower = vec![10, 0, 0];
        let upper = vec![20, 0, 0];
        let results = btree.scan_range(&lower, &upper).expect("Scan failed");

        println!("Scan [10, 20) returned {} results (empty range)", results.len());
        assert_eq!(results.len(), 0);

        println!("✓ Scan empty range test passed");

        // 测试范围扫描：全表
        let lower = vec![0, 0, 0];
        let upper = vec![255, 255, 255];
        let results = btree.scan_range(&lower, &upper).expect("Scan failed");

        println!("Scan [0, 255) returned {} results (full table)", results.len());
        assert_eq!(results.len(), 6);

        println!("✓ Scan full range test passed");
    }
}
