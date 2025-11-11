// B+ Tree Insert 测试
use crate::ix::bplustree::BPTree;
use crate::ix::node::BPTreeNode;

pub fn run_insert_test() {
    println!("\n========== B+ Tree Insert Test ==========\n");
    
    // 创建 B+ 树，order=3
    let mut btree = BPTree::new(3);
    
    // 测试1：插入第一个 key（创建根节点）
    println!("Test 1: Insert first key into empty tree");
    let key1 = vec![1, 2, 3];
    let rid1 = (10u32, 1u16);
    
    if let Ok(_) = btree.insert(key1.clone(), rid1) {
        println!("✓ Successfully inserted first key");
        println!("  Root page_id: {}", btree.root);
    } else {
        println!("✗ Failed to insert first key");
        return;
    }
    
    // 测试2：插入第二个 key（不分裂）
    println!("\nTest 2: Insert second key (no split)");
    let key2 = vec![5, 6, 7];
    let rid2 = (20u32, 2u16);
    
    if let Ok(_) = btree.insert(key2.clone(), rid2) {
        println!("✓ Successfully inserted second key");
    } else {
        println!("✗ Failed to insert second key");
        return;
    }
    
    // 测试3：插入第三个 key（不分裂，order=3 时可以有 3 个 key）
    println!("\nTest 3: Insert third key (no split, max keys = order)");
    let key3 = vec![3, 4, 5];
    let rid3 = (30u32, 3u16);
    
    if let Ok(_) = btree.insert(key3.clone(), rid3) {
        println!("✓ Successfully inserted third key");
    } else {
        println!("✗ Failed to insert third key");
        return;
    }
    
    // 测试4：插入第四个 key（应该触发分裂）
    println!("\nTest 4: Insert fourth key (should trigger split)");
    let key4 = vec![2, 3, 4];
    let rid4 = (40u32, 4u16);
    
    if let Ok(_) = btree.insert(key4.clone(), rid4) {
        println!("✓ Successfully inserted fourth key (with split)");
        println!("  Root page_id: {}", btree.root);
    } else {
        println!("✗ Failed to insert fourth key");
        return;
    }
    
    // 测试5：再插入几个 key
    println!("\nTest 5: Insert additional keys");
    let keys = vec![
        (vec![10, 11, 12], (50u32, 5u16)),
        (vec![0, 1, 2], (60u32, 6u16)),
        (vec![7, 8, 9], (70u32, 7u16)),
    ];
    
    for (key, rid) in keys {
        if let Ok(_) = btree.insert(key.clone(), rid) {
            println!("✓ Inserted key: {:?}, rid: {:?}", key, rid);
        } else {
            println!("✗ Failed to insert key: {:?}", key);
            return;
        }
    }
    
    println!("\n========== Test Complete ==========");
    println!("Final root page_id: {}", btree.root);
}
