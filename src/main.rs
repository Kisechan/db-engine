mod fm;
mod mm;
mod rm;
mod pm;
mod ix;
mod common;
mod test;

use ix::bplustree::BPTree;

fn main() -> Result<(), String> {
    println!("========== B+ Tree Insert & Delete Demo ==========\n");
    
    // 创建 B+ 树，order=4
    let mut btree = BPTree::new(4);
    
    println!("--- Phase 1: Insertion with splits ---");
    let insert_keys = vec![
        (vec![10], (100u32, 1u16)),
        (vec![20], (200u32, 2u16)),
        (vec![30], (300u32, 3u16)),
        (vec![40], (400u32, 4u16)),
        (vec![50], (500u32, 5u16)),
        (vec![15], (150u32, 6u16)),
        (vec![25], (250u32, 7u16)),
        (vec![35], (350u32, 8u16)),
    ];
    
    for (key, rid) in &insert_keys {
        match btree.insert(key.clone(), *rid) {
            Ok(_) => println!("✓ Inserted key: {:?} with rid: {:?}", key, rid),
            Err(e) => println!("✗ Failed to insert key: {:?}, error: {:?}", key, e),
        }
    }
    
    println!("\n--- Phase 2: Deletion ---");
    let delete_keys = vec![
        (vec![10], (100u32, 1u16)),
        (vec![30], (300u32, 3u16)),
        (vec![50], (500u32, 5u16)),
    ];
    
    for (key, rid) in &delete_keys {
        match btree.delete(key.clone(), *rid) {
            Ok(_) => println!("✓ Deleted key: {:?}", key),
            Err(e) => println!("✗ Failed to delete key: {:?}, error: {:?}", key, e),
        }
    }
    
    println!("\n--- Phase 3: Delete non-existent key ---");
    let key_not_exist = vec![200u8];
    match btree.delete(key_not_exist.clone(), (999u32, 99u16)) {
        Ok(_) => println!("✓ Deleted key: {:?}", key_not_exist),
        Err(_) => println!("✓ Correctly failed to delete non-existent key: {:?}", key_not_exist),
    }
    
    println!("\n========== Demo Complete ==========");
    Ok(())
}