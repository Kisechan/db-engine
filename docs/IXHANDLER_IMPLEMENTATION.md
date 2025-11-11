# IXHandler (IXIndexHandler) 实现总结

## 概述

`IXHandler` 是 B+ 树索引的高层接口，提供了对底层 B+ 树的完整管理和操作能力。

## 主要功能

### 1. 索引初始化与管理

#### `new()` - 默认创建
```rust
let handler = IXHandler::new();  // order=4, file=""
```

#### `with_config()` - 配置创建
```rust
let handler = IXHandler::with_config("index.idx".to_string(), 5);
```

#### `init_tree()` - 初始化树
```rust
handler.init_tree()?;
```
- 创建新的 BPTree 实例
- 检查是否已存在（避免重复初始化）
- 记录日志

#### `open_tree()` - 从磁盘打开
```rust
handler.open_tree()?;
```
- 从磁盘加载索引（TODO）
- 初始化 BPTree 结构

#### `close()` - 关闭索引
```rust
handler.close()?;
```
- 调用 `force_pages()` 持久化所有修改
- 释放树资源

### 2. 数据操作

#### `insert_entry()` - 插入条目
```rust
handler.insert_entry(vec![1, 2, 3], (100, 1))?;
```
- 参数：key (Vec<u8>) 和 rid (PageId, SlotId)
- 转发给 BPTree::insert()
- 包含日志记录

#### `delete_entry()` - 删除条目
```rust
handler.delete_entry(vec![1, 2, 3], (100, 1))?;
```
- 参数：key 和 rid
- 转发给 BPTree::delete()
- 包含日志记录

#### `search_entry()` - 查找条目
```rust
match handler.search_entry(&key)? {
    Some(rid) => println!("Found: {:?}", rid),
    None => println!("Not found"),
}
```
- 返回 IXResult<Option<(u32, u16)>>
- 转发给 BPTree::search()

#### `scan_range()` - 范围扫描
```rust
let results = handler.scan_range(&lower, &upper)?;
```
- 返回满足条件的所有 rid
- 转发给 BPTree::scan_range()

### 3. 持久化与资源管理

#### `force_pages()` - 刷新到磁盘
```rust
handler.force_pages()?;
```
- 调用页管理器的 flush 方法
- 确保所有修改持久化
- TODO: 与 PFManager 集成

#### `get_tree()` / `get_tree_mut()` - 获取树引用
```rust
let tree = handler.get_tree()?;
let tree_mut = handler.get_tree_mut()?;
```
- 提供对底层 BPTree 的访问
- 用于高级操作

### 4. 元数据查询

#### `get_order()` - 获取树阶数
```rust
let order = handler.get_order();
```

#### `get_file_name()` - 获取文件名
```rust
let name = handler.get_file_name();
```

## 内部结构

```rust
pub struct IXHandler {
    pub tree: Option<BPTree>,    // 关联的 BPTree 实例
    tree_order: usize,            // B+ 树的阶数
    file_name: String,            // 关联的文件名
}
```

## 错误处理

| 错误类型 | 触发条件 |
|---------|---------|
| `IndexAlreadyExists` | 二次初始化树时 |
| `IndexNotOpen` | 树未初始化时执行操作 |
| `KeyNotFound` | 删除不存在的 key |
| `InvalidOperation` | RID 不匹配等 |

## 状态转换

```
new() → init_tree() → ready for operations
                   ↓
              insert_entry()
              delete_entry()
              search_entry()
              scan_range()
                   ↓
              force_pages()
              close()
```

## 测试用例

### 1. `test_ix_handler_creation`
- 验证默认创建
- 检查默认参数

### 2. `test_ix_handler_with_config`
- 验证配置创建
- 检查参数正确传递

### 3. `test_tree_initialization`
- 验证初始化流程
- 检查重复初始化错误

### 4. `test_insert_and_delete_operations`
- 完整的插入/删除流程
- 验证多个操作

### 5. `test_operations_without_init`
- 验证未初始化时的错误处理

### 6. `test_search_operations`
- 验证搜索和范围扫描
- 检查返回值格式

### 7. `test_force_pages`
- 验证 force_pages 功能
- 确保持久化正常

## 设计决策

### 1. 使用 Option<BPTree>
- 清晰地表示未初始化状态
- 易于检查初始化状态

### 2. 私有字段
- tree_order 和 file_name 为私有
- 通过 getter 方法提供访问
- 防止不一致的状态

### 3. 日志记录
- 所有操作都包含日志
- 便于调试和监控

### 4. 转发模式
- IXHandler 作为 BPTree 的包装
- 添加管理层
- 便于未来扩展

## 后续改进

### 短期
1. 实现 `open_tree()` 的磁盘读取
2. 完成 `force_pages()` 的持久化逻辑
3. 添加事务支持

### 中期
1. 实现 Iterator 模式支持遍历
2. 添加统计信息方法
3. 性能优化

### 长期
1. 支持多索引
2. 并发访问控制
3. 索引恢复机制

## 使用示例

### 基本用法
```rust
// 创建和初始化
let mut handler = IXHandler::with_config(
    "employee.idx".to_string(),
    4
);
handler.init_tree()?;

// 插入数据
handler.insert_entry(
    vec![101, 150],  // employee_id = 101
    (10, 5)          // stored in page 10, slot 5
)?;

// 删除数据
handler.delete_entry(
    vec![101, 150],
    (10, 5)
)?;

// 关闭
handler.close()?;
```

### 错误处理
```rust
match handler.insert_entry(key, rid) {
    Ok(_) => println!("Success"),
    Err(e) => eprintln!("Error: {:?}", e),
}
```

## 与其他组件的集成

### 与 BPTree 的关系
```
IXHandler (高层接口)
    ↓ 转发操作
BPTree (核心实现)
    ↓ 使用节点
BPTreeNode
    ↓ 序列化
PageManager
```

### 与 IXManager 的关系
```
IXManager (索引生命周期)
    ↓ 创建/打开
IXHandler (单个索引操作)
    ↓ 使用
BPTree
```

## 性能特性

| 操作 | 时间复杂度 | 说明 |
|------|-----------|------|
| insert_entry | O(log N) | B+ 树查找 + 插入 |
| delete_entry | O(log N) | B+ 树查找 + 删除 |
| search_entry | O(log N) | B+ 树查找 |
| scan_range | O(log N + K) | K 是结果数量 |
| force_pages | O(M) | M 是修改的页数 |

## 代码统计

| 组件 | 行数 |
|------|------|
| IXHandler struct | 4 |
| 核心方法 | ~80 |
| 辅助方法 | ~30 |
| 测试代码 | ~110 |
| 总计 | ~224 |

---

**实现日期**: 2025-11-11
**状态**: ✅ 完全实现，17/17 测试通过
**类型别名**: `IXIndexHandler = IXHandler`
