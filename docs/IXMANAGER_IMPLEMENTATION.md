# IXManager 实现总结

## 概述

`IXManager` 是索引管理器，负责管理数据库中所有的 B+ 树索引的生命周期。它作为应用程序和具体索引处理器 (`IXHandler`) 之间的中间层。

## 主要功能

### 1. 索引生命周期管理

#### 创建索引
```rust
manager.create_index("employee", 0, 4)?;
```
- 参数：表名、索引编号、属性长度
- 自动构造索引文件名：`employee.idx0`
- 创建并初始化 `IXHandler`
- 存储到内部映射中

#### 销毁索引
```rust
manager.destroy_index("employee", 0)?;
```
- 查找并关闭对应索引
- 释放所有资源
- 返回 `IndexNotFound` 如果不存在

#### 打开索引
```rust
manager.open_index("employee", 0)?;
```
- 打开已存在的索引
- 从磁盘加载索引结构
- 支持重复打开（无操作）

#### 关闭索引
```rust
manager.close_index("employee", 0)?;
```
- 关闭单个索引
- 调用 `force_pages()` 持久化
- 从管理器中移除

### 2. 索引查询与访问

#### 获取处理器（可变）
```rust
let handler = manager.get_handler_mut("employee", 0)?;
handler.insert_entry(key, rid)?;
```
- 返回对 `IXHandler` 的可变引用
- 允许修改操作

#### 获取处理器（不可变）
```rust
let handler = manager.get_handler("employee", 0)?;
let index_count = handler.get_order();
```
- 返回对 `IXHandler` 的不可变引用
- 仅允许查询操作

#### 列出所有索引
```rust
let indexes = manager.list_indexes();
for index in indexes {
    println!("Index: {}", index);
}
```
- 返回所有打开索引的名称列表
- 格式：`{table}_{index_no}`

### 3. 批量操作

#### 关闭所有索引
```rust
manager.close_all()?;
```
- 关闭所有打开的索引
- 确保所有修改持久化
- 清空处理器映射

## 内部结构

```rust
pub struct IXManager {
    // (table_name_index_no) -> IXHandler
    handlers: HashMap<String, IXHandler>,
}
```

### 索引键格式
- 使用 `{table}_{index_no}` 作为键
- 例如：`employee_0`、`department_2`

## 操作流程

### 创建新索引的完整流程

```
1. 调用 create_index("employee", 0, 4)
2. 检查是否已存在 (employee_0)
3. 构造文件名：employee.idx0
4. 创建 IXHandler 实例
5. 调用 handler.init_tree()
   ├─ 创建 BPTree 实例
   ├─ 创建根节点
   └─ 初始化树结构
6. 存储到 handlers 映射
7. 返回成功
```

### 使用索引的完整流程

```
1. 调用 get_handler_mut("employee", 0)
2. 从 handlers 映射查找
3. 返回可变引用
4. 调用 handler.insert_entry(key, rid)
   └─ 转发到 BPTree::insert()
5. 数据插入到 B+ 树
```

## 错误处理

| 错误 | 触发条件 | 处理 |
|------|---------|------|
| `IndexAlreadyExists` | 创建已存在的索引 | 拒绝创建 |
| `IndexNotFound` | 关闭/访问不存在的索引 | 返回错误 |
| `IndexNotOpen` | 获取未打开的索引 | 返回错误 |
| 其他 IXError | BPTree 内部错误 | 转发给调用者 |

## 测试用例

### 1. `test_ix_manager_creation`
验证默认创建和空状态

### 2. `test_create_index`
验证索引创建流程

### 3. `test_duplicate_index_creation`
验证重复创建的错误处理

### 4. `test_open_close_index`
验证打开和关闭流程

### 5. `test_get_handler`
验证处理器获取

### 6. `test_multiple_indexes`
验证管理多个索引

### 7. `test_insert_delete_via_handler`
验证通过管理器操作数据

## 与其他组件的关系

```
应用层
    ↓
IXManager (管理所有索引)
    ├─ IXHandler (单个索引操作)
    │   └─ BPTree (B+ 树实现)
    │       └─ BPTreeNode (树节点)
    └─ HashMap (索引映射)
    
┌─→ FileManager (持久化)
└─→ PageManager (页管理)
```

## 主要设计特点

### 1. 映射存储
- 使用 HashMap 存储所有打开的处理器
- 键为 `{table}_{index_no}`
- 支持快速查找

### 2. 延迟初始化
- 索引在打开时才初始化
- 支持重复打开
- 节省资源

### 3. 自动资源管理
- `close()` 自动调用 `force_pages()`
- `close_all()` 批量清理
- Drop 时清空资源

### 4. 错误传播
- 所有错误返回给调用者
- 支持链式错误处理
- 便于调试

## 常见使用模式

### 模式 1：单次操作

```rust
let mut manager = IXManager::new();
manager.create_index("emp", 0, 4)?;
{
    let handler = manager.get_handler_mut("emp", 0)?;
    handler.insert_entry(vec![1], (100, 1))?;
}
manager.close_index("emp", 0)?;
```

### 模式 2：批量操作

```rust
let mut manager = IXManager::new();

// 创建多个索引
manager.create_index("emp", 0, 4)?;
manager.create_index("emp", 1, 10)?;
manager.create_index("dept", 0, 6)?;

// 批量插入
for idx in manager.list_indexes() {
    let parts: Vec<&str> = idx.split('_').collect();
    if parts.len() == 2 {
        let table = parts[0];
        let idx_no = parts[1].parse()?;
        let handler = manager.get_handler_mut(table, idx_no)?;
        handler.insert_entry(vec![1, 2], (100, 1))?;
    }
}

// 关闭所有
manager.close_all()?;
```

### 模式 3：持久化

```rust
let mut manager = IXManager::new();
manager.create_index("emp", 0, 4)?;

// 执行操作
{
    let handler = manager.get_handler_mut("emp", 0)?;
    handler.insert_entry(vec![1], (100, 1))?;
    // force_pages 在关闭时自动调用
}

manager.close_all()?;  // 确保所有数据持久化
```

## 性能特性

| 操作 | 时间复杂度 | 说明 |
|------|-----------|------|
| create_index | O(1) | HashMap 插入 |
| destroy_index | O(1) | HashMap 移除 |
| open_index | O(1) | HashMap 查找 |
| close_index | O(1) | HashMap 移除 |
| get_handler | O(1) | HashMap 查找 |
| list_indexes | O(n) | n = 打开的索引数 |
| close_all | O(n) | 逐个关闭 |

## 与 IXHandler 的交互

```
IXManager 提供：
├─ 生命周期管理 (create/destroy/open/close)
├─ 索引映射 (存储、查找)
└─ 批量操作 (list, close_all)

IXHandler 提供：
├─ 数据操作 (insert/delete/search)
├─ 树管理 (init/open)
└─ 资源管理 (force_pages/close)
```

## 后续改进方向

### 短期
1. 实现索引持久化配置
2. 添加索引统计信息
3. 支持索引重建

### 中期
1. 实现索引缓存管理
2. 添加并发访问控制
3. 支持索引监控

### 长期
1. 分布式索引管理
2. 动态索引优化
3. 自适应性能调优

## 代码统计

| 组件 | 行数 |
|------|------|
| 核心结构 | 3 |
| 主要方法 | ~120 |
| 辅助方法 | ~20 |
| 测试代码 | ~100 |
| 总计 | ~243 |

## 集成建议

### 在 RecordManager 中使用
```rust
pub struct RecordManager {
    tm: TableManager,
    ix_manager: IXManager,  // 添加索引管理器
}

impl RecordManager {
    pub fn insert_record(&mut self, table: &str, record: &Record) -> Result<RID> {
        let rid = self.tm.insert_record(table, record)?;
        
        // 更新索引
        for idx_no in 0..self.ix_manager.list_indexes().len() {
            let handler = self.ix_manager.get_handler_mut(table, idx_no)?;
            handler.insert_entry(extract_key(record), rid)?;
        }
        
        Ok(rid)
    }
}
```

## 使用示例（完整）

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut manager = IXManager::new();
    
    // 创建员工表索引
    manager.create_index("employee", 0, 4)?;  // ID 索引
    manager.create_index("employee", 1, 4)?;  // 名字索引
    
    // 插入数据
    {
        let id_index = manager.get_handler_mut("employee", 0)?;
        id_index.insert_entry(vec![1, 0, 0, 0], (100, 0))?;  // 员工ID=1
        
        let name_index = manager.get_handler_mut("employee", 1)?;
        name_index.insert_entry(vec![74, 111, 104, 110],  (100, 0))?;  // "John"
    }
    
    // 查询（使用索引）
    {
        let id_index = manager.get_handler("employee", 0)?;
        match id_index.search_entry(&vec![1, 0, 0, 0])? {
            Some(rid) => println!("Found employee at {:?}", rid),
            None => println!("Employee not found"),
        }
    }
    
    // 清理
    manager.close_all()?;
    
    Ok(())
}
```

---

**实现日期**: 2025-11-11
**状态**: ✅ 完全实现，7/7 测试通过
**总测试**: 24 个单元测试全部通过
