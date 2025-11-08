# 系统表实现变更总结

## 项目需求
为数据库引擎添加完整的系统表（Catalog）管理功能，包括表的元数据存储和自动初始化逻辑。

## 解决方案概览

### 核心思想
实现一个**内存 + 磁盘持久化**的数据字典系统，通过 `CatalogManager` 统一管理所有表的元数据。

```
┌─────────────────┐      ┌──────────────────────┐      ┌──────────────────┐
│  RecordManager  │─────▶│  CatalogManager      │◀────▶│ data/catalog.tbl │
│                 │      │                      │      │ (bincode 序列化)  │
└─────────────────┘      │  HashMap<String,     │      └──────────────────┘
                         │   TableSchema>       │
                         │  next_table_id: u32  │
                         └──────────────────────┘
```

## 实现细节

### 1️⃣ 扩展 TableSchema 结构（types.rs）

**之前**:
```rust
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub root_pages: Vec<PageId>,
}
```

**之后**:
```rust
pub struct TableSchema {
    pub table_name: String,
    pub table_id: u32,              // ✨ 新增
    pub columns: Vec<ColumnDef>,
    pub root_pages: Vec<PageId>,
    pub create_time: u64,           // ✨ 新增
    pub row_count: u64,             // ✨ 新增
    pub last_modified: u64,         // ✨ 新增
}
```

**新增方法**:
```rust
impl TableSchema {
    pub fn current_timestamp() -> u64 {
        // 获取当前时间戳（秒）
    }
}
```

### 2️⃣ 增强 CatalogManager（catalog_manager.rs）

**添加字段**:
```rust
pub struct CatalogManager {
    schemas: HashMap<String, TableSchema>,
    next_table_id: u32,  // ✨ 新增：自动分配 ID
}
```

**改进 `new()` 方法**:
```rust
// 自动计算下一个可用的 table_id
let max_id = mgr.schemas.values()
    .map(|schema| schema.table_id)
    .max()
    .unwrap_or(0);
mgr.next_table_id = max_id + 1;
```

**改进 `create_table()` 方法**:
```rust
pub fn create_table(&mut self, mut schema: TableSchema) -> Result<(), String> {
    // 自动分配 table_id
    schema.table_id = self.next_table_id;
    self.next_table_id += 1;
    
    // 初始化时间戳
    let now = TableSchema::current_timestamp();
    schema.create_time = now;
    schema.last_modified = now;
    schema.row_count = 0;
    
    // 存储并持久化
    self.schemas.insert(name.clone(), schema);
    self.flush_to_disk()?;
}
```

**更新 `Default` 实现**:
```rust
impl Default for CatalogManager {
    fn default() -> Self {
        CatalogManager {
            schemas: HashMap::new(),
            next_table_id: 1,  // ✨ 从 1 开始
        }
    }
}
```

### 3️⃣ 更新 task1.rs 测试程序

**修改表创建代码**:
```rust
let schema = TableSchema {
    table_name: "account".to_string(),
    table_id: 0,           // ✨ 会被自动覆盖
    columns: vec![/* ... */],
    root_pages: vec![],
    create_time: 0,        // ✨ 会被初始化
    row_count: 0,
    last_modified: 0,      // ✨ 会被初始化
};
```

## 工作流程演示

### 初始化阶段
```
应用启动
  ↓
CatalogManager::new()
  ├─ 判断 data/catalog.tbl 是否存在
  ├─ 若不存在：使用空 HashMap，next_table_id = 1
  ├─ 若存在：反序列化加载，重新计算 next_table_id = max(table_id) + 1
  └─ 输出：[CatalogManager] Initialized with 0 tables, next_table_id=1
```

### 创建表阶段
```
RecordManager::create_table(schema)
  ↓
CatalogManager::create_table(schema)
  ├─ 自动分配：schema.table_id = 1
  ├─ 初始化时间：schema.create_time = 1234567890
  ├─ 初始化时间：schema.last_modified = 1234567890
  ├─ 初始化计数：schema.row_count = 0
  ├─ 内存存储：schemas["account"] = schema
  ├─ 磁盘持久化：bincode 序列化写入 data/catalog.tbl
  └─ 输出：
     [CatalogManager] Added table 'account' to memory cache with table_id=1
     [CatalogManager] Flushed 1 tables to disk (142 bytes)
```

## 系统表存储的元数据

| 字段名 | 类型 | 来源 | 说明 |
|--------|------|------|------|
| table_name | String | 用户 | 表的逻辑名称 |
| **table_id** | u32 | 系统 | 全局唯一标识，从 1 递增 |
| columns | Vec<ColumnDef> | 用户 | 表的列定义 |
| root_pages | Vec<PageId> | 系统 | 初始数据页 |
| **create_time** | u64 | 系统 | 创建时间戳（秒） |
| **row_count** | u64 | 系统 | 表中的行数 |
| **last_modified** | u64 | 系统 | 最后修改时间戳（秒） |

## 文件修改汇总

### 修改的文件
1. ✏️ `src/rm/types.rs` - 扩展 TableSchema 结构
2. ✏️ `src/rm/catalog_manager.rs` - 添加 ID 分配逻辑
3. ✏️ `src/test/task1.rs` - 更新 schema 初始化

### 新增文档
1. 📄 `SYSTEM_CATALOG_DESIGN.md` - 详细设计文档
2. 📄 `CATALOG_QUICK_REFERENCE.md` - 快速参考指南

## 验证结果

✅ **编译成功**
```
warning: ... (仅为未使用导入警告)
Finished `dev` profile
```

✅ **运行测试成功**
```
===== Task1 DB Test Start =====
[CatalogManager] Initialized with 0 tables, next_table_id=1
...
[CatalogManager] Added table 'account' to memory cache with table_id=1
[CatalogManager] Flushed 1 tables to disk (142 bytes)
...
Inserted 10000 records successfully!
Scanned 10000 records.
===== Test Completed Successfully =====
```

## 关键特性

### 🎯 自动分配
- table_id 从 1 开始自动递增
- 每次创建表时自动 +1
- 系统重启后自动恢复到最大值 + 1

### 🎯 自动初始化
- 创建时自动设置时间戳
- 自动初始化行数为 0
- 自动记录创建时间

### 🎯 持久化保证
- 每次修改立即 flush 到磁盘
- bincode 序列化确保数据完整性
- 单页 4096 字节存储（支持 ~100-500 个表）

### 🎯 查询高效
- O(1) 时间查找表元数据
- 所有数据在内存 HashMap 中
- 无磁盘 I/O 开销

## 后续改进方向

### 短期
- [ ] 在每次 INSERT/DELETE 后更新 row_count
- [ ] 添加索引元数据存储
- [ ] 添加表统计信息（如平均行大小）

### 中期
- [ ] 支持超大 Catalog（分层索引）
- [ ] 支持多线程并发访问
- [ ] Schema 版本控制

### 长期
- [ ] B+ 树索引结构
- [ ] 在线 Schema 升级
- [ ] 分布式 Catalog 同步

## 总结

通过本次实现，数据库引擎现在拥有了完整的**系统表管理**功能：

1. ✅ **元数据存储**：table_id、时间戳、行数等
2. ✅ **自动分配**：table_id 自动递增，无需用户干预
3. ✅ **持久化**：Catalog 自动 flush 到磁盘
4. ✅ **高效查询**：O(1) 内存查找
5. ✅ **完全功能**：task1.rs 成功运行 10000 条记录插入

该设计简洁、高效、易于维护，完全满足中等规模数据库应用的需求。
