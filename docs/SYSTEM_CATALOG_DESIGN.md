# 系统表（Catalog）设计文档

## 概述

数据库引擎中的数据字典（Data Dictionary）通过 **Catalog 系统** 来管理，负责维护所有创建表的元数据信息。系统采用**内存缓存 + 磁盘持久化**的混合策略。

## 架构设计

### 1. 核心组件

#### `CatalogManager` - 数据字典管理器
- **位置**: `src/rm/catalog_manager.rs`
- **职责**: 
  - 维护内存中的表 Schema 缓存 (`HashMap<String, TableSchema>`)
  - 自动分配表 ID（从 1 开始递增）
  - 序列化/反序列化到磁盘
  - 提供表元数据的 CRUD 接口

#### `TableSchema` - 表模式结构
- **位置**: `src/rm/types.rs`
- **功能**: 存储表的完整元数据

### 2. 系统表存储的元数据

```rust
pub struct TableSchema {
    // 标识信息
    pub table_name: String,           // 表名（唯一，用户指定）
    pub table_id: u32,                // 表的全局唯一 ID（系统自动分配）
    
    // 模式定义
    pub columns: Vec<ColumnDef>,      // 所有列的定义
    
    // 存储信息
    pub root_pages: Vec<PageId>,      // 初始/主数据页
    
    // 统计信息
    pub create_time: u64,             // 创建时间戳（秒）
    pub row_count: u64,               // 表中的记录数
    pub last_modified: u64,           // 最后修改时间戳（秒）
}

pub struct ColumnDef {
    pub name: String,                 // 列名
    pub data_type: DataType,          // 数据类型（Int32/Char/VarChar）
    pub nullable: bool,               // 是否允许 NULL
}

pub enum DataType {
    Int32,
    Char(usize),                      // 固定长度字符串
    VarChar,                          // 变长字符串
}
```

## 工作流程

### 1. 初始化流程

```
应用启动
  ↓
CatalogManager::new()
  ├─ 创建空 HashMap<String, TableSchema>
  ├─ 设置 next_table_id = 1
  ├─ 尝试从磁盘加载 catalog.tbl
  │   ├─ 如果存在：反序列化并加载所有表元数据
  │   └─ 如果不存在：以空 catalog 启动
  └─ 计算下一个可用 table_id = max_id + 1
```

### 2. 创建表流程

```
用户调用 RecordManager::create_table(schema)
  ↓
TableManager::create_table(schema)
  ├─ 调用 CatalogManager::create_table(schema)
  │   ├─ 验证表名未被使用
  │   ├─ 自动分配 table_id = next_table_id++
  │   ├─ 初始化时间戳：
  │   │   ├─ create_time = 当前时间戳
  │   │   ├─ last_modified = 当前时间戳
  │   │   └─ row_count = 0
  │   ├─ 存储到内存缓存
  │   └─ 调用 flush_to_disk() 持久化
  │       ├─ 序列化整个 schemas HashMap
  │       ├─ 写入 data/catalog.tbl 的第 0 页（4096 字节）
  │       └─ 输出: "[CatalogManager] Flushed X tables to disk (Y bytes)"
  │
  └─ 创建数据文件：data/{table_name}.tbl
      ├─ 初始化 FileHeader
      └─ 写入文件头到第 0 页
```

### 3. 查询表流程

```
用户查询表信息
  ↓
CatalogManager::get_table(table_name)
  ├─ 从内存缓存查找
  └─ 返回 &TableSchema（不可变引用）

CatalogManager::get_table_schema(table_name)
  ├─ 从内存缓存查找
  └─ 返回 TableSchema（克隆副本）
```

### 4. 表打开流程

```
打开表进行 CRUD 操作
  ↓
TableManager::open_table(table_name)
  ├─ 检查表是否已打开（避免重复）
  ├─ 从 CatalogManager 查询 schema
  ├─ 从磁盘加载表对应的文件头
  ├─ 创建 FileHandler（管理页面分配）
  ├─ 创建 TableHandler（管理记录插入/查询）
  └─ 加入 open_tables Map
```

## 磁盘持久化格式

### Catalog 文件结构
- **文件路径**: `data/catalog.tbl`
- **页大小**: 4096 字节（PAGE_SIZE）
- **页码**: 第 0 页（固定）
- **数据格式**:
  ```
  ┌─────────────────────────────────────────┐
  │ Serialized HashMap<String, TableSchema> │  (变长，末尾补 0)
  ├─────────────────────────────────────────┤
  │ 0 字节填充（对齐到 4096 字节）            │
  └─────────────────────────────────────────┘
  ```

### 序列化方式
- 使用 `bincode` crate 进行序列化
- `serde` 属性装饰所有元数据结构
- 支持版本升级（通过重新序列化）

## 性能特性

### 优势
1. **快速启动**: 整个 Catalog 一次加载到内存
2. **零查询延迟**: 所有查询直接在内存 HashMap 中进行
3. **原子性**: 每次修改都完整 flush，保证一致性
4. **可扩展**: 支持无限数量的表（受内存限制）

### 限制
1. **内存占用**: Catalog 大小必须 ≤ 4096 字节
   - 适合小中型系统（几百个表）
   - 超大型系统需要分层索引

2. **并发限制**: 单 HashMap，无表级锁
   - 当前设计适合单线程应用
   - 多线程需要添加 RwLock

## 元数据字段说明

| 字段 | 类型 | 来源 | 说明 |
|------|------|------|------|
| table_name | String | 用户提供 | 表名唯一标识 |
| table_id | u32 | 系统分配 | 全局唯一 ID，从 1 开始 |
| columns | Vec<ColumnDef> | 用户定义 | 表结构 |
| root_pages | Vec<PageId> | 系统维护 | 初始数据页 |
| create_time | u64 | 系统生成 | 创建时间戳（秒） |
| row_count | u64 | 应用维护 | 当前行数（可选，用于优化） |
| last_modified | u64 | 系统更新 | 最后修改时间 |

## 使用示例

### 创建表
```rust
let schema = TableSchema {
    table_name: "account".to_string(),
    table_id: 0,  // 会被 CatalogManager 覆盖
    columns: vec![
        ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        },
        // ... 更多列
    ],
    root_pages: vec![],
    create_time: 0,      // 会被初始化
    row_count: 0,
    last_modified: 0,    // 会被初始化
};

let mut rm = RecordManager::new(table_manager, logger);
rm.create_table(schema)?;  // table_id 自动分配，时间戳自动初始化
```

### 查询表信息
```rust
if let Some(schema) = catalog.get_table("account") {
    println!("Table ID: {}", schema.table_id);
    println!("Created: {}", schema.create_time);
    println!("Rows: {}", schema.row_count);
}
```

## 运行示例输出

```
===== Task1 DB Test Start =====
[CatalogManager] Catalog file not found, starting with empty schema
[CatalogManager] Initialized with 0 tables, next_table_id=1
...
Creating table: account
[CatalogManager] Added table 'account' to memory cache with table_id=1
[CatalogManager] Flushed 1 tables to disk (142 bytes)
[TableManager] Created table file: data/account.tbl with initialized header
Table 'account' created in catalog.
...
Inserted 10000 records successfully!
Scanned 10000 records.
===== Test Completed Successfully =====
```

## 扩展方向

### 短期改进
1. **统计维护**: 在每次 INSERT/DELETE 后更新 row_count
2. **索引元数据**: 添加索引信息存储
3. **视图支持**: 添加视图定义存储

### 中期改进
1. **分层索引**: 支持超大 Catalog（> 4096 字节）
2. **并发支持**: 添加 RwLock 支持多线程
3. **版本控制**: Schema 演变历史记录

### 长期改进
1. **B+ 树索引**: 用 B+ 树替代 HashMap
2. **热迁移**: 不停机 schema 升级
3. **分布式**: 支持跨数据库 Catalog 同步

## 相关文件

- `src/rm/catalog_manager.rs` - CatalogManager 实现
- `src/rm/types.rs` - TableSchema 和 ColumnDef 定义
- `src/rm/table_manager.rs` - 表管理器（使用 CatalogManager）
- `src/rm/record_manager.rs` - 记录管理器入口
- `src/test/task1.rs` - 测试程序

## 总结

该系统采用**简洁高效**的设计，通过内存 HashMap 提供快速元数据访问，并通过 bincode 序列化实现持久化。完全满足中等规模数据库应用的需求。
