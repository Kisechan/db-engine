# 执行引擎快速参考

## 模块结构

```
src/exec/
├── iterator.rs (131 行)   - 火volcano模型接口定义
├── scan.rs (198 行)       - 全表扫描执行器实现
└── mod.rs (25 行)         - 模块导出
```

## 核心接口

### Executor Trait

```rust
pub trait Executor {
    fn init(&mut self) -> Result<(), String>;
    fn next(&mut self) -> Result<Option<ExecutorRecord>, String>;
    fn close(&mut self) -> Result<(), String>;
}
```

### 记录结构

```rust
pub struct ExecutorRecord {
    pub rid: RID,           // 记录位置
    pub data: Vec<u8>,      // 记录数据
}
```

## 使用示例

### 1. 创建并初始化执行器

```rust
use crate::exec::{Executor, SeqScanExecutor};

// 假设 table_handler 已经创建
let mut executor = SeqScanExecutor::new(
    "users".to_string(),
    table_handler
);

executor.init()?;
```

### 2. 逐条读取记录

```rust
while let Some(record) = executor.next()? {
    println!("RID: ({}, {})", record.rid.page_id, record.rid.slot_id);
    println!("Data: {:?}", record.data);
    
    // 对记录进行处理...
}
```

### 3. 关闭执行器

```rust
executor.close()?;
```

## 主要特性

| 特性 | 说明 |
|------|------|
| **流式处理** | 每次 next() 只返回一条记录，内存占用恒定 |
| **所有权清晰** | SeqScanExecutor 拥有 TableHandler，无引用冲突 |
| **错误处理** | 所有操作返回 Result，支持链式调用 |
| **可扩展性** | Executor trait 支持多态组合 |
| **与 rm 模块集成** | 直接使用 TableHandler 接口 |

## 与 rm 模块的接口

### TableHandler 依赖

```rust
// 获取数据页列表
table_handler.get_data_pages() -> &[PageId]

// 获取单条记录
table_handler.get(rid: RID) -> Result<Vec<u8>>

// 获取页缓冲
table_handler.buffer_manager.fetch_page(page_id) 
    -> Result<&mut [u8]>

// 释放页缓冲
table_handler.buffer_manager.unpin_page(page_id, dirty)
    -> Result<()>

// 刷新缓冲区
table_handler.flush() -> Result<()>
```

### PageHandler 依赖

```rust
// 读取页头信息
PageHandler::new(page_buf, page_id)
    .read_header() -> Result<PageHeader>

// 读取 slot 条目
PageHandler::new(page_buf, page_id)
    .read_slot(slot_id) -> Result<SlotEntry>
```

## 设计原理

### 火山模型的优势

1. **内存效率**: O(1) 空间，无需缓存所有结果
2. **流式处理**: 适合处理大规模数据
3. **算子组合**: 支持任意层级的执行树
4. **物理优化**: 便于实现下推、融合等优化技术

### 扫描流程

1. **init()** - 加载数据页列表，初始化游标
2. **next()** - 跳过已删除的 slot，读取有效记录
3. **close()** - 刷新缓冲区，释放资源

## 测试状态

- ✅ iterator.rs: 1 个测试通过 (MockExecutor)
- ✅ scan.rs: 1 个测试通过 (接口验证)
- ✅ 编译成功，无错误

## 下一步扩展

### 可以实现的算子

```rust
// 1. 过滤算子
pub struct FilterExecutor {
    child: Box<dyn Executor>,
    predicate: Expression,
}

// 2. 投影算子
pub struct ProjectionExecutor {
    child: Box<dyn Executor>,
    columns: Vec<String>,
}

// 3. 排序算子
pub struct SortExecutor {
    child: Box<dyn Executor>,
    order_by: Vec<OrderBy>,
}

// 4. 聚合算子
pub struct AggregateExecutor {
    child: Box<dyn Executor>,
    group_by: Vec<String>,
    aggregates: Vec<(AggFunc, String)>,
}
```

### 算子组合示例

```rust
// 构建执行树: Project(Filter(SeqScan))
let seq_scan = Box::new(
    SeqScanExecutor::new("users".to_string(), handler)
) as Box<dyn Executor>;

let filter = Box::new(
    FilterExecutor::new(seq_scan, where_expr)
) as Box<dyn Executor>;

let project = Box::new(
    ProjectionExecutor::new(filter, vec!["id", "name"])
) as Box<dyn Executor>;

// 执行
let mut executor: Box<dyn Executor> = project;
executor.init()?;
while let Some(record) = executor.next()? {
    // 处理投影和过滤后的记录
}
```

## 常见问题

### Q: 为什么 SeqScanExecutor 拥有 TableHandler 而不是引用它？

**A**: 如果使用引用，会产生生命周期问题。SeqScanExecutor 内部方法需要同时调用 TableHandler 的多个方法（fetch_page, read_header, read_slot 等），这会导致多个可变借用冲突。直接拥有可以避免这些问题。

### Q: 如何处理已删除的记录？

**A**: 在 rm 模块中，删除记录时会将 slot.offset 设为 -1（而不是物理删除）。SeqScanExecutor 的 skip_deleted_records() 方法会检查这个标记，自动跳过已删除的 slot。

### Q: 性能如何？

**A**: 
- 每条记录: O(1) 时间
- 全表扫描: O(N) 时间，N 为记录总数
- 内存占用: O(1) - 每次只保存 1 条记录

### Q: 支持并发访问吗？

**A**: 当前版本不支持并发。可以通过添加互斥锁或使用原子操作来支持并发。

## 参考文献

- Volcano Model: Graefe, G. (1990). "Volcano, An Extensible and Parallel Query Evaluation System"
- 现代数据库系统设计（第五版）- 执行引擎章节
