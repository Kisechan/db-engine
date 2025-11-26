# 执行引擎设计文档 - 火山模型实现

## 1. 概述

执行引擎采用 **Volcano 模型**（火山模型），这是现代数据库系统中最常用的迭代式查询执行模型。

### 火山模型的核心特点

```
    Project
      |
    Filter
      |
   SeqScan (数据源)
```

每层执行算子（executor）采用统一的接口：
- **init()**: 初始化算子，准备执行状态
- **next()**: 获取下一条记录，返回 `Option<Record>` 或错误
- **close()**: 清理资源（可选）

## 2. 架构设计

### 2.1 接口层 (iterator.rs)

#### Executor Trait

```rust
pub trait Executor {
    fn init(&mut self) -> Result<(), String>;
    fn next(&mut self) -> Result<Option<ExecutorRecord>, String>;
    fn close(&mut self) -> Result<(), String>;
}
```

**优势**：
- 流式处理，内存占用恒定
- 支持任意组合和嵌套
- 便于实现物理优化（算子融合、谓词下推）

### 2.2 执行器实现

#### SeqScanExecutor（全表扫描）

```
输入: table_name, table_handler
输出: Record 流（RID + 数据）
```

**工作流程**:

1. **初始化 (init)**
   - 加载表的所有数据页 ID
   - 初始化扫描位置（第一页，第一个 slot）
   - 加载第一页的 slot 数量

2. **获取记录 (next)**
   ```
   loop {
       if slot_idx >= page.slot_count {
           // 移到下一页
           current_page_idx += 1
           if current_page_idx >= data_pages.len() {
               return None  // 扫描完成
           }
           load_next_page()
           continue
       }
       
       if slot[slot_idx].offset == -1 {
           // 记录已删除，跳过
           slot_idx += 1
           continue
       }
       
       // 读取当前记录
       record = read_record(RID)
       slot_idx += 1
       return Some(record)
   }
   ```

3. **关闭 (close)**
   - 刷新缓冲区
   - 释放资源

## 3. 数据流

### 记录格式

```rust
pub struct ExecutorRecord {
    pub rid: RID,           // 记录位置 (page_id, slot_id)
    pub data: Vec<u8>,      // 记录的序列化数据
}
```

### RID 说明

- **page_id**: 数据页编号（4 字节）
- **slot_id**: 页内 slot 索引（2 字节）

## 4. 与现有模块的集成

### 与 rm 模块的交互

```
SeqScanExecutor
    ├─ table_handler.get_data_pages()
    │  └─ 获取表的所有数据页 ID 列表
    │
    ├─ table_handler.buffer_manager.fetch_page()
    │  └─ 获取页内容（自动处理缓冲）
    │
    ├─ PageHandler.read_header()
    │  └─ 读取页头（获取 slot 数量）
    │
    ├─ PageHandler.read_slot()
    │  └─ 读取 slot 条目（检查有效性）
    │
    └─ table_handler.get(rid)
       └─ 读取完整记录（处理变长数据）
```

## 5. 所有权模型

### 设计原则

SeqScanExecutor **拥有** TableHandler，避免引用生命周期问题：

```rust
pub struct SeqScanExecutor {
    table_handler: TableHandler,  // 直接拥有（不是 Option 或引用）
    // ...
}
```

**好处**：
- 所有权清晰
- 避免借用冲突
- 内部方法可自由调用 `self.table_handler` 的可变方法

## 6. 使用示例

### 基本扫描流程

```rust
// 1. 创建执行器
let mut executor = SeqScanExecutor::new(
    "users".to_string(),
    table_handler
);

// 2. 初始化
executor.init()?;

// 3. 逐条读取记录
loop {
    match executor.next()? {
        Some(record) => {
            println!("RID: ({}, {}), 数据: {:?}", 
                     record.rid.page_id, 
                     record.rid.slot_id, 
                     record.data);
        }
        None => break,
    }
}

// 4. 清理资源
executor.close()?;
```

## 7. 性能特征

### 时间复杂度

- **init()**: O(1) - 仅初始化指针
- **next()**: O(1) 摊销 - 每条记录恒定时间
- **全表扫描**: O(N) - N 为记录总数

### 空间复杂度

- 内存占用: O(1) - 每次只在内存中保持 1 条记录
- 缓冲区: 由 BufferManager 管理，与池大小无关

## 8. 扩展点

### 未来可以实现的算子

1. **FilterExecutor**: 应用 WHERE 条件
   ```rust
   pub struct FilterExecutor {
       child: Box<dyn Executor>,
       predicate: Expression,
   }
   ```

2. **ProjectionExecutor**: 选择特定列
   ```rust
   pub struct ProjectionExecutor {
       child: Box<dyn Executor>,
       columns: Vec<String>,
   }
   ```

3. **JoinExecutor**: 联接两个表
   ```rust
   pub struct HashJoinExecutor {
       left: Box<dyn Executor>,
       right: Box<dyn Executor>,
       join_key: String,
   }
   ```

4. **AggregateExecutor**: 聚合操作
   ```rust
   pub struct AggregateExecutor {
       child: Box<dyn Executor>,
       agg_func: AggFunction,
   }
   ```

### 实现递归处理

```rust
// 使用 Box<dyn Executor> 进行多态组合
let seq_scan = Box::new(SeqScanExecutor::new(...));
let filter = Box::new(FilterExecutor::new(seq_scan, ...));
let project = Box::new(ProjectionExecutor::new(filter, ...));

// 执行查询
let mut root_executor: Box<dyn Executor> = project;
root_executor.init()?;
while let Some(record) = root_executor.next()? {
    // 处理记录
}
```

## 9. 错误处理

### 可能的错误场景

1. **未初始化错误**
   - 调用 next() 前未调用 init()
   - 返回: `Err("Executor not initialized")`

2. **页加载失败**
   - 从磁盘读取页失败
   - 返回: BufferManager 的错误信息

3. **损坏数据**
   - 页头或 slot 条目损坏
   - 返回: PageHandler 的错误信息

## 10. 总结

| 组件 | 职责 |
|------|------|
| **iterator.rs** | 定义标准接口 (Executor trait) |
| **scan.rs** | 实现全表扫描 (SeqScanExecutor) |
| **mod.rs** | 导出公共 API |

这个设计为后续的优化和扩展提供了坚实的基础，同时保持了代码的简洁性和可维护性。
