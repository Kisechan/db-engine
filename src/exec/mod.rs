// 执行引擎模块 (Execution Engine)
// 
// 实现火山模型的查询执行引擎，包含：
// - iterator: 火volcano模型的标准执行器接口
// - scan: 全表扫描执行器的实现
// 
// 火山模型架构：
// ```
//   Project
//     │
//     ├─ Filter
//     │   └─ SeqScan (扫描输出)
// ```
// 
// 每层算子通过 next() 从下层获取记录，进行处理后向上层返回。
// 这样设计的优势：
// - 内存占用低（逐条处理）
// - 支持流式处理
// - 便于实现算子融合和下推优化

pub mod iterator;
pub mod scan;
pub mod filter;
pub mod join;
pub mod statement_executor;

pub use iterator::{Executor, ExecutorRecord, ExecutorBox};
pub use scan::SeqScanExecutor;
pub use filter::FilterExecutor;
pub use join::NestedLoopJoinExecutor;
pub use statement_executor::{StatementExecutor, ExecutionResult, ExecutorError};
