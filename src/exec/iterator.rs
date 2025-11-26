// 执行器接口 - 火山模型 (Volcano Model) 标准接口
// 
// 火山模型是经典的迭代式查询执行模型，采用树状执行算子结构。
// 每个算子实现了统一的接口，通过 init() 初始化，next() 逐条返回结果。
// 
// 架构：
//   Executor (trait)
//     └── next() -> Option<Record>
//     └── init() -> Result<()>
//
// 优点：
// - 内存占用低（流式处理）
// - 支持算子组合
// - 便于实现物理层优化

use crate::common::types::RID;

// 执行器返回的记录类型
// 包含 RID（用于后续定位）和数据字节流
#[derive(Debug, Clone)]
pub struct ExecutorRecord {
    pub rid: RID,
    pub data: Vec<u8>,
}

// 火山模型执行器的标准接口
// 
// 所有算子都需要实现此 trait，包括扫描、过滤、投影、聚合等。
pub trait Executor {
    // 初始化执行器
    // 
    // 该方法在第一次调用 next() 前必须调用，用于：
    // - 打开文件/表
    // - 初始化内部状态
    // - 准备游标位置
    // 
    // # Returns
    // - Ok(()) 表示初始化成功
    // - Err(String) 表示初始化失败
    fn init(&mut self) -> Result<(), String>;

    // 获取下一条记录
    // 
    // 该方法每次调用时返回一条记录，直到没有记录为止。
    // 采用流式设计，每次只在内存中保持一条记录。
    // 
    // # Returns
    // - Ok(Some(record)) 表示成功获取一条记录
    // - Ok(None) 表示到达流的末尾（遍历完成）
    // - Err(String) 表示执行过程中出错
    fn next(&mut self) -> Result<Option<ExecutorRecord>, String>;

    // 关闭执行器（可选操作）
    // 
    // 用于释放资源，如关闭文件句柄、释放缓冲区等。
    // 默认实现为空操作。
    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

// 使用 Box<dyn Executor> 来支持多态执行器组合
pub type ExecutorBox = Box<dyn Executor>;

#[cfg(test)]
mod tests {
    use super::*;

    // 简单的 Mock 执行器用于测试
    struct MockExecutor {
        data: Vec<ExecutorRecord>,
        current_index: usize,
    }

    impl MockExecutor {
        fn new(data: Vec<ExecutorRecord>) -> Self {
            MockExecutor {
                data,
                current_index: 0,
            }
        }
    }

    impl Executor for MockExecutor {
        fn init(&mut self) -> Result<(), String> {
            self.current_index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<ExecutorRecord>, String> {
            if self.current_index < self.data.len() {
                let record = self.data[self.current_index].clone();
                self.current_index += 1;
                Ok(Some(record))
            } else {
                Ok(None)
            }
        }
    }

    #[test]
    fn test_executor_interface() {
        let records = vec![
            ExecutorRecord {
                rid: crate::common::types::RID { page_id: 1, slot_id: 0 },
                data: vec![1, 2, 3],
            },
            ExecutorRecord {
                rid: crate::common::types::RID { page_id: 1, slot_id: 1 },
                data: vec![4, 5, 6],
            },
        ];

        let mut executor = MockExecutor::new(records);
        executor.init().unwrap();

        // 获取第一条记录
        let rec1 = executor.next().unwrap();
        assert!(rec1.is_some());
        assert_eq!(rec1.unwrap().data, vec![1, 2, 3]);

        // 获取第二条记录
        let rec2 = executor.next().unwrap();
        assert!(rec2.is_some());
        assert_eq!(rec2.unwrap().data, vec![4, 5, 6]);

        // 检查遍历完成
        let rec3 = executor.next().unwrap();
        assert!(rec3.is_none());
    }
}
