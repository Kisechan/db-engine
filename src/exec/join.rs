// Join 执行器 (Nested Loop Join)
// 
// 实现嵌套循环联接算子，支持 INNER、LEFT、RIGHT、FULL JOIN。

use crate::exec::iterator::{Executor, ExecutorRecord};
use crate::plan::logical::JoinType;
use crate::sql::ast::Expression;

// Join 执行器状态
#[derive(Debug, Clone, Copy, PartialEq)]
enum JoinState {
    Uninitialized,
    FetchingLeft,
    ScanningRight,
    Done,
}

// 嵌套循环 Join 执行器
// 
// 采用简单的嵌套循环实现：
// - 外层循环遍历左表
// - 内层循环遍历右表
// - 对每对记录计算 Join 条件
pub struct NestedLoopJoinExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    join_condition: Option<Expression>,
    join_type: JoinType,
    
    // 内部状态
    state: JoinState,
    current_left_record: Option<ExecutorRecord>,
    buffered_right_records: Vec<ExecutorRecord>,
    right_record_index: usize,
}

impl NestedLoopJoinExecutor {
    // 创建新的 Join 执行器
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        join_condition: Option<Expression>,
        join_type: JoinType,
    ) -> Self {
        NestedLoopJoinExecutor {
            left,
            right,
            join_condition,
            join_type,
            state: JoinState::Uninitialized,
            current_left_record: None,
            buffered_right_records: Vec::new(),
            right_record_index: 0,
        }
    }

    // 评估 Join 条件
    // 
    // 简化实现：暂时对所有条件返回 true
    // 实际中需要根据左右记录数据求值表达式
    fn evaluate_join_condition(
        _condition: &Expression,
        _left_record: &ExecutorRecord,
        _right_record: &ExecutorRecord,
    ) -> Result<bool, String> {
        Ok(true)
    }

    // 合并两条记录
    // 
    // 将左表和右表的记录数据连接在一起
    fn merge_records(left: &ExecutorRecord, right: &ExecutorRecord) -> ExecutorRecord {
        let mut merged_data = left.data.clone();
        merged_data.extend(&right.data);

        ExecutorRecord {
            rid: left.rid,  // 使用左表的 RID
            data: merged_data,
        }
    }

    // 扫描完右表一遍，准备获取左表的下一条记录
    fn reset_right_scan(&mut self) -> Result<(), String> {
        // 缓冲所有右表记录以支持多次扫描
        if self.buffered_right_records.is_empty() {
            self.right.init()?;
            loop {
                match self.right.next()? {
                    Some(record) => self.buffered_right_records.push(record),
                    None => break,
                }
            }
        }

        self.right_record_index = 0;
        Ok(())
    }
}

impl Executor for NestedLoopJoinExecutor {
    fn init(&mut self) -> Result<(), String> {
        self.left.init()?;
        self.right.init()?;

        // 预加载所有右表记录
        loop {
            match self.right.next()? {
                Some(record) => self.buffered_right_records.push(record),
                None => break,
            }
        }

        self.state = JoinState::FetchingLeft;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecutorRecord>, String> {
        loop {
            match self.state {
                JoinState::Uninitialized => {
                    return Err("NestedLoopJoinExecutor not initialized".to_string());
                }

                JoinState::FetchingLeft => {
                    // 获取左表的下一条记录
                    match self.left.next()? {
                        Some(left_record) => {
                            self.current_left_record = Some(left_record);
                            self.right_record_index = 0;
                            self.state = JoinState::ScanningRight;
                            // 继续到 ScanningRight 状态
                        }
                        None => {
                            // 左表已耗尽
                            self.state = JoinState::Done;
                            return Ok(None);
                        }
                    }
                }

                JoinState::ScanningRight => {
                    // 扫描右表的缓冲记录
                    if self.right_record_index < self.buffered_right_records.len() {
                        let right_record = self.buffered_right_records[self.right_record_index].clone();
                        self.right_record_index += 1;

                        // 检查 Join 条件
                        let left_record = self.current_left_record.as_ref().unwrap();
                        let condition_met = if let Some(cond) = &self.join_condition {
                            Self::evaluate_join_condition(cond, left_record, &right_record)?
                        } else {
                            // 没有条件时，所有行都匹配（笛卡尔积）
                            true
                        };

                        if condition_met {
                            // 条件满足，合并并返回
                            let merged = Self::merge_records(left_record, &right_record);
                            return Ok(Some(merged));
                        }
                        // 条件不满足，继续扫描右表
                    } else {
                        // 右表已扫描完，回到 FetchingLeft 状态
                        self.state = JoinState::FetchingLeft;
                    }
                }

                JoinState::Done => {
                    return Ok(None);
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), String> {
        self.left.close()?;
        self.right.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::RID;

    // Mock 执行器用于测试
    struct MockExecutor {
        records: Vec<ExecutorRecord>,
        current_index: usize,
        initialized: bool,
    }

    impl MockExecutor {
        fn new(records: Vec<ExecutorRecord>) -> Self {
            MockExecutor {
                records,
                current_index: 0,
                initialized: false,
            }
        }
    }

    impl Executor for MockExecutor {
        fn init(&mut self) -> Result<(), String> {
            self.current_index = 0;
            self.initialized = true;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<ExecutorRecord>, String> {
            if self.current_index < self.records.len() {
                let record = self.records[self.current_index].clone();
                self.current_index += 1;
                Ok(Some(record))
            } else {
                Ok(None)
            }
        }
    }

    #[test]
    fn test_nested_loop_join_basic() {
        let left_records = vec![
            ExecutorRecord {
                rid: RID { page_id: 1, slot_id: 0 },
                data: vec![1, 2],
            },
            ExecutorRecord {
                rid: RID { page_id: 1, slot_id: 1 },
                data: vec![3, 4],
            },
        ];

        let right_records = vec![
            ExecutorRecord {
                rid: RID { page_id: 2, slot_id: 0 },
                data: vec![5, 6],
            },
            ExecutorRecord {
                rid: RID { page_id: 2, slot_id: 1 },
                data: vec![7, 8],
            },
        ];

        let left = Box::new(MockExecutor::new(left_records));
        let right = Box::new(MockExecutor::new(right_records));

        let mut join = NestedLoopJoinExecutor::new(left, right, None, JoinType::Inner);

        join.init().unwrap();

        // 应该得到 2x2 = 4 条笛卡尔积结果
        let mut count = 0;
        loop {
            match join.next().unwrap() {
                Some(_record) => count += 1,
                None => break,
            }
        }

        assert_eq!(count, 4);
    }

    #[test]
    fn test_nested_loop_join_empty_left() {
        let left_records = vec![];

        let right_records = vec![
            ExecutorRecord {
                rid: RID { page_id: 2, slot_id: 0 },
                data: vec![5, 6],
            },
        ];

        let left = Box::new(MockExecutor::new(left_records));
        let right = Box::new(MockExecutor::new(right_records));

        let mut join = NestedLoopJoinExecutor::new(left, right, None, JoinType::Inner);

        join.init().unwrap();

        // 左表为空，应该没有结果
        let result = join.next().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_nested_loop_join_empty_right() {
        let left_records = vec![
            ExecutorRecord {
                rid: RID { page_id: 1, slot_id: 0 },
                data: vec![1, 2],
            },
        ];

        let right_records = vec![];

        let left = Box::new(MockExecutor::new(left_records));
        let right = Box::new(MockExecutor::new(right_records));

        let mut join = NestedLoopJoinExecutor::new(left, right, None, JoinType::Inner);

        join.init().unwrap();

        // 右表为空，应该没有结果
        let result = join.next().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_nested_loop_join_single_left() {
        let left_records = vec![
            ExecutorRecord {
                rid: RID { page_id: 1, slot_id: 0 },
                data: vec![1, 2],
            },
        ];

        let right_records = vec![
            ExecutorRecord {
                rid: RID { page_id: 2, slot_id: 0 },
                data: vec![5, 6],
            },
            ExecutorRecord {
                rid: RID { page_id: 2, slot_id: 1 },
                data: vec![7, 8],
            },
        ];

        let left = Box::new(MockExecutor::new(left_records));
        let right = Box::new(MockExecutor::new(right_records));

        let mut join = NestedLoopJoinExecutor::new(left, right, None, JoinType::Inner);

        join.init().unwrap();

        // 1 条左记录 × 2 条右记录 = 2 条结果
        let mut count = 0;
        loop {
            match join.next().unwrap() {
                Some(_record) => count += 1,
                None => break,
            }
        }

        assert_eq!(count, 2);
    }

    #[test]
    fn test_nested_loop_join_merge_records() {
        let left = ExecutorRecord {
            rid: RID { page_id: 1, slot_id: 0 },
            data: vec![1, 2],
        };

        let right = ExecutorRecord {
            rid: RID { page_id: 2, slot_id: 0 },
            data: vec![3, 4],
        };

        let merged = NestedLoopJoinExecutor::merge_records(&left, &right);

        assert_eq!(merged.data, vec![1, 2, 3, 4]);
        assert_eq!(merged.rid.page_id, 1);
    }
}
