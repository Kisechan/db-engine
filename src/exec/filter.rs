// 过滤执行器 (Filter Executor)
// 
// 对输入的记录进行条件过滤，只返回满足条件的记录。

use crate::exec::iterator::{Executor, ExecutorRecord};
use crate::sql::ast::Expression;

// 过滤执行器
// 
// 对来自子执行器的每条记录应用谓词，只返回满足条件的记录。
pub struct FilterExecutor {
    child: Box<dyn Executor>,
    predicate: Expression,
    is_initialized: bool,
}

impl FilterExecutor {
    // 创建新的过滤执行器
    pub fn new(child: Box<dyn Executor>, predicate: Expression) -> Self {
        FilterExecutor {
            child,
            predicate,
            is_initialized: false,
        }
    }

    // 评估谓词是否匹配记录
    // 
    // 简化实现：暂时只支持基本的列引用和常量
    // 实际中需要完整的表达式求值引擎
    fn evaluate_predicate(predicate: &Expression, _record: &ExecutorRecord) -> Result<bool, String> {
        match predicate {
            // 暂时简化：对于常量表达式，直接返回 true
            // 对于实际应用，需要根据 record 的内容求值
            Expression::Literal(_) => Ok(true),
            Expression::Column(_) => Ok(true),
            Expression::BinaryOp { .. } => Ok(true),
            Expression::UnaryOp { .. } => Ok(true),
            Expression::Parenthesized(expr) => {
                Self::evaluate_predicate(expr, _record)
            }
        }
    }
}

impl Executor for FilterExecutor {
    fn init(&mut self) -> Result<(), String> {
        self.child.init()?;
        self.is_initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecutorRecord>, String> {
        if !self.is_initialized {
            return Err("FilterExecutor not initialized".to_string());
        }

        // 逐条获取记录，直到找到满足条件的或到达末尾
        loop {
            match self.child.next()? {
                Some(record) => {
                    // 评估谓词
                    if Self::evaluate_predicate(&self.predicate, &record)? {
                        return Ok(Some(record));
                    }
                    // 不满足条件，继续获取下一条
                }
                None => {
                    // 子执行器已耗尽
                    return Ok(None);
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), String> {
        self.child.close()?;
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
    }

    impl MockExecutor {
        fn new(records: Vec<ExecutorRecord>) -> Self {
            MockExecutor {
                records,
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
    fn test_filter_executor() {
        let mock_records = vec![
            ExecutorRecord {
                rid: RID { page_id: 1, slot_id: 0 },
                data: vec![1, 2, 3],
            },
            ExecutorRecord {
                rid: RID { page_id: 1, slot_id: 1 },
                data: vec![4, 5, 6],
            },
        ];

        let mock = Box::new(MockExecutor::new(mock_records));
        let mut filter = FilterExecutor::new(
            mock,
            Expression::Literal(crate::sql::ast::Literal::Integer(1)),
        );

        filter.init().unwrap();
        let rec1 = filter.next().unwrap();
        assert!(rec1.is_some());
    }
}
