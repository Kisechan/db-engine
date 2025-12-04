// 物理计划生成器 (Physical Planner)
// 
// 将优化后的逻辑计划树转换为物理执行器树。

use crate::plan::logical::{LogicalPlan, JoinType};
use crate::exec::iterator::ExecutorBox;
use crate::exec::scan::SeqScanExecutor;
use crate::exec::filter::FilterExecutor;
use crate::exec::join::NestedLoopJoinExecutor;
use crate::rm::table_manager::TableManager;
use crate::sql::ast::Expression;

// 物理规划错误
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlannerError {
    // 表不存在或无法打开
    TableNotFound(String),
    // 其他错误
    Other(String),
}

impl std::fmt::Display for PhysicalPlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PhysicalPlannerError::TableNotFound(name) => {
                write!(f, "Table '{}' not found", name)
            }
            PhysicalPlannerError::Other(msg) => {
                write!(f, "Physical planner error: {}", msg)
            }
        }
    }
}

// 物理规划器
pub struct PhysicalPlanner {
    table_manager: TableManager,
}

impl PhysicalPlanner {
    // 创建新的物理规划器
    pub fn new(table_manager: TableManager) -> Self {
        PhysicalPlanner { table_manager }
    }

    // 将逻辑计划转换为物理执行器树
    pub fn plan(&mut self, logical_plan: LogicalPlan) -> Result<ExecutorBox, PhysicalPlannerError> {
        self.convert_to_executor(logical_plan)
    }

    // 递归将逻辑节点转换为物理执行器
    fn convert_to_executor(
        &mut self,
        plan: LogicalPlan,
    ) -> Result<ExecutorBox, PhysicalPlannerError> {
        match plan {
            // 扫描节点 -> SeqScanExecutor
            LogicalPlan::Scan { table_name } => {
                self.create_seq_scan_executor(&table_name)
            }

            // 过滤节点 -> FilterExecutor(child)
            LogicalPlan::Filter { child, predicate } => {
                let child_executor = self.convert_to_executor(*child)?;
                Ok(Box::new(FilterExecutor::new(child_executor, predicate)))
            }

            // 投影节点 -> 暂时跳过投影，只处理子树
            // （实际中应该实现 ProjectionExecutor）
            LogicalPlan::Project { child, columns: _ } => {
                self.convert_to_executor(*child)
            }

            // Join 节点 -> NestedLoopJoinExecutor
            LogicalPlan::Join {
                left,
                right,
                on_condition,
                join_type,
            } => {
                let left_executor = self.convert_to_executor(*left)?;
                let right_executor = self.convert_to_executor(*right)?;

                Ok(Box::new(NestedLoopJoinExecutor::new(
                    left_executor,
                    right_executor,
                    on_condition,
                    join_type,
                )))
            }
        }
    }

    // 创建全表扫描执行器
    fn create_seq_scan_executor(
        &mut self,
        table_name: &str,
    ) -> Result<ExecutorBox, PhysicalPlannerError> {
        self.table_manager
            .open_table(table_name)
            .map_err(|e| PhysicalPlannerError::Other(e))?;

        let table_handler = self.table_manager
            .get_table_handler_mut(table_name)
            .ok_or_else(|| PhysicalPlannerError::TableNotFound(table_name.to_string()))?;

        Ok(Box::new(SeqScanExecutor::new(
            table_name.to_string(),
            table_handler.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rm::catalog_manager::CatalogManager;
    use crate::rm::types::{TableSchema, ColumnDef, DataType};

    #[test]
    fn test_physical_planner_simple_scan() {
        // 创建测试用的 table_manager
        let catalog = CatalogManager::default();
        let table_manager = TableManager::new(catalog, std::path::PathBuf::from(".")).expect("Failed to create table_manager");

        let mut planner = PhysicalPlanner::new(table_manager);

        // 创建简单的 Scan 逻辑计划
        let logical_plan = LogicalPlan::Scan {
            table_name: "test_table".to_string(),
        };

        // 由于测试环境中表不存在，预期会失败
        let result = planner.plan(logical_plan);
        assert!(result.is_err());
    }

    #[test]
    fn test_physical_planner_with_filter() {
        let catalog = CatalogManager::default();
        let table_manager = TableManager::new(catalog, std::path::PathBuf::from(".")).expect("Failed to create table_manager");

        let mut planner = PhysicalPlanner::new(table_manager);

        // 创建包含 Filter 的逻辑计划
        let logical_plan = LogicalPlan::Filter {
            child: Box::new(LogicalPlan::Scan {
                table_name: "test_table".to_string(),
            }),
            predicate: Expression::Column("age".to_string()),
        };

        let result = planner.plan(logical_plan);
        assert!(result.is_err());
    }

    #[test]
    fn test_physical_planner_with_join() {
        let catalog = CatalogManager::default();
        let table_manager = TableManager::new(catalog, std::path::PathBuf::from(".")).expect("Failed to create table_manager");

        let mut planner = PhysicalPlanner::new(table_manager);

        // 创建包含 Join 的逻辑计划
        let logical_plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table_name: "users".to_string(),
            }),
            right: Box::new(LogicalPlan::Scan {
                table_name: "orders".to_string(),
            }),
            on_condition: None,
            join_type: JoinType::Inner,
        };

        let result = planner.plan(logical_plan);
        assert!(result.is_err()); // 表不存在
    }
}
