// 查询优化器 (Query Optimizer)
// 
// 对逻辑计划进行优化，目前支持：
// - 谓词下推 (Predicate Pushdown)

use crate::plan::logical::{LogicalPlan, JoinType};
use crate::sql::ast::{Expression, BinaryOperator};
use std::collections::HashSet;

// 查询优化器
pub struct Optimizer;

impl Optimizer {
    // 优化逻辑计划
    pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
        log::debug!("Optimizing logical plan: {:?}", plan);
        
        // 先进行谓词下推
        let pushed = Self::pushdown_predicates(plan);
        
        log::debug!("Optimized plan: {:?}", pushed);
        
        // 未来可以添加其他优化：
        // - 列投影下推
        // - 常量折叠
        // - 死代码消除等
        
        pushed
    }

    // 谓词下推优化
    // 
    // 将 Filter 条件尽可能下推到靠近数据源的位置，
    // 以减少参与后续操作的数据量。
    fn pushdown_predicates(plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // Filter 节点需要特殊处理
            LogicalPlan::Filter { child, predicate } => {
                Self::handle_filter(*child, predicate)
            }
            
            // Project 节点直接递归处理子树
            LogicalPlan::Project { child, columns } => {
                LogicalPlan::Project {
                    child: Box::new(Self::pushdown_predicates(*child)),
                    columns,
                }
            }
            
            // Join 节点直接递归处理两个子树
            LogicalPlan::Join { left, right, on_condition, join_type } => {
                LogicalPlan::Join {
                    left: Box::new(Self::pushdown_predicates(*left)),
                    right: Box::new(Self::pushdown_predicates(*right)),
                    on_condition,
                    join_type,
                }
            }
            
            // Scan 节点无需处理
            other => other,
        }
    }

    // 处理 Filter 节点
    // 
    // 尝试将 Filter 条件下推到子树中
    fn handle_filter(child: LogicalPlan, predicate: Expression) -> LogicalPlan {
        match child {
            // Filter 在 Filter 上方：合并为一个 AND 条件
            LogicalPlan::Filter { child: inner_child, predicate: inner_predicate } => {
                let combined = Expression::BinaryOp {
                    left: Box::new(inner_predicate),
                    op: BinaryOperator::And,
                    right: Box::new(predicate),
                };
                Self::pushdown_predicates(LogicalPlan::Filter {
                    child: inner_child,
                    predicate: combined,
                })
            }
            
            // Filter 在 Join 上方：分析谓词并决定是否下推
            LogicalPlan::Join { left, right, on_condition, join_type } => {
                Self::pushdown_filter_over_join(
                    predicate,
                    *left,
                    *right,
                    on_condition,
                    join_type,
                )
            }
            
            // Filter 在 Project 上方：直接下推到 Project 下面
            LogicalPlan::Project { child, columns } => {
                LogicalPlan::Project {
                    child: Box::new(Self::pushdown_predicates(LogicalPlan::Filter {
                        child,
                        predicate,
                    })),
                    columns,
                }
            }
            
            // Filter 在 Scan 上方：无法继续下推
            other => LogicalPlan::Filter {
                child: Box::new(other),
                predicate,
            },
        }
    }

    // 在 Join 上方下推 Filter
    // 
    // 分析谓词涉及的表字段，决定如何分布过滤条件
    fn pushdown_filter_over_join(
        predicate: Expression,
        left: LogicalPlan,
        right: LogicalPlan,
        on_condition: Option<Expression>,
        join_type: JoinType,
    ) -> LogicalPlan {
        // 获取 Join 两侧的表名
        let left_tables = Self::extract_table_names(&left);
        let right_tables = Self::extract_table_names(&right);

        // 提取谓词中涉及的列名
        let predicate_columns = Self::extract_columns_from_expression(&predicate);

        // 判断谓词涉及的表
        let involves_left = predicate_columns
            .iter()
            .any(|col| Self::column_belongs_to_table(col, &left_tables));
        let involves_right = predicate_columns
            .iter()
            .any(|col| Self::column_belongs_to_table(col, &right_tables));

        match (involves_left, involves_right) {
            // 谓词只涉及左表：下推到左子树
            (true, false) => {
                LogicalPlan::Join {
                    left: Box::new(Self::pushdown_predicates(LogicalPlan::Filter {
                        child: Box::new(left),
                        predicate,
                    })),
                    right: Box::new(Self::pushdown_predicates(right)),
                    on_condition,
                    join_type,
                }
            }

            // 谓词只涉及右表：下推到右子树
            (false, true) => {
                LogicalPlan::Join {
                    left: Box::new(Self::pushdown_predicates(left)),
                    right: Box::new(Self::pushdown_predicates(LogicalPlan::Filter {
                        child: Box::new(right),
                        predicate,
                    })),
                    on_condition,
                    join_type,
                }
            }

            // 谓词涉及两个表或都不涉及：
            // - INNER JOIN：可以作为联接条件
            // - OUTER JOIN：必须保留在 Join 上方
            (true, true) | (false, false) => {
                match join_type {
                    JoinType::Inner => {
                        // INNER JOIN 可以将条件与 on_condition 合并
                        let new_condition = match on_condition {
                            Some(cond) => Expression::BinaryOp {
                                left: Box::new(cond),
                                op: BinaryOperator::And,
                                right: Box::new(predicate),
                            },
                            None => predicate,
                        };

                        LogicalPlan::Join {
                            left: Box::new(Self::pushdown_predicates(left)),
                            right: Box::new(Self::pushdown_predicates(right)),
                            on_condition: Some(new_condition),
                            join_type,
                        }
                    }
                    _ => {
                        // OUTER JOIN 必须保留 Filter 在上方
                        LogicalPlan::Filter {
                            child: Box::new(LogicalPlan::Join {
                                left: Box::new(Self::pushdown_predicates(left)),
                                right: Box::new(Self::pushdown_predicates(right)),
                                on_condition,
                                join_type,
                            }),
                            predicate,
                        }
                    }
                }
            }
        }
    }

    // 从逻辑计划中提取所有表名
    fn extract_table_names(plan: &LogicalPlan) -> HashSet<String> {
        match plan {
            LogicalPlan::Scan { table_name } => {
                let mut set = HashSet::new();
                set.insert(table_name.clone());
                set
            }
            LogicalPlan::Filter { child, .. } => Self::extract_table_names(child),
            LogicalPlan::Project { child, .. } => Self::extract_table_names(child),
            LogicalPlan::Join { left, right, .. } => {
                let mut left_tables = Self::extract_table_names(left);
                left_tables.extend(Self::extract_table_names(right));
                left_tables
            }
        }
    }

    // 从表达式中提取所有列名
    fn extract_columns_from_expression(expr: &Expression) -> HashSet<String> {
        match expr {
            Expression::Column(name) => {
                let mut set = HashSet::new();
                set.insert(name.clone());
                set
            }
            Expression::Literal(_) => HashSet::new(),
            Expression::BinaryOp { left, right, .. } => {
                let mut left_cols = Self::extract_columns_from_expression(left);
                left_cols.extend(Self::extract_columns_from_expression(right));
                left_cols
            }
            Expression::UnaryOp { expr, .. } => {
                Self::extract_columns_from_expression(expr)
            }
            Expression::Parenthesized(expr) => {
                Self::extract_columns_from_expression(expr)
            }
        }
    }

    // 判断列名是否属于某个表
    // 
    // 对于简化版本，如果列名中没有表前缀，假设它属于任何表
    // （实际中应该通过元数据表查询）
    fn column_belongs_to_table(column: &str, tables: &HashSet<String>) -> bool {
        // 如果列名包含 '.'，检查前缀
        if let Some(dot_idx) = column.find('.') {
            let table_prefix = &column[..dot_idx];
            tables.contains(table_prefix)
        } else {
            // 如果没有前缀，假设属于该表集合（简化处理）
            !tables.is_empty()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{BinaryOperator, Literal};

    #[test]
    fn test_optimize_simple_filter() {
        // 构造：Filter(Scan)
        let plan = LogicalPlan::Filter {
            child: Box::new(LogicalPlan::Scan {
                table_name: "users".to_string(),
            }),
            predicate: Expression::Column("age".to_string()),
        };

        let optimized = Optimizer::optimize(plan);
        // 结构应保持不变
        assert_eq!(optimized.node_type(), "Filter");
    }

    #[test]
    fn test_optimize_filter_over_filter() {
        // 构造：Filter(Filter(Scan))
        let plan = LogicalPlan::Filter {
            child: Box::new(LogicalPlan::Filter {
                child: Box::new(LogicalPlan::Scan {
                    table_name: "users".to_string(),
                }),
                predicate: Expression::Column("age".to_string()),
            }),
            predicate: Expression::Column("status".to_string()),
        };

        let optimized = Optimizer::optimize(plan);
        // 应该合并为一个 Filter，谓词使用 AND 连接
        assert_eq!(optimized.node_type(), "Filter");
    }

    #[test]
    fn test_pushdown_filter_left_only() {
        // 构造：Filter(Join(Scan(users), Scan(orders)))
        // 其中 Filter 条件只涉及 users 表
        let plan = LogicalPlan::Filter {
            child: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::Scan {
                    table_name: "users".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table_name: "orders".to_string(),
                }),
                on_condition: None,
                join_type: JoinType::Inner,
            }),
            predicate: Expression::Column("users.age".to_string()),
        };

        let optimized = Optimizer::optimize(plan);
        // 优化后应该是：Join(Filter(Scan(users)), Scan(orders))
        assert_eq!(optimized.node_type(), "Join");
    }

    #[test]
    fn test_pushdown_filter_right_only() {
        // 构造：Filter(Join(Scan(users), Scan(orders)))
        // 其中 Filter 条件只涉及 orders 表
        let plan = LogicalPlan::Filter {
            child: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::Scan {
                    table_name: "users".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table_name: "orders".to_string(),
                }),
                on_condition: None,
                join_type: JoinType::Inner,
            }),
            predicate: Expression::Column("orders.total".to_string()),
        };

        let optimized = Optimizer::optimize(plan);
        // 优化后应该是：Join(Scan(users), Filter(Scan(orders)))
        assert_eq!(optimized.node_type(), "Join");
    }

    #[test]
    fn test_pushdown_filter_both_tables() {
        // 构造：Filter(Join(Scan(users), Scan(orders)))
        // 其中 Filter 条件涉及两个表
        let plan = LogicalPlan::Filter {
            child: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::Scan {
                    table_name: "users".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table_name: "orders".to_string(),
                }),
                on_condition: None,
                join_type: JoinType::Inner,
            }),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column("users.id".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::Column("orders.user_id".to_string())),
            },
        };

        let optimized = Optimizer::optimize(plan);
        // INNER JOIN 可以将条件作为 on_condition
        assert_eq!(optimized.node_type(), "Join");
    }

    #[test]
    fn test_pushdown_filter_over_project() {
        // 构造：Filter(Project(Scan))
        let plan = LogicalPlan::Filter {
            child: Box::new(LogicalPlan::Project {
                child: Box::new(LogicalPlan::Scan {
                    table_name: "users".to_string(),
                }),
                columns: vec!["id".to_string(), "name".to_string()],
            }),
            predicate: Expression::Column("age".to_string()),
        };

        let optimized = Optimizer::optimize(plan);
        // 应该下推到 Project 下面
        assert_eq!(optimized.node_type(), "Project");
    }

    #[test]
    fn test_extract_table_names() {
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table_name: "users".to_string(),
            }),
            right: Box::new(LogicalPlan::Scan {
                table_name: "orders".to_string(),
            }),
            on_condition: None,
            join_type: JoinType::Inner,
        };

        let tables = Optimizer::extract_table_names(&plan);
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
    }

    #[test]
    fn test_extract_columns_from_expression() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("age".to_string())),
            op: BinaryOperator::And,
            right: Box::new(Expression::Column("status".to_string())),
        };

        let cols = Optimizer::extract_columns_from_expression(&expr);
        assert_eq!(cols.len(), 2);
        assert!(cols.contains("age"));
        assert!(cols.contains("status"));
    }

    #[test]
    fn test_column_belongs_to_table() {
        let mut tables = HashSet::new();
        tables.insert("users".to_string());

        // 带前缀的列
        assert!(Optimizer::column_belongs_to_table("users.age", &tables));
        assert!(!Optimizer::column_belongs_to_table("orders.id", &tables));

        // 不带前缀的列
        assert!(Optimizer::column_belongs_to_table("age", &tables));
    }

    #[test]
    fn test_outer_join_filter_not_pushed() {
        // 构造：Filter(LeftJoin(...))，条件涉及两个表
        let plan = LogicalPlan::Filter {
            child: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::Scan {
                    table_name: "users".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table_name: "orders".to_string(),
                }),
                on_condition: None,
                join_type: JoinType::Left,
            }),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column("users.id".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::Column("orders.user_id".to_string())),
            },
        };

        let optimized = Optimizer::optimize(plan);
        // LEFT JOIN 不能下推涉及两个表的条件
        assert_eq!(optimized.node_type(), "Filter");
    }
}
