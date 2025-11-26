// 逻辑计划 (Logical Plan) 定义
// 
// 逻辑计划树表示查询的高级结构，由 AST 经过 Binding 得到。
// 每个计划节点代表一个算子，形成树状结构。

use crate::sql::ast::Expression;
use std::fmt;

// 逻辑计划树的一个节点
// 使用递归枚举表示不同类型的算子
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    // 全表扫描算子
    // - table_name: 被扫描的表名
    Scan {
        table_name: String,
    },

    // 过滤（WHERE）算子
    // - child: 输入的计划节点
    // - predicate: 过滤条件（表达式）
    Filter {
        child: Box<LogicalPlan>,
        predicate: Expression,
    },

    // 投影（SELECT）算子
    // - child: 输入的计划节点
    // - columns: 投影的列名列表
    Project {
        child: Box<LogicalPlan>,
        columns: Vec<String>,
    },

    // 联接（JOIN）算子
    // - left: 左表的计划
    // - right: 右表的计划
    // - on_condition: 联接条件
    // - join_type: 联接类型（INNER, LEFT, RIGHT, FULL）
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on_condition: Option<Expression>,
        join_type: JoinType,
    },
}

// 联接类型枚举
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER JOIN"),
            JoinType::Left => write!(f, "LEFT JOIN"),
            JoinType::Right => write!(f, "RIGHT JOIN"),
            JoinType::Full => write!(f, "FULL JOIN"),
        }
    }
}

impl LogicalPlan {
    // 获取当前计划节点的类型名称（用于调试）
    pub fn node_type(&self) -> &'static str {
        match self {
            LogicalPlan::Scan { .. } => "Scan",
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Project { .. } => "Project",
            LogicalPlan::Join { .. } => "Join",
        }
    }

    // 计算逻辑计划树的高度
    pub fn height(&self) -> usize {
        match self {
            LogicalPlan::Scan { .. } => 1,
            LogicalPlan::Filter { child, .. } => 1 + child.height(),
            LogicalPlan::Project { child, .. } => 1 + child.height(),
            LogicalPlan::Join { left, right, .. } => 1 + std::cmp::max(left.height(), right.height()),
        }
    }

    // 获取计划树中所有出现的表名
    pub fn table_names(&self) -> Vec<String> {
        match self {
            LogicalPlan::Scan { table_name } => vec![table_name.clone()],
            LogicalPlan::Filter { child, .. } => child.table_names(),
            LogicalPlan::Project { child, .. } => child.table_names(),
            LogicalPlan::Join { left, right, .. } => {
                let mut names = left.table_names();
                names.extend(right.table_names());
                names.sort();
                names.dedup();
                names
            }
        }
    }

    // 格式化输出计划树（用于调试）
    pub fn format_tree(&self, indent: usize) -> String {
        let prefix = " ".repeat(indent);
        match self {
            LogicalPlan::Scan { table_name } => {
                format!("{}Scan({})", prefix, table_name)
            }
            LogicalPlan::Filter { child, predicate } => {
                format!(
                    "{}Filter(predicate: {:?})\n{}",
                    prefix,
                    predicate,
                    child.format_tree(indent + 2)
                )
            }
            LogicalPlan::Project { child, columns } => {
                format!(
                    "{}Project(columns: [{}])\n{}",
                    prefix,
                    columns.join(", "),
                    child.format_tree(indent + 2)
                )
            }
            LogicalPlan::Join {
                left,
                right,
                on_condition,
                join_type,
            } => {
                let cond_str = match on_condition {
                    Some(cond) => format!("on: {:?}", cond),
                    None => "on: <no condition>".to_string(),
                };
                format!(
                    "{}{} ({})\n{}\n{}",
                    prefix,
                    join_type,
                    cond_str,
                    left.format_tree(indent + 2),
                    right.format_tree(indent + 2)
                )
            }
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.format_tree(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logical_plan_scan() {
        let plan = LogicalPlan::Scan {
            table_name: "users".to_string(),
        };
        assert_eq!(plan.node_type(), "Scan");
        assert_eq!(plan.height(), 1);
        assert_eq!(plan.table_names(), vec!["users".to_string()]);
    }

    #[test]
    fn test_logical_plan_height() {
        let scan = Box::new(LogicalPlan::Scan {
            table_name: "users".to_string(),
        });
        let filter = LogicalPlan::Filter {
            child: scan,
            predicate: Expression::Column("age".to_string()),
        };
        assert_eq!(filter.height(), 2);
    }

    #[test]
    fn test_logical_plan_table_names() {
        let left = Box::new(LogicalPlan::Scan {
            table_name: "users".to_string(),
        });
        let right = Box::new(LogicalPlan::Scan {
            table_name: "orders".to_string(),
        });
        let join = LogicalPlan::Join {
            left,
            right,
            on_condition: None,
            join_type: JoinType::Inner,
        };
        let mut names = join.table_names();
        names.sort();
        assert_eq!(names, vec!["orders".to_string(), "users".to_string()]);
    }

    #[test]
    fn test_logical_plan_format_tree() {
        let plan = LogicalPlan::Scan {
            table_name: "users".to_string(),
        };
        let output = plan.format_tree(0);
        assert!(output.contains("Scan(users)"));
    }
}
