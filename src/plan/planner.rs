// 简单规划器 (Simple Planner)
// 
// 将 SQL AST 转换为逻辑计划树。
// 过程中使用元数据表检查表和列的存在性（Binding）。

use crate::plan::logical::LogicalPlan;
use crate::sql::ast::{SelectStmt, SelectField, Expression};
use crate::rm::catalog_manager::CatalogManager;

// 规划错误
#[derive(Debug, Clone, PartialEq)]
pub enum PlannerError {
    // 表不存在
    TableNotFound(String),
    // 列不存在
    ColumnNotFound(String, String), // (column_name, table_name)
    // 无效的列引用
    InvalidColumnRef(String),
    // 没有 FROM 子句
    NoFromClause,
    // SELECT 字段为空
    EmptySelectFields,
}

impl std::fmt::Display for PlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PlannerError::TableNotFound(name) => {
                write!(f, "Table '{}' not found", name)
            }
            PlannerError::ColumnNotFound(col, table) => {
                write!(f, "Column '{}' not found in table '{}'", col, table)
            }
            PlannerError::InvalidColumnRef(col) => {
                write!(f, "Invalid column reference: '{}'", col)
            }
            PlannerError::NoFromClause => {
                write!(f, "SELECT statement must have a FROM clause")
            }
            PlannerError::EmptySelectFields => {
                write!(f, "SELECT must specify at least one field")
            }
        }
    }
}

// 简单规划器
pub struct Planner {
    catalog: CatalogManager,
}

impl Planner {
    // 创建新的规划器
    pub fn new(catalog: CatalogManager) -> Self {
        Planner { catalog }
    }

    // 将 SelectStmt 转换为逻辑计划树
    pub fn plan_select(&self, stmt: &SelectStmt) -> Result<LogicalPlan, PlannerError> {
        log::debug!("Planning SELECT statement");
        
        // 检查 SELECT 字段是否为空
        if stmt.fields.is_empty() {
            return Err(PlannerError::EmptySelectFields);
        }

        // 检查是否有 FROM 子句
        let table_name = stmt
            .from_table
            .as_ref()
            .ok_or(PlannerError::NoFromClause)?
            .clone();

        // Binding: 验证表是否存在
        self.verify_table_exists(&table_name)?;

        // 构建基础扫描算子
        let mut plan = LogicalPlan::Scan { table_name };

        // 如果有 WHERE 子句，添加过滤算子
        if let Some(where_clause) = &stmt.where_clause {
            plan = LogicalPlan::Filter {
                child: Box::new(plan),
                predicate: where_clause.condition.clone(),
            };
        }

        // 添加投影算子
        let columns = self.extract_projection_columns(stmt, &stmt.from_table.as_ref().unwrap())?;
        plan = LogicalPlan::Project {
            child: Box::new(plan),
            columns,
        };

        log::debug!("Generated logical plan: {:?}", plan);
        Ok(plan)
    }

    // 验证表是否存在
    fn verify_table_exists(&self, table_name: &str) -> Result<(), PlannerError> {
        if !self.catalog.table_exists(table_name) {
            return Err(PlannerError::TableNotFound(table_name.to_string()));
        }
        Ok(())
    }

    // 验证列是否存在于表中
    fn verify_column_exists(&self, table_name: &str, column_name: &str) -> Result<(), PlannerError> {
        let schema = self.catalog
            .get_table_schema(table_name)
            .map_err(|_| PlannerError::TableNotFound(table_name.to_string()))?;

        // 检查列是否存在
        let exists = schema.columns.iter().any(|col| col.name == column_name);
        if !exists {
            return Err(PlannerError::ColumnNotFound(
                column_name.to_string(),
                table_name.to_string(),
            ));
        }

        Ok(())
    }

    // 从 SELECT 语句中提取投影列
    fn extract_projection_columns(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
    ) -> Result<Vec<String>, PlannerError> {
        let mut columns = Vec::new();

        for field in &stmt.fields {
            match field {
                SelectField::All => {
                    // SELECT * 的情况，获取表的所有列
                    let schema = self.catalog
                        .get_table_schema(table_name)
                        .map_err(|_| PlannerError::TableNotFound(table_name.to_string()))?;

                    columns.extend(schema.columns.iter().map(|col| col.name.clone()));
                }
                SelectField::Column(col_name) => {
                    // 验证列是否存在
                    self.verify_column_exists(table_name, col_name)?;
                    columns.push(col_name.clone());
                }
                SelectField::Expression(_expr_str) => {
                    // 简化处理：暂时将表达式列作为 "expr" 返回
                    columns.push("expr".to_string());
                }
            }
        }

        if columns.is_empty() {
            return Err(PlannerError::EmptySelectFields);
        }

        Ok(columns)
    }

    // 从 WHERE 条件表达式中提取所有列名（用于验证）
    fn extract_columns_from_expression(&self, expr: &Expression) -> Vec<String> {
        match expr {
            Expression::Column(name) => vec![name.clone()],
            Expression::Literal(_) => vec![],
            Expression::BinaryOp { left, right, .. } => {
                let mut cols = self.extract_columns_from_expression(left);
                cols.extend(self.extract_columns_from_expression(right));
                cols
            }
            Expression::UnaryOp { expr, .. } => self.extract_columns_from_expression(expr),
            Expression::Parenthesized(expr) => self.extract_columns_from_expression(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{WhereClause, BinaryOperator, Literal};
    use crate::common::types::{TableSchema, ColumnDef, DataType};

    // 创建测试用的 CatalogManager
    fn create_test_catalog() -> CatalogManager {
        let mut catalog = CatalogManager::default();

        // 添加 users 表
        let users_schema = TableSchema {
            table_name: "users".to_string(),
            table_id: 1,
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                    nullable: true,
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                },
            ],
            root_pages: vec![],
            create_time: 0,
            row_count: 0,
            last_modified: 0,
        };
        let _ = catalog.create_table(users_schema);

        // 添加 orders 表
        let orders_schema = TableSchema {
            table_name: "orders".to_string(),
            table_id: 2,
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnDef {
                    name: "user_id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnDef {
                    name: "total".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                },
            ],
            root_pages: vec![],
            create_time: 0,
            row_count: 0,
            last_modified: 0,
        };
        let _ = catalog.create_table(orders_schema);

        catalog
    }

    #[test]
    fn test_plan_simple_select() {
        let catalog = create_test_catalog();
        let planner = Planner::new(catalog);

        let stmt = SelectStmt {
            distinct: false,
            fields: vec![SelectField::All],
            from_table: Some("users".to_string()),
            where_clause: None,
            group_by: None,
            order_by: None,
            limit: None,
        };

        let plan = planner.plan_select(&stmt).unwrap();
        assert_eq!(plan.node_type(), "Project");
        assert_eq!(plan.height(), 2);
    }

    #[test]
    fn test_plan_select_with_columns() {
        let catalog = create_test_catalog();
        let planner = Planner::new(catalog);

        let stmt = SelectStmt {
            distinct: false,
            fields: vec![
                SelectField::Column("id".to_string()),
                SelectField::Column("name".to_string()),
            ],
            from_table: Some("users".to_string()),
            where_clause: None,
            group_by: None,
            order_by: None,
            limit: None,
        };

        let plan = planner.plan_select(&stmt).unwrap();
        assert_eq!(plan.node_type(), "Project");
    }

    #[test]
    fn test_plan_select_with_where() {
        let catalog = create_test_catalog();
        let planner = Planner::new(catalog);

        let stmt = SelectStmt {
            distinct: false,
            fields: vec![SelectField::All],
            from_table: Some("users".to_string()),
            where_clause: Some(WhereClause {
                condition: Expression::BinaryOp {
                    left: Box::new(Expression::Column("age".to_string())),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expression::Literal(Literal::Integer(18))),
                },
            }),
            group_by: None,
            order_by: None,
            limit: None,
        };

        let plan = planner.plan_select(&stmt).unwrap();
        assert_eq!(plan.node_type(), "Project");
        assert_eq!(plan.height(), 3); // Project -> Filter -> Scan
    }

    #[test]
    fn test_plan_nonexistent_table() {
        let catalog = create_test_catalog();
        let planner = Planner::new(catalog);

        let stmt = SelectStmt {
            distinct: false,
            fields: vec![SelectField::All],
            from_table: Some("nonexistent".to_string()),
            where_clause: None,
            group_by: None,
            order_by: None,
            limit: None,
        };

        let result = planner.plan_select(&stmt);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            PlannerError::TableNotFound("nonexistent".to_string())
        );
    }

    #[test]
    fn test_plan_nonexistent_column() {
        let catalog = create_test_catalog();
        let planner = Planner::new(catalog);

        let stmt = SelectStmt {
            distinct: false,
            fields: vec![SelectField::Column("nonexistent_col".to_string())],
            from_table: Some("users".to_string()),
            where_clause: None,
            group_by: None,
            order_by: None,
            limit: None,
        };

        let result = planner.plan_select(&stmt);
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_no_from_clause() {
        let catalog = create_test_catalog();
        let planner = Planner::new(catalog);

        let stmt = SelectStmt {
            distinct: false,
            fields: vec![SelectField::All],
            from_table: None,
            where_clause: None,
            group_by: None,
            order_by: None,
            limit: None,
        };

        let result = planner.plan_select(&stmt);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), PlannerError::NoFromClause);
    }

    #[test]
    fn test_extract_projection_columns_with_all() {
        let catalog = create_test_catalog();
        let planner = Planner::new(catalog);

        let stmt = SelectStmt {
            distinct: false,
            fields: vec![SelectField::All],
            from_table: Some("users".to_string()),
            where_clause: None,
            group_by: None,
            order_by: None,
            limit: None,
        };

        let columns = planner
            .extract_projection_columns(&stmt, "users")
            .unwrap();
        assert_eq!(columns.len(), 3); // id, name, age
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"name".to_string()));
        assert!(columns.contains(&"age".to_string()));
    }

    #[test]
    fn test_extract_columns_from_expression() {
        let catalog = create_test_catalog();
        let planner = Planner::new(catalog);

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("age".to_string())),
            op: BinaryOperator::And,
            right: Box::new(Expression::Column("id".to_string())),
        };

        let cols = planner.extract_columns_from_expression(&expr);
        assert_eq!(cols.len(), 2);
        assert!(cols.contains(&"age".to_string()));
        assert!(cols.contains(&"id".to_string()));
    }
}
