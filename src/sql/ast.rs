// SQL 抽象语法树 (AST) 定义

use std::fmt;
use crate::common::types::DataType;

// SQL 语句根节点
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Select(SelectStmt),
    Insert(InsertStmt),
    Delete(DeleteStmt),
    Update(UpdateStmt),
    
    // 数据库管理语句
    CreateDatabase(CreateDatabaseStmt),
    DropDatabase(DropDatabaseStmt),
    UseDatabase(UseDatabaseStmt),
    ShowDatabases,
    ShowTables,
}

// CREATE TABLE 语句
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

// 列定义
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataType::Int32 => write!(f, "INT"),
            DataType::Varchar => write!(f, "VARCHAR"),
            DataType::Char(n) => write!(f, "CHAR({})", n),
        }
    }
}

// DROP TABLE 语句
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    pub table_name: String,
    pub if_exists: bool,
}

// CREATE DATABASE 语句
#[derive(Debug, Clone, PartialEq)]
pub struct CreateDatabaseStmt {
    pub database_name: String,
    pub if_not_exists: bool,
}

// DROP DATABASE 语句
#[derive(Debug, Clone, PartialEq)]
pub struct DropDatabaseStmt {
    pub database_name: String,
    pub if_exists: bool,
}

// USE DATABASE 语句
#[derive(Debug, Clone, PartialEq)]
pub struct UseDatabaseStmt {
    pub database_name: String,
}

// SELECT 语句（重点）
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub distinct: bool,
    pub fields: Vec<SelectField>,
    pub from_table: Option<String>,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<Vec<String>>,
    pub order_by: Option<Vec<OrderBy>>,
    pub limit: Option<u32>,
}

// SELECT 语句中的字段
#[derive(Debug, Clone, PartialEq)]
pub enum SelectField {
    All,                    // *
    Column(String),         // column_name
    Expression(String),     // 表达式（简化版）
}

// WHERE 子句
#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub condition: Expression,
}

// 表达式
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Literal(Literal),
    Column(String),
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    Parenthesized(Box<Expression>),
}

// 字面量
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

// 二元操作符
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum BinaryOperator {
    // 比较操作符
    Eq,     // =
    Ne,     // != 或 <>
    Lt,     // <
    Le,     // <=
    Gt,     // >
    Ge,     // >=
    // 逻辑操作符
    And,    // AND
    Or,     // OR
    // 算术操作符
    Plus,   // +
    Minus,  // -
    Mult,   // *
    Div,    // /
    Mod,    // %
    // 字符串操作符
    Like,   // LIKE
    In,     // IN
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinaryOperator::Eq => write!(f, "="),
            BinaryOperator::Ne => write!(f, "!="),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::Le => write!(f, "<="),
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::Ge => write!(f, ">="),
            BinaryOperator::And => write!(f, "AND"),
            BinaryOperator::Or => write!(f, "OR"),
            BinaryOperator::Plus => write!(f, "+"),
            BinaryOperator::Minus => write!(f, "-"),
            BinaryOperator::Mult => write!(f, "*"),
            BinaryOperator::Div => write!(f, "/"),
            BinaryOperator::Mod => write!(f, "%"),
            BinaryOperator::Like => write!(f, "LIKE"),
            BinaryOperator::In => write!(f, "IN"),
        }
    }
}

// 一元操作符
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum UnaryOperator {
    Not,    // NOT
    Minus,  // -（负号）
    Plus,   // +（正号）
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UnaryOperator::Not => write!(f, "NOT"),
            UnaryOperator::Minus => write!(f, "-"),
            UnaryOperator::Plus => write!(f, "+"),
        }
    }
}

// ORDER BY 子句
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub column: String,
    pub asc: bool,  // true = ASC, false = DESC
}

// INSERT 语句
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Literal>>,
}

// DELETE 语句
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub table_name: String,
    pub where_clause: Option<WhereClause>,
}

// UPDATE 语句
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table_name: String,
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<WhereClause>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_ast() {
        let stmt = CreateTableStmt {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Char(50),
                    nullable: true,
                },
            ],
        };

        assert_eq!(stmt.table_name, "users");
        assert_eq!(stmt.columns.len(), 2);
    }

    #[test]
    fn test_select_ast() {
        let stmt = SelectStmt {
            distinct: false,
            fields: vec![SelectField::Column("name".to_string()), SelectField::Column("age".to_string())],
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

        assert_eq!(stmt.from_table, Some("users".to_string()));
        assert_eq!(stmt.fields.len(), 2);
    }

    #[test]
    fn test_binary_operator_display() {
        assert_eq!(BinaryOperator::Eq.to_string(), "=");
        assert_eq!(BinaryOperator::And.to_string(), "AND");
        assert_eq!(BinaryOperator::Lt.to_string(), "<");
    }

    #[test]
    fn test_create_database_ast() {
        let stmt = CreateDatabaseStmt {
            database_name: "KisechansDB".to_string(),
            if_not_exists: true,
        };
        assert_eq!(stmt.database_name, "KisechansDB");
        assert_eq!(stmt.if_not_exists, true);
    }

    #[test]
    fn test_drop_database_ast() {
        let stmt = DropDatabaseStmt {
            database_name: "testdb".to_string(),
            if_exists: true,
        };
        assert_eq!(stmt.database_name, "testdb");
        assert_eq!(stmt.if_exists, true);
    }

    #[test]
    fn test_use_database_ast() {
        let stmt = UseDatabaseStmt {
            database_name: "production".to_string(),
        };
        assert_eq!(stmt.database_name, "production");
    }

    #[test]
    fn test_statement_variants() {
        let create_db = Statement::CreateDatabase(CreateDatabaseStmt {
            database_name: "test".to_string(),
            if_not_exists: false,
        });
        let drop_db = Statement::DropDatabase(DropDatabaseStmt {
            database_name: "test".to_string(),
            if_exists: true,
        });
        let use_db = Statement::UseDatabase(UseDatabaseStmt {
            database_name: "test".to_string(),
        });
        let show_dbs = Statement::ShowDatabases;
        let show_tables = Statement::ShowTables;

        assert!(matches!(create_db, Statement::CreateDatabase(_)));
        assert!(matches!(drop_db, Statement::DropDatabase(_)));
        assert!(matches!(use_db, Statement::UseDatabase(_)));
        assert!(matches!(show_dbs, Statement::ShowDatabases));
        assert!(matches!(show_tables, Statement::ShowTables));
    }
}
