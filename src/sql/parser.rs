// SQL 语法分析器 (Parser)
//
// # 功能概述
//
// 递归下降 Parser，将 Token 流转换为 AST（抽象语法树）
//
// # 解析流程
//
// ```text
// Token 流 → parse() → Statement AST
//   ├─ CREATE DATABASE → parse_create_database()
//   ├─ DROP DATABASE → parse_drop_database()
//   ├─ USE DATABASE → parse_use_database()
//   ├─ SHOW → parse_show()
//   ├─ CREATE TABLE → parse_create_table()
//   ├─ DROP TABLE → parse_drop_table()
//   └─ SELECT → parse_select()
// ```
//
// # 使用示例
//
// ```rust,ignore
// use db_engine::sql::{Lexer, Parser};
//
// let sql = "CREATE DATABASE mydb";
// let lexer = Lexer::new(sql);
// let tokens = lexer.tokenize();
//
// let mut parser = Parser::new(tokens);
// let statement = parser.parse()?;
// println!("{:?}", statement);
// ```
//

use crate::sql::lexer::Token;
use crate::sql::ast::*;

// 解析错误类型
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    UnexpectedToken {
        expected: String,
        found: Token,
        position: usize,
    },
    UnexpectedEof {
        expected: String,
    },
    InvalidSyntax {
        message: String,
        position: usize,
    },
    UnsupportedFeature {
        feature: String,
    },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParseError::UnexpectedToken { expected, found, position } => {
                write!(f, "Parse error at position {}: expected {}, found {}", 
                    position, expected, found)
            }
            ParseError::UnexpectedEof { expected } => {
                write!(f, "Parse error: unexpected end of input, expected {}", expected)
            }
            ParseError::InvalidSyntax { message, position } => {
                write!(f, "Parse error at position {}: {}", position, message)
            }
            ParseError::UnsupportedFeature { feature } => {
                write!(f, "Parse error: unsupported feature: {}", feature)
            }
        }
    }
}

impl std::error::Error for ParseError {}

// SQL Parser (递归下降)
pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
}

impl Parser {
    // 创建新的 Parser
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, current: 0 }
    }

    // 主解析入口
    //
    // # 返回
    // - `Ok(Statement)`: 解析成功
    // - `Err(ParseError)`: 解析失败
    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        // 跳过可能的前导分号
        while self.match_token(&Token::Semicolon) {
            self.advance();
        }

        // 根据第一个 token 判断语句类型
        match self.peek() {
            Token::CreateDatabase => self.parse_create_database(),
            Token::DropDatabase => self.parse_drop_database(),
            Token::Use => self.parse_use_database(),
            Token::Show => self.parse_show(),
            Token::CreateTable => self.parse_create_table(),
            Token::DropTable => self.parse_drop_table(),
            Token::Select => self.parse_select(),
            Token::Insert => Err(ParseError::UnsupportedFeature {
                feature: "INSERT statement".to_string(),
            }),
            Token::Update => Err(ParseError::UnsupportedFeature {
                feature: "UPDATE statement".to_string(),
            }),
            Token::Delete => Err(ParseError::UnsupportedFeature {
                feature: "DELETE statement".to_string(),
            }),
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: "SQL statement".to_string(),
            }),
            token => Err(ParseError::UnexpectedToken {
                expected: "SQL statement keyword".to_string(),
                found: token.clone(),
                position: self.current,
            }),
        }
    }

    // ========== 数据库管理语句解析 ==========

    // 解析 CREATE DATABASE [IF NOT EXISTS] name
    fn parse_create_database(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::CreateDatabase)?;
        
        // 检查 IF NOT EXISTS
        let if_not_exists = if self.match_token(&Token::If) {
            self.advance(); // 消费 IF
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };

        // 获取数据库名
        let database_name = self.expect_identifier()?;

        Ok(Statement::CreateDatabase(CreateDatabaseStmt {
            database_name,
            if_not_exists,
        }))
    }

    // 解析 DROP DATABASE [IF EXISTS] name
    fn parse_drop_database(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::DropDatabase)?;
        
        // 检查 IF EXISTS
        let if_exists = if self.match_token(&Token::If) {
            self.advance(); // 消费 IF
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };

        // 获取数据库名
        let database_name = self.expect_identifier()?;

        Ok(Statement::DropDatabase(DropDatabaseStmt {
            database_name,
            if_exists,
        }))
    }

    // 解析 USE database_name
    fn parse_use_database(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Use)?;
        
        // 获取数据库名
        let database_name = self.expect_identifier()?;

        Ok(Statement::UseDatabase(UseDatabaseStmt {
            database_name,
        }))
    }

    // 解析 SHOW (DATABASES | TABLES)
    fn parse_show(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Show)?;

        match self.peek() {
            Token::Databases => {
                self.advance();
                Ok(Statement::ShowDatabases)
            }
            Token::Tables => {
                self.advance();
                Ok(Statement::ShowTables)
            }
            token => Err(ParseError::UnexpectedToken {
                expected: "DATABASES or TABLES".to_string(),
                found: token.clone(),
                position: self.current,
            }),
        }
    }

    // ========== 表管理语句解析 ==========

    // 解析 CREATE TABLE table_name (column_def, ...)
    fn parse_create_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::CreateTable)?;

        // 获取表名
        let table_name = self.expect_identifier()?;

        // 期望左括号
        self.expect(&Token::LeftParen)?;

        // 解析列定义列表
        let mut columns = Vec::new();
        loop {
            // 解析单个列定义
            let column = self.parse_column_def()?;
            columns.push(column);

            // 检查是否有更多列
            if self.match_token(&Token::Comma) {
                self.advance();
                continue;
            } else {
                break;
            }
        }

        // 期望右括号
        self.expect(&Token::RightParen)?;

        if columns.is_empty() {
            return Err(ParseError::InvalidSyntax {
                message: "Table must have at least one column".to_string(),
                position: self.current,
            });
        }

        Ok(Statement::CreateTable(CreateTableStmt {
            table_name,
            columns,
        }))
    }

    // 解析列定义: column_name data_type [NULL | NOT NULL]
    fn parse_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        // 列名
        let name = self.expect_identifier()?;

        // 数据类型
        let data_type = self.parse_data_type()?;

        // 可选的 NULL/NOT NULL
        let nullable = if self.match_token(&Token::Not) {
            self.advance(); // 消费 NOT
            self.expect(&Token::Null)?;
            false
        } else if self.match_token(&Token::Null) {
            self.advance(); // 消费 NULL
            true
        } else {
            true // 默认可空
        };

        Ok(ColumnDef {
            name,
            data_type,
            nullable,
        })
    }

    // 解析数据类型: INT | FLOAT | VARCHAR(n) | CHAR(n)
    fn parse_data_type(&mut self) -> Result<DataType, ParseError> {
        let identifier = self.expect_identifier()?;
        
        match identifier.to_uppercase().as_str() {
            "INT" | "INTEGER" => Ok(DataType::Int),
            "FLOAT" | "REAL" | "DOUBLE" => Ok(DataType::Float),
            "VARCHAR" => {
                self.expect(&Token::LeftParen)?;
                let size = self.expect_integer()?;
                self.expect(&Token::RightParen)?;
                Ok(DataType::Varchar(size as usize))
            }
            "CHAR" => {
                self.expect(&Token::LeftParen)?;
                let size = self.expect_integer()?;
                self.expect(&Token::RightParen)?;
                Ok(DataType::Char(size as usize))
            }
            _ => Err(ParseError::InvalidSyntax {
                message: format!("Unknown data type: {}", identifier),
                position: self.current - 1,
            }),
        }
    }

    // 解析 DROP TABLE [IF EXISTS] table_name
    fn parse_drop_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::DropTable)?;
        
        // 检查 IF EXISTS
        let if_exists = if self.match_token(&Token::If) {
            self.advance(); // 消费 IF
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };

        // 获取表名
        let table_name = self.expect_identifier()?;

        Ok(Statement::DropTable(DropTableStmt {
            table_name,
            if_exists,
        }))
    }

    // ========== SELECT 语句解析 ==========

    // 解析 SELECT 语句（简化版）
    //
    // 支持的语法：
    // SELECT [DISTINCT] field1, field2, ... FROM table [WHERE condition] [LIMIT n]
    fn parse_select(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Select)?;

        // 检查 DISTINCT
        let distinct = if self.match_token(&Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        // 解析 SELECT 字段列表
        let fields = self.parse_select_fields()?;

        // 解析 FROM 子句
        let from_table = if self.match_token(&Token::From) {
            self.advance();
            Some(self.expect_identifier()?)
        } else {
            None
        };

        // 解析 WHERE 子句（可选）
        let where_clause = if self.match_token(&Token::Where) {
            self.advance();
            Some(WhereClause {
                condition: self.parse_expression()?,
            })
        } else {
            None
        };

        // 解析 ORDER BY 子句（可选）
        let order_by = if self.match_token(&Token::OrderBy) {
            self.advance();
            Some(self.parse_order_by()?)
        } else {
            None
        };

        // 解析 GROUP BY 子句（可选）
        let group_by = if self.match_token(&Token::GroupBy) {
            self.advance();
            Some(self.parse_group_by()?)
        } else {
            None
        };

        // 解析 LIMIT 子句（可选）
        let limit = if self.match_token(&Token::Limit) {
            self.advance();
            Some(self.expect_integer()? as u32)
        } else {
            None
        };

        Ok(Statement::Select(SelectStmt {
            distinct,
            fields,
            from_table,
            where_clause,
            group_by,
            order_by,
            limit,
        }))
    }

    // 解析 SELECT 字段列表
    fn parse_select_fields(&mut self) -> Result<Vec<SelectField>, ParseError> {
        let mut fields = Vec::new();

        loop {
            // 检查 *
            if self.match_token(&Token::Star) {
                self.advance();
                fields.push(SelectField::All);
            } else {
                // 解析列名或表达式
                let field_name = self.expect_identifier()?;
                fields.push(SelectField::Column(field_name));
            }

            // 检查是否有更多字段
            if self.match_token(&Token::Comma) {
                self.advance();
                continue;
            } else {
                break;
            }
        }

        if fields.is_empty() {
            return Err(ParseError::InvalidSyntax {
                message: "SELECT must have at least one field".to_string(),
                position: self.current,
            });
        }

        Ok(fields)
    }

    // 解析 ORDER BY 子句
    fn parse_order_by(&mut self) -> Result<Vec<OrderBy>, ParseError> {
        let mut order_by = Vec::new();

        loop {
            let column = self.expect_identifier()?;
            
            // 检查 ASC/DESC
            let asc = if self.match_token(&Token::Asc) {
                self.advance();
                true
            } else if self.match_token(&Token::Desc) {
                self.advance();
                false
            } else {
                true // 默认 ASC
            };

            order_by.push(OrderBy { column, asc });

            // 检查是否有更多排序字段
            if self.match_token(&Token::Comma) {
                self.advance();
                continue;
            } else {
                break;
            }
        }

        Ok(order_by)
    }

    // 解析 GROUP BY 子句
    fn parse_group_by(&mut self) -> Result<Vec<String>, ParseError> {
        let mut columns = Vec::new();

        loop {
            let column = self.expect_identifier()?;
            columns.push(column);

            // 检查是否有更多分组字段
            if self.match_token(&Token::Comma) {
                self.advance();
                continue;
            } else {
                break;
            }
        }

        Ok(columns)
    }

    // ========== 表达式解析 ==========

    // 解析表达式（简化版，支持基本的二元操作）
    //
    // 优先级（从低到高）：
    // 1. OR
    // 2. AND
    // 3. 比较操作符 (=, !=, <, <=, >, >=)
    // 4. 算术操作符 (+, -)
    // 5. 算术操作符 (*, /, %)
    // 6. 一元操作符 (NOT, -)
    // 7. 基本表达式 (字面量, 列名, 括号)
    fn parse_expression(&mut self) -> Result<Expression, ParseError> {
        self.parse_or_expression()
    }

    // 解析 OR 表达式
    fn parse_or_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_and_expression()?;

        while self.match_token(&Token::Or) {
            self.advance();
            let right = self.parse_and_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    // 解析 AND 表达式
    fn parse_and_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_comparison_expression()?;

        while self.match_token(&Token::And) {
            self.advance();
            let right = self.parse_comparison_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    // 解析比较表达式
    fn parse_comparison_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_additive_expression()?;

        if let Some(op) = self.match_comparison_op() {
            self.advance();
            let right = self.parse_additive_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    // 解析加减表达式
    fn parse_additive_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_multiplicative_expression()?;

        while let Some(op) = self.match_additive_op() {
            self.advance();
            let right = self.parse_multiplicative_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    // 解析乘除模表达式
    fn parse_multiplicative_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_unary_expression()?;

        while let Some(op) = self.match_multiplicative_op() {
            self.advance();
            let right = self.parse_unary_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    // 解析一元表达式
    fn parse_unary_expression(&mut self) -> Result<Expression, ParseError> {
        match self.peek() {
            Token::Not => {
                self.advance();
                let expr = self.parse_unary_expression()?;
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(expr),
                })
            }
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary_expression()?;
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary_expression(),
        }
    }

    // 解析基本表达式
    fn parse_primary_expression(&mut self) -> Result<Expression, ParseError> {
        match self.peek() {
            // 括号表达式
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(&Token::RightParen)?;
                Ok(Expression::Parenthesized(Box::new(expr)))
            }
            // 整数字面量
            Token::Integer(n) => {
                let value = *n;
                self.advance();
                Ok(Expression::Literal(Literal::Integer(value)))
            }
            // 浮点数字面量
            Token::Float(f) => {
                let value = *f;
                self.advance();
                Ok(Expression::Literal(Literal::Float(value)))
            }
            // 字符串字面量
            Token::String(s) => {
                let value = s.clone();
                self.advance();
                Ok(Expression::Literal(Literal::String(value)))
            }
            // NULL
            Token::Null => {
                self.advance();
                Ok(Expression::Literal(Literal::Null))
            }
            // TRUE
            Token::True => {
                self.advance();
                Ok(Expression::Literal(Literal::Boolean(true)))
            }
            // FALSE
            Token::False => {
                self.advance();
                Ok(Expression::Literal(Literal::Boolean(false)))
            }
            // 标识符（列名）
            Token::Identifier(name) => {
                let col_name = name.clone();
                self.advance();
                Ok(Expression::Column(col_name))
            }
            token => Err(ParseError::UnexpectedToken {
                expected: "expression".to_string(),
                found: token.clone(),
                position: self.current,
            }),
        }
    }

    // ========== 辅助方法 ==========

    // 查看当前 token（不移动位置）
    fn peek(&self) -> &Token {
        if self.current < self.tokens.len() {
            &self.tokens[self.current]
        } else {
            &Token::Eof
        }
    }

    // 消费当前 token 并前进
    fn advance(&mut self) {
        if self.current < self.tokens.len() {
            self.current += 1;
        }
    }

    // 检查当前 token 是否匹配（不消费）
    fn match_token(&self, expected: &Token) -> bool {
        self.peek() == expected
    }

    // 期望特定 token，否则报错
    fn expect(&mut self, expected: &Token) -> Result<(), ParseError> {
        if self.match_token(expected) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken {
                expected: format!("{}", expected),
                found: self.peek().clone(),
                position: self.current,
            })
        }
    }

    // 期望标识符
    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Token::Identifier(name) => {
                let result = name.clone();
                self.advance();
                Ok(result)
            }
            Token::Database => {
                // 允许 DATABASE 作为标识符（例如在 CREATE DATABASE DATABASE）
                self.advance();
                Ok("DATABASE".to_string())
            }
            token => Err(ParseError::UnexpectedToken {
                expected: "identifier".to_string(),
                found: token.clone(),
                position: self.current,
            }),
        }
    }

    // 期望整数
    fn expect_integer(&mut self) -> Result<i64, ParseError> {
        match self.peek() {
            Token::Integer(n) => {
                let result = *n;
                self.advance();
                Ok(result)
            }
            token => Err(ParseError::UnexpectedToken {
                expected: "integer".to_string(),
                found: token.clone(),
                position: self.current,
            }),
        }
    }

    // 匹配比较操作符
    fn match_comparison_op(&self) -> Option<BinaryOperator> {
        match self.peek() {
            Token::Equal => Some(BinaryOperator::Eq),
            Token::NotEqual => Some(BinaryOperator::Ne),
            Token::LessThan => Some(BinaryOperator::Lt),
            Token::LessThanEqual => Some(BinaryOperator::Le),
            Token::GreaterThan => Some(BinaryOperator::Gt),
            Token::GreaterThanEqual => Some(BinaryOperator::Ge),
            Token::Like => Some(BinaryOperator::Like),
            Token::In => Some(BinaryOperator::In),
            _ => None,
        }
    }

    // 匹配加减操作符
    fn match_additive_op(&self) -> Option<BinaryOperator> {
        match self.peek() {
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            _ => None,
        }
    }

    // 匹配乘除模操作符
    fn match_multiplicative_op(&self) -> Option<BinaryOperator> {
        match self.peek() {
            Token::Star => Some(BinaryOperator::Mult),
            Token::Slash => Some(BinaryOperator::Div),
            Token::Percent => Some(BinaryOperator::Mod),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::lexer::Lexer;

    // 辅助函数：从 SQL 字符串创建 Parser
    fn parse_sql(sql: &str) -> Result<Statement, ParseError> {
        let lexer = Lexer::new(sql);
        let tokens = lexer.tokenize();
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    #[test]
    fn test_parse_create_database() {
        let stmt = parse_sql("CREATE DATABASE mydb").unwrap();
        assert_eq!(
            stmt,
            Statement::CreateDatabase(CreateDatabaseStmt {
                database_name: "mydb".to_string(),
                if_not_exists: false,
            })
        );
    }

    #[test]
    fn test_parse_create_database_if_not_exists() {
        let stmt = parse_sql("CREATE DATABASE IF NOT EXISTS mydb").unwrap();
        assert_eq!(
            stmt,
            Statement::CreateDatabase(CreateDatabaseStmt {
                database_name: "mydb".to_string(),
                if_not_exists: true,
            })
        );
    }

    #[test]
    fn test_parse_drop_database() {
        let stmt = parse_sql("DROP DATABASE mydb").unwrap();
        assert_eq!(
            stmt,
            Statement::DropDatabase(DropDatabaseStmt {
                database_name: "mydb".to_string(),
                if_exists: false,
            })
        );
    }

    #[test]
    fn test_parse_drop_database_if_exists() {
        let stmt = parse_sql("DROP DATABASE IF EXISTS mydb").unwrap();
        assert_eq!(
            stmt,
            Statement::DropDatabase(DropDatabaseStmt {
                database_name: "mydb".to_string(),
                if_exists: true,
            })
        );
    }

    #[test]
    fn test_parse_use_database() {
        let stmt = parse_sql("USE mydb").unwrap();
        assert_eq!(
            stmt,
            Statement::UseDatabase(UseDatabaseStmt {
                database_name: "mydb".to_string(),
            })
        );
    }

    #[test]
    fn test_parse_show_databases() {
        let stmt = parse_sql("SHOW DATABASES").unwrap();
        assert_eq!(stmt, Statement::ShowDatabases);
    }

    #[test]
    fn test_parse_show_tables() {
        let stmt = parse_sql("SHOW TABLES").unwrap();
        assert_eq!(stmt, Statement::ShowTables);
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = parse_sql("CREATE TABLE users (id INT, name VARCHAR(50))").unwrap();
        
        if let Statement::CreateTable(create_stmt) = stmt {
            assert_eq!(create_stmt.table_name, "users");
            assert_eq!(create_stmt.columns.len(), 2);
            assert_eq!(create_stmt.columns[0].name, "id");
            assert_eq!(create_stmt.columns[0].data_type, DataType::Int);
            assert_eq!(create_stmt.columns[1].name, "name");
            assert_eq!(create_stmt.columns[1].data_type, DataType::Varchar(50));
        } else {
            panic!("Expected CreateTable statement");
        }
    }

    #[test]
    fn test_parse_create_table_with_nullable() {
        let stmt = parse_sql("CREATE TABLE users (id INT NOT NULL, name VARCHAR(50) NULL)").unwrap();
        
        if let Statement::CreateTable(create_stmt) = stmt {
            assert_eq!(create_stmt.columns[0].nullable, false);
            assert_eq!(create_stmt.columns[1].nullable, true);
        } else {
            panic!("Expected CreateTable statement");
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = parse_sql("DROP TABLE users").unwrap();
        assert_eq!(
            stmt,
            Statement::DropTable(DropTableStmt {
                table_name: "users".to_string(),
                if_exists: false,
            })
        );
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let stmt = parse_sql("DROP TABLE IF EXISTS users").unwrap();
        assert_eq!(
            stmt,
            Statement::DropTable(DropTableStmt {
                table_name: "users".to_string(),
                if_exists: true,
            })
        );
    }

    #[test]
    fn test_parse_select_all() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();
        
        if let Statement::Select(select_stmt) = stmt {
            assert_eq!(select_stmt.distinct, false);
            assert_eq!(select_stmt.fields, vec![SelectField::All]);
            assert_eq!(select_stmt.from_table, Some("users".to_string()));
            assert!(select_stmt.where_clause.is_none());
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_parse_select_columns() {
        let stmt = parse_sql("SELECT id, name FROM users").unwrap();
        
        if let Statement::Select(select_stmt) = stmt {
            assert_eq!(select_stmt.fields.len(), 2);
            assert_eq!(select_stmt.fields[0], SelectField::Column("id".to_string()));
            assert_eq!(select_stmt.fields[1], SelectField::Column("name".to_string()));
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let stmt = parse_sql("SELECT * FROM users WHERE age > 18").unwrap();
        
        if let Statement::Select(select_stmt) = stmt {
            assert!(select_stmt.where_clause.is_some());
            
            let where_clause = select_stmt.where_clause.unwrap();
            if let Expression::BinaryOp { left, op, right } = where_clause.condition {
                assert_eq!(*left, Expression::Column("age".to_string()));
                assert_eq!(op, BinaryOperator::Gt);
                assert_eq!(*right, Expression::Literal(Literal::Integer(18)));
            } else {
                panic!("Expected BinaryOp expression");
            }
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_parse_select_distinct() {
        let stmt = parse_sql("SELECT DISTINCT name FROM users").unwrap();
        
        if let Statement::Select(select_stmt) = stmt {
            assert_eq!(select_stmt.distinct, true);
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_parse_error_unexpected_token() {
        let result = parse_sql("INVALID STATEMENT");
        assert!(result.is_err());
        
        if let Err(ParseError::UnexpectedToken { .. }) = result {
            // Expected
        } else {
            panic!("Expected UnexpectedToken error");
        }
    }

    #[test]
    fn test_parse_error_empty_input() {
        let result = parse_sql("");
        assert!(result.is_err());
    }
}
