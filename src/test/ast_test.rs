// SQL 词法和 AST 演示程序

use crate::sql::{Lexer, Token};
use crate::sql::ast::*;

pub fn demo_lexer() {
    println!("\n========== SQL 词法分析演示 ==========\n");

    // 示例 1: SELECT 查询
    let query1 = "SELECT id, name FROM users WHERE age > 18 AND status = 'active'";
    println!("查询: {}\n", query1);
    let lexer = Lexer::new(query1);
    let tokens = lexer.tokenize();
    println!("词法分析结果:");
    for (i, token) in tokens.iter().enumerate() {
        if token != &Token::Eof {
            println!("  [{}] {:?}", i, token);
        }
    }

    // 示例 2: CREATE TABLE
    let query2 = "CREATE TABLE users (id INT, name VARCHAR(50), age INT)";
    println!("\n查询: {}\n", query2);
    let lexer = Lexer::new(query2);
    let tokens = lexer.tokenize();
    println!("词法分析结果:");
    for (i, token) in tokens.iter().enumerate() {
        if token != &Token::Eof {
            println!("  [{}] {:?}", i, token);
        }
    }

    // 示例 3: INSERT
    let query3 = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)";
    println!("\n查询: {}\n", query3);
    let lexer = Lexer::new(query3);
    let tokens = lexer.tokenize();
    println!("词法分析结果:");
    for (i, token) in tokens.iter().enumerate() {
        if token != &Token::Eof {
            println!("  [{}] {:?}", i, token);
        }
    }

    // 示例 4: UPDATE
    let query4 = "UPDATE users SET age = 26 WHERE id = 1";
    println!("\n查询: {}\n", query4);
    let lexer = Lexer::new(query4);
    let tokens = lexer.tokenize();
    println!("词法分析结果:");
    for (i, token) in tokens.iter().enumerate() {
        if token != &Token::Eof {
            println!("  [{}] {:?}", i, token);
        }
    }

    // 示例 5: DELETE
    let query5 = "DELETE FROM users WHERE age < 18";
    println!("\n查询: {}\n", query5);
    let lexer = Lexer::new(query5);
    let tokens = lexer.tokenize();
    println!("词法分析结果:");
    for (i, token) in tokens.iter().enumerate() {
        if token != &Token::Eof {
            println!("  [{}] {:?}", i, token);
        }
    }
}

pub fn demo_ast() {
    println!("\n========== SQL AST 演示 ==========\n");

    // 示例 1: CREATE TABLE AST
    println!("1. CREATE TABLE 语句 AST:");
    let create_stmt = CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar(50),
                nullable: true,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int,
                nullable: true,
            },
        ],
    };
    println!("  表名: {}", create_stmt.table_name);
    println!("  列数: {}", create_stmt.columns.len());
    for col in &create_stmt.columns {
        println!("    - {}: {}", col.name, col.data_type);
    }

    // 示例 2: SELECT AST
    println!("\n2. SELECT 语句 AST:");
    let select_stmt = SelectStmt {
        distinct: false,
        fields: vec![
            SelectField::Column("id".to_string()),
            SelectField::Column("name".to_string()),
            SelectField::Column("age".to_string()),
        ],
        from_table: Some("users".to_string()),
        where_clause: Some(WhereClause {
            condition: Expression::BinaryOp {
                left: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::Column("age".to_string())),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expression::Literal(Literal::Integer(18))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::Column("status".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::Literal(Literal::String("active".to_string()))),
                }),
            },
        }),
        group_by: None,
        order_by: Some(vec![OrderBy {
            column: "age".to_string(),
            asc: false,
        }]),
        limit: Some(10),
    };
    println!("  表: {:?}", select_stmt.from_table);
    println!("  字段数: {}", select_stmt.fields.len());
    println!("  有 WHERE 子句: {}", select_stmt.where_clause.is_some());
    println!("  有 ORDER BY: {}", select_stmt.order_by.is_some());
    println!("  LIMIT: {:?}", select_stmt.limit);

    // 示例 3: INSERT AST
    println!("\n3. INSERT 语句 AST:");
    let insert_stmt = InsertStmt {
        table_name: "users".to_string(),
        columns: Some(vec!["id".to_string(), "name".to_string(), "age".to_string()]),
        values: vec![
            vec![Literal::Integer(1), Literal::String("Alice".to_string()), Literal::Integer(25)],
            vec![Literal::Integer(2), Literal::String("Bob".to_string()), Literal::Integer(30)],
        ],
    };
    println!("  表: {}", insert_stmt.table_name);
    println!("  列: {:?}", insert_stmt.columns);
    println!("  值数量: {}", insert_stmt.values.len());

    // 示例 4: DELETE AST
    println!("\n4. DELETE 语句 AST:");
    let delete_stmt = DeleteStmt {
        table_name: "users".to_string(),
        where_clause: Some(WhereClause {
            condition: Expression::BinaryOp {
                left: Box::new(Expression::Column("age".to_string())),
                op: BinaryOperator::Lt,
                right: Box::new(Expression::Literal(Literal::Integer(18))),
            },
        }),
    };
    println!("  表: {}", delete_stmt.table_name);
    println!("  有 WHERE: {}", delete_stmt.where_clause.is_some());

    // 示例 5: UPDATE AST
    println!("\n5. UPDATE 语句 AST:");
    let update_stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![
            ("age".to_string(), Expression::Literal(Literal::Integer(26))),
            (
                "status".to_string(),
                Expression::Literal(Literal::String("inactive".to_string())),
            ),
        ],
        where_clause: Some(WhereClause {
            condition: Expression::BinaryOp {
                left: Box::new(Expression::Column("id".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::Literal(Literal::Integer(1))),
            },
        }),
    };
    println!("  表: {}", update_stmt.table_name);
    println!("  赋值数: {}", update_stmt.assignments.len());
    println!("  有 WHERE: {}", update_stmt.where_clause.is_some());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_demo_lexer() {
        demo_lexer();
    }

    #[test]
    fn test_demo_ast() {
        demo_ast();
    }
}
