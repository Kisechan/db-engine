// 统一的 SQL 语句执行器
//
// # 功能概述
//
// StatementExecutor 提供了统一的 SQL 语句执行接口：
// - 词法分析：Lexer::tokenize(sql)
// - 语法分析：将 tokens 转换为 AST（目前手动构建）
// - 语句执行：根据 Statement 类型分发到不同的执行路径
//
// # 执行流程
//
// ```text
// SQL 字符串
//   ↓ Lexer::tokenize()
// Token 流
//   ↓ Parser::parse() [TODO]
// AST (Statement)
//   ↓ execute()
// ├─ DDL (CREATE/DROP DATABASE/TABLE)
// │  └→ DatabaseManager / CatalogManager
// ├─ DML (SELECT/INSERT/UPDATE/DELETE)
// │  └→ Planner → Optimizer → PhysicalPlanner → Executor
// └─ DCL (SHOW DATABASES/TABLES, USE DATABASE)
//    └→ DatabaseManager
// ```
//
// # 使用示例
//
// ```rust,ignore
// use db_engine::exec::StatementExecutor;
// use db_engine::rm::DatabaseManager;
//
// let mut db_mgr = DatabaseManager::new("./data")?;
// let mut executor = StatementExecutor::new(&mut db_mgr);
//
// // 创建数据库
// let result = executor.execute("CREATE DATABASE KisechansDB")?;
// println!("{:?}", result);
//
// // 切换数据库
// let result = executor.execute("USE KisechansDB")?;
//
// // 查询数据
// let result = executor.execute("SELECT * FROM users WHERE age > 18")?;
// if let ExecutionResult::Query(records) = result {
//     for record in records {
//         println!("{:?}", record);
//     }
// }
// ```
//
use crate::rm::database_manager::{DatabaseManager, DatabaseError};
use crate::sql::lexer::Lexer;
use crate::sql::ast::{Statement, CreateDatabaseStmt, DropDatabaseStmt, UseDatabaseStmt};
use crate::plan::planner::{Planner, PlannerError};
use crate::plan::optimizer::Optimizer;
use crate::plan::physical::{PhysicalPlanner, PhysicalPlannerError};
use crate::exec::iterator::{Executor, ExecutorRecord};

// 执行结果枚举
#[derive(Debug, Clone)]
pub enum ExecutionResult {
    // DDL 成功消息（如 "Database created"）
    Success(String),
    
    // SELECT 查询结果（记录列表）
    Query(Vec<ExecutorRecord>),
    
    // DML 影响的行数（INSERT/UPDATE/DELETE）
    RowsAffected(usize),
    
    // 执行错误
    Error(String),
}

impl std::fmt::Display for ExecutionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExecutionResult::Success(msg) => write!(f, "{}", msg),
            ExecutionResult::Query(records) => {
                write!(f, "Query returned {} row(s)", records.len())
            }
            ExecutionResult::RowsAffected(count) => {
                write!(f, "{} row(s) affected", count)
            }
            ExecutionResult::Error(msg) => write!(f, "Error: {}", msg),
        }
    }
}

// 执行器错误类型
#[derive(Debug)]
pub enum ExecutorError {
    DatabaseError(DatabaseError),
    PlannerError(PlannerError),
    PhysicalPlannerError(PhysicalPlannerError),
    LexerError(String),
    ParserError(String),
    ExecutionError(String),
    NoDatabaseSelected,
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExecutorError::DatabaseError(e) => write!(f, "{}", e),
            ExecutorError::PlannerError(e) => write!(f, "{}", e),
            ExecutorError::PhysicalPlannerError(e) => write!(f, "{}", e),
            ExecutorError::LexerError(msg) => write!(f, "Lexer error: {}", msg),
            ExecutorError::ParserError(msg) => write!(f, "Parser error: {}", msg),
            ExecutorError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            ExecutorError::NoDatabaseSelected => {
                write!(f, "No database selected. Use 'USE database_name' first")
            }
        }
    }
}

impl std::error::Error for ExecutorError {}

impl From<DatabaseError> for ExecutorError {
    fn from(err: DatabaseError) -> Self {
        ExecutorError::DatabaseError(err)
    }
}

impl From<PlannerError> for ExecutorError {
    fn from(err: PlannerError) -> Self {
        ExecutorError::PlannerError(err)
    }
}

impl From<PhysicalPlannerError> for ExecutorError {
    fn from(err: PhysicalPlannerError) -> Self {
        ExecutorError::PhysicalPlannerError(err)
    }
}

// SQL 语句执行器
pub struct StatementExecutor<'a> {
    db_manager: &'a mut DatabaseManager,
}

impl<'a> StatementExecutor<'a> {
    // 创建新的语句执行器
    //
    // # 参数
    // - `db_manager`: 数据库管理器的可变引用
    //
    // # 返回
    // - `StatementExecutor`: 执行器实例
    pub fn new(db_manager: &'a mut DatabaseManager) -> Self {
        StatementExecutor { db_manager }
    }

    // 执行 SQL 语句
    //
    // # 参数
    // - `sql`: SQL 语句字符串
    //
    // # 返回
    // - `Ok(ExecutionResult)`: 执行成功，返回结果
    // - `Err(ExecutorError)`: 执行失败，返回错误
    //
    // # 流程
    // 1. 词法分析：将 SQL 字符串转换为 Token 流
    // 2. 语法分析：将 Token 流转换为 AST
    // 3. 语句分发：根据 Statement 类型执行对应的逻辑
    pub fn execute(&mut self, sql: &str) -> Result<ExecutionResult, ExecutorError> {
        // 步骤 1：词法分析
        let lexer = Lexer::new(sql);
        let tokens = lexer.tokenize();

        // 步骤 2：语法分析
        let mut parser = crate::sql::parser::Parser::new(tokens);
        let statement = match parser.parse() {
            Ok(stmt) => stmt,
            Err(e) => return Ok(ExecutionResult::Error(format!("Parse error: {}", e))),
        };

        // 步骤 3：执行语句
        self.execute_statement(statement)
    }

    // 执行已解析的 SQL 语句（AST）
    //
    // # 参数
    // - `statement`: 已解析的 SQL 语句 AST
    //
    // # 返回
    // - `Ok(ExecutionResult)`: 执行成功，返回结果
    // - `Err(ExecutorError)`: 执行失败，返回错误
    pub fn execute_statement(&mut self, statement: Statement) -> Result<ExecutionResult, ExecutorError> {
        match statement {
            // 数据库管理语句
            Statement::CreateDatabase(stmt) => self.execute_create_database(stmt),
            Statement::DropDatabase(stmt) => self.execute_drop_database(stmt),
            Statement::UseDatabase(stmt) => self.execute_use_database(stmt),
            Statement::ShowDatabases => self.execute_show_databases(),
            Statement::ShowTables => self.execute_show_tables(),
            
            // 表管理语句
            Statement::CreateTable(stmt) => self.execute_create_table(stmt),
            Statement::DropTable(stmt) => self.execute_drop_table(stmt),
            
            // 查询语句
            Statement::Select(stmt) => self.execute_select(stmt),
            
            // DML 语句（暂未实现）
            Statement::Insert(_) => Ok(ExecutionResult::Error(
                "INSERT statement not implemented yet".to_string()
            )),
            Statement::Update(_) => Ok(ExecutionResult::Error(
                "UPDATE statement not implemented yet".to_string()
            )),
            Statement::Delete(_) => Ok(ExecutionResult::Error(
                "DELETE statement not implemented yet".to_string()
            )),
        }
    }

    // ========== 数据库管理语句执行 ==========

    // 执行 CREATE DATABASE
    fn execute_create_database(&mut self, stmt: CreateDatabaseStmt) -> Result<ExecutionResult, ExecutorError> {
        match self.db_manager.create_database(&stmt.database_name, stmt.if_not_exists) {
            Ok(()) => Ok(ExecutionResult::Success(
                format!("Database '{}' created successfully", stmt.database_name)
            )),
            Err(e) => Ok(ExecutionResult::Error(e.to_string())),
        }
    }

    // 执行 DROP DATABASE
    fn execute_drop_database(&mut self, stmt: DropDatabaseStmt) -> Result<ExecutionResult, ExecutorError> {
        match self.db_manager.drop_database(&stmt.database_name, stmt.if_exists) {
            Ok(()) => Ok(ExecutionResult::Success(
                format!("Database '{}' dropped successfully", stmt.database_name)
            )),
            Err(e) => Ok(ExecutionResult::Error(e.to_string())),
        }
    }

    // 执行 USE DATABASE
    fn execute_use_database(&mut self, stmt: UseDatabaseStmt) -> Result<ExecutionResult, ExecutorError> {
        match self.db_manager.use_database(&stmt.database_name) {
            Ok(()) => Ok(ExecutionResult::Success(
                format!("Switched to database '{}'", stmt.database_name)
            )),
            Err(e) => Ok(ExecutionResult::Error(e.to_string())),
        }
    }

    // 执行 SHOW DATABASES
    fn execute_show_databases(&mut self) -> Result<ExecutionResult, ExecutorError> {
        match self.db_manager.list_databases() {
            Ok(databases) => {
                let msg = if databases.is_empty() {
                    "No databases found".to_string()
                } else {
                    format!("Databases:\n{}", databases.join("\n"))
                };
                Ok(ExecutionResult::Success(msg))
            }
            Err(e) => Ok(ExecutionResult::Error(e.to_string())),
        }
    }

    // 执行 SHOW TABLES
    fn execute_show_tables(&mut self) -> Result<ExecutionResult, ExecutorError> {
        match self.db_manager.list_tables() {
            Ok(tables) => {
                let msg = if tables.is_empty() {
                    "No tables found in current database".to_string()
                } else {
                    format!("Tables:\n{}", tables.join("\n"))
                };
                Ok(ExecutionResult::Success(msg))
            }
            Err(e) => Ok(ExecutionResult::Error(e.to_string())),
        }
    }

    // ========== 表管理语句执行 ==========

    // 执行 CREATE TABLE
    fn execute_create_table(&mut self, stmt: crate::sql::ast::CreateTableStmt) -> Result<ExecutionResult, ExecutorError> {
        // 检查是否选择了数据库
        let context = match self.db_manager.current_context_mut() {
            Ok(ctx) => ctx,
            Err(_) => return Ok(ExecutionResult::Error(
                "No database selected. Use 'USE database_name' first".to_string()
            )),
        };

        // 将 AST 的列定义转换为 TableSchema 的列定义
        use crate::rm::types::{ColumnDef, DataType, TableSchema};
        
        let columns: Vec<ColumnDef> = stmt.columns.iter().map(|col| {
            let col_type = match &col.data_type {
                crate::sql::ast::DataType::Int => DataType::Int32,
                crate::sql::ast::DataType::Float => {
                    // Float 类型暂不支持，使用 Int32 代替
                    DataType::Int32
                },
                crate::sql::ast::DataType::Varchar(n) => DataType::VarChar,
                crate::sql::ast::DataType::Char(n) => DataType::Char(*n),
            };
            
            ColumnDef {
                name: col.name.clone(),
                data_type: col_type,
                nullable: col.nullable,
            }
        }).collect();

        // 构建 TableSchema
        let schema = TableSchema {
            table_name: stmt.table_name.clone(),
            table_id: 0, // 将由 CatalogManager 自动分配
            columns,
            root_pages: Vec::new(),
            create_time: TableSchema::current_timestamp(),
            row_count: 0,
            last_modified: TableSchema::current_timestamp(),
        };

        // 创建表（通过 TableManager）
        match context.table_manager.create_table(schema) {
            Ok(()) => Ok(ExecutionResult::Success(
                format!("Table '{}' created successfully", stmt.table_name)
            )),
            Err(e) => Ok(ExecutionResult::Error(e)),
        }
    }

    // 执行 DROP TABLE
    fn execute_drop_table(&mut self, stmt: crate::sql::ast::DropTableStmt) -> Result<ExecutionResult, ExecutorError> {
        // 检查是否选择了数据库
        let context = match self.db_manager.current_context_mut() {
            Ok(ctx) => ctx,
            Err(_) => return Ok(ExecutionResult::Error(
                "No database selected. Use 'USE database_name' first".to_string()
            )),
        };

        // 删除表
        match context.table_manager.drop_table(&stmt.table_name) {
            Ok(()) => Ok(ExecutionResult::Success(
                format!("Table '{}' dropped successfully", stmt.table_name)
            )),
            Err(e) => {
                // 如果设置了 IF EXISTS 且表不存在，不报错
                if stmt.if_exists && e.contains("not found") {
                    Ok(ExecutionResult::Success(
                        format!("Table '{}' does not exist (skipped)", stmt.table_name)
                    ))
                } else {
                    Ok(ExecutionResult::Error(e))
                }
            }
        }
    }

    // ========== 查询语句执行 ==========

    // 执行 SELECT 查询
    fn execute_select(&mut self, stmt: crate::sql::ast::SelectStmt) -> Result<ExecutionResult, ExecutorError> {
        // 由于 Planner 和 PhysicalPlanner 需要拥有 catalog 和 table_manager，
        // 而它们不支持 Clone，我们暂时返回未实现错误
        // TODO: 重构 Planner 和 PhysicalPlanner 使用引用而非所有权
        
        Ok(ExecutionResult::Error(
            "SELECT query execution not fully implemented yet. \
             Requires refactoring Planner and PhysicalPlanner to use references.".to_string()
        ))
        
        /* 原计划实现（需要 Clone 支持）:
        
        // 检查是否选择了数据库
        let context = match self.db_manager.current_context() {
            Ok(ctx) => ctx,
            Err(_) => return Ok(ExecutionResult::Error(
                "No database selected. Use 'USE database_name' first".to_string()
            )),
        };

        // 步骤 1：生成逻辑计划
        let planner = Planner::new(context.catalog.clone());
        let logical_plan = match planner.plan_select(&stmt) {
            Ok(plan) => plan,
            Err(e) => return Ok(ExecutionResult::Error(format!("Planner error: {}", e))),
        };

        println!("[StatementExecutor] Logical plan: {:?}", logical_plan);

        // 步骤 2：优化逻辑计划
        let optimized_plan = Optimizer::optimize(logical_plan);
        println!("[StatementExecutor] Optimized plan: {:?}", optimized_plan);

        // 步骤 3：生成物理计划（执行器树）
        let mut physical_planner = PhysicalPlanner::new(context.table_manager.clone());
        let mut executor = match physical_planner.plan(optimized_plan) {
            Ok(exec) => exec,
            Err(e) => return Ok(ExecutionResult::Error(format!("Physical planner error: {}", e))),
        };

        // 步骤 4：执行查询
        match executor.init() {
            Ok(()) => {},
            Err(e) => return Ok(ExecutionResult::Error(format!("Executor init error: {}", e))),
        }

        let mut results = Vec::new();
        loop {
            match executor.next() {
                Ok(Some(record)) => results.push(record),
                Ok(None) => break,
                Err(e) => return Ok(ExecutionResult::Error(format!("Executor error: {}", e))),
            }
        }

        println!("[StatementExecutor] Query returned {} row(s)", results.len());
        Ok(ExecutionResult::Query(results))
        */
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::*;

    #[test]
    fn test_create_database() {
        let mut db_mgr = DatabaseManager::new("./test_data/executor_test").unwrap();
        let mut executor = StatementExecutor::new(&mut db_mgr);

        let stmt = Statement::CreateDatabase(CreateDatabaseStmt {
            database_name: "testdb".to_string(),
            if_not_exists: false,
        });

        let result = executor.execute_statement(stmt).unwrap();
        assert!(matches!(result, ExecutionResult::Success(_)));

        // 清理
        let _ = std::fs::remove_dir_all("./test_data/executor_test");
    }

    #[test]
    fn test_show_databases() {
        let mut db_mgr = DatabaseManager::new("./test_data/executor_test2").unwrap();
        db_mgr.create_database("db1", false).unwrap();
        db_mgr.create_database("db2", false).unwrap();

        let mut executor = StatementExecutor::new(&mut db_mgr);
        let result = executor.execute_statement(Statement::ShowDatabases).unwrap();
        
        if let ExecutionResult::Success(msg) = result {
            assert!(msg.contains("db1"));
            assert!(msg.contains("db2"));
        } else {
            panic!("Expected Success result");
        }

        // 清理
        let _ = std::fs::remove_dir_all("./test_data/executor_test2");
    }

    #[test]
    fn test_no_database_selected() {
        let mut db_mgr = DatabaseManager::new("./test_data/executor_test3").unwrap();
        let mut executor = StatementExecutor::new(&mut db_mgr);

        let stmt = Statement::ShowTables;
        let result = executor.execute_statement(stmt).unwrap();
        
        if let ExecutionResult::Error(msg) = result {
            assert!(msg.contains("No database selected"));
        } else {
            panic!("Expected Error result");
        }

        // 清理
        let _ = std::fs::remove_dir_all("./test_data/executor_test3");
    }

    #[test]
    fn test_execute_sql_string() {
        // 清理旧数据
        let _ = std::fs::remove_dir_all("./test_data/executor_test4");
        
        let mut db_mgr = DatabaseManager::new("./test_data/executor_test4").unwrap();
        let mut executor = StatementExecutor::new(&mut db_mgr);

        // 测试创建数据库
        let result = executor.execute("CREATE DATABASE testdb").unwrap();
        assert!(matches!(result, ExecutionResult::Success(_)));

        // 测试显示数据库
        let result = executor.execute("SHOW DATABASES").unwrap();
        if let ExecutionResult::Success(msg) = result {
            assert!(msg.contains("testdb"));
        } else {
            panic!("Expected Success result");
        }

        // 测试切换数据库
        let result = executor.execute("USE testdb").unwrap();
        assert!(matches!(result, ExecutionResult::Success(_)));

        // 清理
        let _ = std::fs::remove_dir_all("./test_data/executor_test4");
    }

    #[test]
    fn test_execute_create_table_sql() {
        // 清理旧数据
        let _ = std::fs::remove_dir_all("./test_data/executor_test5");
        
        let mut db_mgr = DatabaseManager::new("./test_data/executor_test5").unwrap();
        let mut executor = StatementExecutor::new(&mut db_mgr);

        // 创建并切换到数据库
        executor.execute("CREATE DATABASE testdb").unwrap();
        executor.execute("USE testdb").unwrap();

        // 创建表
        let result = executor.execute("CREATE TABLE users (id INT NOT NULL, name VARCHAR(50))").unwrap();
        if let ExecutionResult::Success(msg) = result {
            assert!(msg.contains("users"));
            assert!(msg.contains("created"));
        } else {
            panic!("Expected Success result, got {:?}", result);
        }

        // 清理
        let _ = std::fs::remove_dir_all("./test_data/executor_test5");
    }
}
