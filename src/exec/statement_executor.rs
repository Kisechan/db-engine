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
use crate::rm::types::{TableSchema, DataType};
use crate::sql::lexer::Lexer;
use crate::sql::ast::{Statement, CreateDatabaseStmt, DropDatabaseStmt, UseDatabaseStmt, 
                      InsertStmt, UpdateStmt, DeleteStmt, Literal};
use crate::plan::planner::{Planner, PlannerError};
use crate::plan::optimizer::Optimizer;
use crate::plan::physical::{PhysicalPlanner, PhysicalPlannerError};
use crate::exec::iterator::{Executor, ExecutorRecord};

// 执行结果枚举
#[derive(Debug, Clone)]
pub enum ExecutionResult {
    // DDL 成功消息（如 "Database created"）
    Success(String),
    
    // SELECT 查询结果（记录列表 + 表schema）
    Query(Vec<ExecutorRecord>, crate::rm::types::TableSchema),
    
    // DML 影响的行数（INSERT/UPDATE/DELETE）
    RowsAffected(usize),
    
    // 执行错误
    Error(String),
}

impl std::fmt::Display for ExecutionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExecutionResult::Success(msg) => write!(f, "{}", msg),
            ExecutionResult::Query(records, _schema) => {
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
        log::debug!("Executing SQL: {}", sql);
        
        // 步骤 1：词法分析
        let lexer = Lexer::new(sql);
        let tokens = lexer.tokenize();

        // 步骤 2：语法分析
        let mut parser = crate::sql::parser::Parser::new(tokens);
        let statement = match parser.parse() {
            Ok(stmt) => stmt,
            Err(e) => {
                log::error!("Parse error: {}", e);
                return Ok(ExecutionResult::Error(format!("Parse error: {}", e)));
            }
        };

        // 步骤 3：执行语句
        let result = self.execute_statement(statement);
        
        match &result {
            Ok(ExecutionResult::Error(err)) => log::error!("Execution failed: {}", err),
            Err(err) => log::error!("Execution error: {:?}", err),
            Ok(_) => log::debug!("Execution successful"),
        }
        
        result
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
            
            // DML 语句
            Statement::Insert(stmt) => self.execute_insert(stmt),
            Statement::Update(stmt) => self.execute_update(stmt),
            Statement::Delete(stmt) => self.execute_delete(stmt),
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
        // 简化版 SELECT 实现：仅支持 SELECT * FROM table（无 WHERE/JOIN）
        
        // 检查是否有 FROM 子句
        let table_name = match &stmt.from_table {
            Some(name) => name,
            None => return Ok(ExecutionResult::Error("SELECT must have FROM clause".to_string())),
        };
        
        if stmt.where_clause.is_some() {
            return Ok(ExecutionResult::Error("WHERE clause not supported in simplified SELECT".to_string()));
        }
        
        // 获取数据库上下文
        let db_context = match self.db_manager.current_context_mut() {
            Ok(ctx) => ctx,
            Err(_) => return Ok(ExecutionResult::Error("No database selected".to_string())),
        };
        
        // 获取表结构（使用 table_manager.catalog）
        let schema = match db_context.table_manager.catalog.get_table_schema(table_name) {
            Ok(s) => s,
            Err(e) => return Ok(ExecutionResult::Error(format!("Table '{}' not found: {}", table_name, e))),
        };
        
        // 获取或打开表
        if !db_context.table_manager.open_tables.contains_key(table_name) {
            // 表未打开，自动打开
            if let Err(e) = db_context.table_manager.open_table(table_name) {
                return Ok(ExecutionResult::Error(
                    format!("Failed to open table '{}': {}", table_name, e)
                ));
            }
        }
        
        let table_handler = db_context.table_manager.open_tables.get_mut(table_name)
            .expect("Table should be opened");
        
        // 扫描所有记录
        let mut rows: Vec<ExecutorRecord> = Vec::new();
        
        // 遍历所有数据页
        for &page_id in table_handler.data_pages.clone().iter() {
            // 读取页面上的有效记录的 RIDs
            let rids_to_read: Vec<crate::common::types::RID> = {
                let page_buf = match table_handler.buffer_manager.fetch_page(page_id) {
                    Ok(buf) => buf,
                    Err(e) => {
                        eprintln!("[SELECT] Failed to fetch page {}: {}", page_id, e);
                        continue;
                    }
                };
                
                // 解析页面记录
                let page_handler = crate::pm::page_handler::PageHandler::new(page_buf, page_id);
                let page_header = match page_handler.read_header() {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("[SELECT] Failed to read page header: {}", e);
                        table_handler.buffer_manager.unpin_page(page_id, false).ok();
                        continue;
                    }
                };
                
                let mut valid_rids = Vec::new();
                
                // 读取每个 slot
                for slot_id in 0..page_header.slot_count {
                    let rid = crate::common::types::RID { page_id, slot_id };
                    
                    // 跳过已删除的记录
                    if let Ok(slot) = page_handler.read_slot(slot_id) {
                        if slot.offset != -1 {
                            valid_rids.push(rid);
                        }
                    }
                }
                
                // unpin 页面
                table_handler.buffer_manager.unpin_page(page_id, false).ok();
                
                valid_rids
            };
            
            // 读取记录数据
            for rid in rids_to_read {
                match table_handler.get(rid) {
                    Ok(record_bytes) => {
                        rows.push(ExecutorRecord {
                            rid,
                            data: record_bytes,
                        });
                    }
                    Err(e) => {
                        eprintln!("[SELECT] Failed to read record {:?}: {}", rid, e);
                    }
                }
            }
        }
        
        println!("[SELECT] Found {} rows from table '{}'", rows.len(), table_name);
        
        // 返回结果（包含schema）
        Ok(ExecutionResult::Query(rows, schema))
    }
    
    // ========== DML 语句执行 ==========
    
    // 执行 INSERT 语句
    fn execute_insert(&mut self, stmt: InsertStmt) -> Result<ExecutionResult, ExecutorError> {
        // 检查是否选择了数据库
        let table_name = stmt.table_name.clone();
        
        // 获取表结构（使用不可变引用）
        let schema = {
            let db_context = match self.db_manager.current_context() {
                Ok(ctx) => ctx,
                Err(_) => return Ok(ExecutionResult::Error("No database selected. Use 'USE database_name' first".to_string())),
            };
            
            // 使用 table_manager.catalog，因为 CREATE TABLE 修改的是这个实例
            match db_context.table_manager.catalog.get_table_schema(&table_name) {
                Ok(s) => s,
                Err(e) => return Ok(ExecutionResult::Error(format!("Table '{}' not found: {}", table_name, e))),
            }
        };
        
        // 验证列数
        let column_count = if let Some(ref cols) = stmt.columns {
            cols.len()
        } else {
            schema.columns.len()
        };
        
        let mut inserted_count = 0;
        
        // 插入每一行数据
        for row in &stmt.values {
            if row.len() != column_count {
                return Ok(ExecutionResult::Error(
                    format!("Column count mismatch: expected {}, got {}", column_count, row.len())
                ));
            }
            
            // 将字面量转换为字节数据
            let record = match self.literals_to_record(&schema, &stmt.columns, row) {
                Ok(r) => r,
                Err(e) => return Ok(ExecutionResult::Error(format!("Failed to convert values: {}", e))),
            };
            
            // 插入记录
            let db_context = match self.db_manager.current_context_mut() {
                Ok(ctx) => ctx,
                Err(e) => return Ok(ExecutionResult::Error(format!("Failed to get database context: {:?}", e))),
            };
            
            // 获取或打开表
            if !db_context.table_manager.open_tables.contains_key(&table_name) {
                // 表未打开，需要打开
                if let Err(e) = db_context.table_manager.open_table(&table_name) {
                    return Ok(ExecutionResult::Error(
                        format!("Failed to open table '{}': {}", table_name, e)
                    ));
                }
            }
            
            let table_handler = db_context.table_manager.open_tables.get_mut(&table_name)
                .expect("Table should be opened");
            
            // 插入记录
            match table_handler.insert(&record) {
                Ok(_rid) => inserted_count += 1,
                Err(e) => return Ok(ExecutionResult::Error(format!("Failed to insert record: {}", e))),
            }
        }
        
        // INSERT 完成后，刷新数据到磁盘
        let db_context = match self.db_manager.current_context_mut() {
            Ok(ctx) => ctx,
            Err(e) => return Ok(ExecutionResult::Error(format!("Failed to get database context: {:?}", e))),
        };
        
        if let Some(table_handler) = db_context.table_manager.open_tables.get_mut(&table_name) {
            if let Err(e) = table_handler.flush() {
                return Ok(ExecutionResult::Error(format!("Failed to flush table data: {}", e)));
            }
        }
        
        Ok(ExecutionResult::Success(
            format!("{} row(s) inserted into '{}'", inserted_count, table_name)
        ))
    }
    
    // 执行 UPDATE 语句
    fn execute_update(&mut self, _stmt: UpdateStmt) -> Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult::Error(
            "UPDATE statement not fully implemented yet".to_string()
        ))
    }
    
    // 执行 DELETE 语句  
    fn execute_delete(&mut self, _stmt: DeleteStmt) -> Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult::Error(
            "DELETE statement not fully implemented yet".to_string()
        ))
    }
    
    // 辅助方法：将字面量列表转换为记录字节
    fn literals_to_record(
        &self,
        schema: &TableSchema,
        columns: &Option<Vec<String>>,
        values: &[Literal],
    ) -> Result<Vec<u8>, String> {
        let mut record = Vec::new();
        
        // 如果指定了列名，需要按表结构顺序填充
        if let Some(col_names) = columns {
            // 创建列名到值的映射
            let mut value_map: std::collections::HashMap<&str, &Literal> = std::collections::HashMap::new();
            for (i, col_name) in col_names.iter().enumerate() {
                value_map.insert(col_name.as_str(), &values[i]);
            }
            
            // 按表结构顺序填充值
            for col_def in &schema.columns {
                if let Some(literal) = value_map.get(col_def.name.as_str()) {
                    self.append_literal_to_record(&mut record, literal, &col_def.data_type)?;
                } else {
                    return Err(format!("Missing value for column '{}'", col_def.name));
                }
            }
        } else {
            // 没有指定列名，按顺序填充
            if values.len() != schema.columns.len() {
                return Err(format!(
                    "Column count mismatch: table has {} columns, got {} values",
                    schema.columns.len(),
                    values.len()
                ));
            }
            
            for (i, literal) in values.iter().enumerate() {
                self.append_literal_to_record(&mut record, literal, &schema.columns[i].data_type)?;
            }
        }
        
        Ok(record)
    }
    
    // 辅助方法：将单个字面量添加到记录中
    fn append_literal_to_record(
        &self,
        record: &mut Vec<u8>,
        literal: &Literal,
        data_type: &DataType,
    ) -> Result<(), String> {
        match (literal, data_type) {
            (Literal::Integer(n), DataType::Int32) => {
                record.extend_from_slice(&(*n as i32).to_le_bytes());
                Ok(())
            }
            (Literal::Boolean(b), DataType::Int32) => {
                // 布尔值存储为 INT (0 或 1)
                let val = if *b { 1i32 } else { 0i32 };
                record.extend_from_slice(&val.to_le_bytes());
                Ok(())
            }
            (Literal::String(s), DataType::VarChar) => {
                // VARCHAR: 存储为 4 字节长度 + 字符串内容
                let bytes = s.as_bytes();
                record.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                record.extend_from_slice(bytes);
                Ok(())
            }
            (Literal::Null, _) => {
                // NULL 值的处理（简化版：使用特殊标记）
                Err("NULL values not fully supported yet".to_string())
            }
            _ => Err(format!(
                "Type mismatch: cannot convert {:?} to {:?}",
                literal, data_type
            )),
        }
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
