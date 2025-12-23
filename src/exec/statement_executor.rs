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
use crate::rm::database_manager::{DatabaseManager, DatabaseError, DatabaseContext};
use crate::sql::lexer::Lexer;
use crate::sql::ast::{
    Statement, CreateDatabaseStmt, DropDatabaseStmt, UseDatabaseStmt,
    InsertStmt, UpdateStmt, DeleteStmt, Literal, CreateTableStmt, DropTableStmt, SelectStmt,
    Expression, WhereClause, BinaryOperator,
};
use crate::sql::parser::Parser;
use crate::plan::planner::PlannerError;
use crate::plan::physical::PhysicalPlannerError;
use crate::exec::iterator::ExecutorRecord;
use crate::common::types::{ColumnDef, DataType, TableSchema, RID};
// 执行结果枚举
#[derive(Debug, Clone)]
pub enum ExecutionResult {
    // DDL 成功消息（如 "Database created"）
    Success(String),
    
    // SELECT 查询结果（记录列表 + 表schema）
    Query(Vec<ExecutorRecord>, TableSchema),
    
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
        let mut parser = Parser::new(tokens);
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

    // ==================== 辅助方法：封装通用操作 ====================
    
    // 获取当前数据库上下文（可变引用）
    fn get_context_mut(&mut self) -> Result<&mut DatabaseContext, ExecutionResult> {
        self.db_manager.current_context_mut()
            .map_err(|_| ExecutionResult::Error(
                "No database selected. Use 'USE database_name' first".to_string()
            ))
    }
    
    // 获取当前数据库上下文（不可变引用）
    fn get_context(&self) -> Result<&DatabaseContext, ExecutionResult> {
        self.db_manager.current_context()
            .map_err(|_| ExecutionResult::Error(
                "No database selected. Use 'USE database_name' first".to_string()
            ))
    }
    
    // 获取表的 Schema
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, ExecutionResult> {
        let context = self.get_context()?;
        context.table_manager.catalog.get_table_schema(table_name)
            .map_err(|e| ExecutionResult::Error(
                format!("Table '{}' not found: {}", table_name, e)
            ))
    }
    
    // 确保表已打开
    fn ensure_table_open(&mut self, table_name: &str) -> Result<(), ExecutionResult> {
        let context = self.get_context_mut()?;
        if !context.table_manager.open_tables.contains_key(table_name) {
            context.table_manager.open_table(table_name)
                .map_err(|e| ExecutionResult::Error(
                    format!("Failed to open table '{}': {}", table_name, e)
                ))?;
        }
        Ok(())
    }
    
    // 刷新表数据到磁盘
    fn flush_table(&mut self, table_name: &str) -> Result<(), ExecutionResult> {
        let context = self.get_context_mut()?;
        if let Some(handler) = context.table_manager.open_tables.get_mut(table_name) {
            handler.flush()
                .map_err(|e| ExecutionResult::Error(
                    format!("Failed to flush table '{}': {}", table_name, e)
                ))?;
        }
        Ok(())
    }
    
    // 扫描表中所有有效记录的 RID
    // 使用底层 TableHandler::list_valid_rids 方法
    fn scan_all_rids(&mut self, table_name: &str) -> Result<Vec<RID>, ExecutionResult> {
        self.ensure_table_open(table_name)?;
        
        let context = self.get_context_mut()?;
        let handler = context.table_manager.open_tables.get_mut(table_name)
            .ok_or_else(|| ExecutionResult::Error(format!("Table '{}' not opened", table_name)))?;
        
        let mut all_rids = Vec::new();
        let data_pages = handler.get_data_pages().to_vec();
        
        for page_id in data_pages {
            match handler.list_valid_rids(page_id) {
                Ok(rids) => all_rids.extend(rids),
                Err(e) => {
                    log::warn!("Failed to list RIDs on page {}: {}", page_id, e);
                }
            }
        }
        
        Ok(all_rids)
    }

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
    fn execute_create_table(&mut self, stmt: CreateTableStmt) -> Result<ExecutionResult, ExecutorError> {
        // 检查是否选择了数据库
        let context = match self.db_manager.current_context_mut() {
            Ok(ctx) => ctx,
            Err(_) => return Ok(ExecutionResult::Error(
                "No database selected. Use 'USE database_name' first".to_string()
            )),
        };

        // 将 AST 的列定义转换为 TableSchema 的列定义
        
        let columns: Vec<ColumnDef> = stmt.columns.iter().map(|col| {
            ColumnDef {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
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
    fn execute_drop_table(&mut self, stmt: DropTableStmt) -> Result<ExecutionResult, ExecutorError> {
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
    //
    // TODO:
    // - [x] 支持 WHERE 子句（条件过滤）
    // - [ ] 支持指定列投影（SELECT col1, col2）
    // - [ ] 支持 ORDER BY 排序
    // - [ ] 支持 LIMIT 分页
    // - [ ] 支持 DISTINCT 去重
    // - [ ] 支持 JOIN 多表连接
    // - [ ] 支持聚合函数（COUNT, SUM, AVG, MAX, MIN）
    // - [ ] 支持 GROUP BY 分组
    //
    fn execute_select(&mut self, stmt: SelectStmt) -> Result<ExecutionResult, ExecutorError> {
        // 获取表名
        let table_name = match &stmt.from_table {
            Some(name) => name.clone(),
            None => return Ok(ExecutionResult::Error("SELECT must have FROM clause".to_string())),
        };
        
        // 获取表 Schema
        let schema = match self.get_table_schema(&table_name) {
            Ok(s) => s,
            Err(e) => return Ok(e),
        };
        
        // 使用 scan_all_rids 获取所有有效记录（底层使用 list_valid_rids）
        let rids = match self.scan_all_rids(&table_name) {
            Ok(r) => r,
            Err(e) => return Ok(e),
        };
        
        // 读取记录并过滤
        let mut rows: Vec<ExecutorRecord> = Vec::new();
        
        let context = match self.get_context_mut() {
            Ok(ctx) => ctx,
            Err(e) => return Ok(e),
        };
        
        let handler = context.table_manager.open_tables.get_mut(&table_name)
            .expect("Table should be opened by scan_all_rids");
        
        for rid in rids {
            match handler.get(rid) {
                Ok(data) => {
                    // 如果有 WHERE 子句，进行条件过滤
                    if let Some(ref where_clause) = stmt.where_clause {
                        match Self::evaluate_where_condition(&schema, &data, where_clause) {
                            Ok(true) => rows.push(ExecutorRecord { rid, data }),
                            Ok(false) => continue, // 条件不满足，跳过
                            Err(e) => return Ok(ExecutionResult::Error(e)),
                        }
                    } else {
                        // 无 WHERE 子句，返回所有记录
                        rows.push(ExecutorRecord { rid, data });
                    }
                }
                Err(e) => log::warn!("Failed to read record {:?}: {}", rid, e),
            }
        }
        
        log::info!("[SELECT] Found {} rows from table '{}' (after WHERE filter)", rows.len(), table_name);
        
        Ok(ExecutionResult::Query(rows, schema))
    }
    
    // 执行 INSERT 语句
    //
    // TODO:
    // - [ ] 支持 INSERT ... SELECT 语法
    // - [ ] 支持 DEFAULT 值
    // - [ ] 支持 NULL 值处理
    // - [ ] 添加类型检查和约束验证
    // - [ ] 支持批量插入优化
    //
    fn execute_insert(&mut self, stmt: InsertStmt) -> Result<ExecutionResult, ExecutorError> {
        let table_name = stmt.table_name.clone();
        
        // 获取表 Schema
        let schema = match self.get_table_schema(&table_name) {
            Ok(s) => s,
            Err(e) => return Ok(e),
        };
        
        // 验证列数
        let column_count = stmt.columns.as_ref()
            .map(|cols| cols.len())
            .unwrap_or(schema.columns.len());
        
        let mut inserted_count = 0;
        
        // 插入每一行数据
        for row in &stmt.values {
            if row.len() != column_count {
                return Ok(ExecutionResult::Error(
                    format!("Column count mismatch: expected {}, got {}", column_count, row.len())
                ));
            }
            
            // 将字面量转换为字节数据
            let record = match Self::literals_to_record(&schema, &stmt.columns, row) {
                Ok(r) => r,
                Err(e) => return Ok(ExecutionResult::Error(format!("Failed to convert values: {}", e))),
            };
            
            // 确保表已打开
            if let Err(e) = self.ensure_table_open(&table_name) {
                return Ok(e);
            }
            
            let context = match self.get_context_mut() {
                Ok(ctx) => ctx,
                Err(e) => return Ok(e),
            };
            
            let handler = context.table_manager.open_tables.get_mut(&table_name)
                .expect("Table should be opened");
            
            // 使用底层 insert 方法
            match handler.insert(&record) {
                Ok(_rid) => inserted_count += 1,
                Err(e) => return Ok(ExecutionResult::Error(format!("Failed to insert record: {}", e))),
            }
        }
        
        // 刷新到磁盘
        if let Err(e) = self.flush_table(&table_name) {
            return Ok(e);
        }
        
        Ok(ExecutionResult::Success(
            format!("{} row(s) inserted into '{}'", inserted_count, table_name)
        ))
    }
    
    // 执行 UPDATE 语句
    //
    // TODO:
    // - [ ] 实现表达式求值（evaluate_expression）
    // - [ ] 实现 WHERE 条件匹配（evaluate_condition）
    // - [ ] 支持更新多个列
    // - [ ] 支持算术表达式（如 SET age = age + 1）
    // - [ ] 添加类型检查
    // - [ ] 支持子查询作为值
    //
    fn execute_update(&mut self, stmt: UpdateStmt) -> Result<ExecutionResult, ExecutorError> {
        let table_name = stmt.table_name.clone();
        
        // 获取表 Schema
        let schema = match self.get_table_schema(&table_name) {
            Ok(s) => s,
            Err(e) => return Ok(e),
        };
        
        // 获取所有记录 RID
        let all_rids = match self.scan_all_rids(&table_name) {
            Ok(r) => r,
            Err(e) => return Ok(e),
        };
        
        let mut updated_count = 0;
        
        for rid in all_rids {
            // 读取当前记录
            let context = match self.get_context_mut() {
                Ok(ctx) => ctx,
                Err(e) => return Ok(e),
            };
            
            let handler = context.table_manager.open_tables.get_mut(&table_name)
                .expect("Table should be opened");
            
            let record = match handler.get(rid) {
                Ok(r) => r,
                Err(_) => continue,
            };
            
            // 检查 WHERE 条件
            if let Some(ref where_clause) = stmt.where_clause {
                match Self::evaluate_where_condition(&schema, &record, where_clause) {
                    Ok(true) => {} // 条件满足，继续更新
                    Ok(false) => continue, // 条件不满足，跳过
                    Err(e) => return Ok(ExecutionResult::Error(e)),
                }
            }
            
            // 应用更新
            let new_record = match Self::apply_updates(&schema, &record, &stmt.assignments) {
                Ok(r) => r,
                Err(e) => return Ok(ExecutionResult::Error(e)),
            };
            
            // 重新获取 handler（因为借用规则）
            let context = match self.get_context_mut() {
                Ok(ctx) => ctx,
                Err(e) => return Ok(e),
            };
            
            let handler = context.table_manager.open_tables.get_mut(&table_name)
                .expect("Table should be opened");
            
            // 使用底层 update 方法
            match handler.update(rid, &new_record) {
                Ok(_) => updated_count += 1,
                Err(e) => return Ok(ExecutionResult::Error(format!("Failed to update record: {}", e))),
            }
        }
        
        // 刷新到磁盘
        if let Err(e) = self.flush_table(&table_name) {
            return Ok(e);
        }
        
        Ok(ExecutionResult::Success(
            format!("{} row(s) updated in '{}'", updated_count, table_name)
        ))
    }
    
    // 执行 DELETE 语句
    //
    // TODO:
    // - [ ] 支持复杂条件（AND/OR/NOT）
    // - [ ] 支持子查询条件
    // - [ ] 添加删除前确认机制（可选）
    // - [ ] 支持 TRUNCATE TABLE 快速清空
    //
    fn execute_delete(&mut self, stmt: DeleteStmt) -> Result<ExecutionResult, ExecutorError> {
        let table_name = stmt.table_name.clone();
        
        // 获取表 Schema（用于 WHERE 条件）
        let schema = match self.get_table_schema(&table_name) {
            Ok(s) => s,
            Err(e) => return Ok(e),
        };
        
        // 使用 scan_all_rids 获取所有记录（底层使用 list_valid_rids）
        let all_rids = match self.scan_all_rids(&table_name) {
            Ok(r) => r,
            Err(e) => return Ok(e),
        };
        
        // 收集需要删除的 RID
        let mut rids_to_delete = Vec::new();
        
        if stmt.where_clause.is_none() {
            // 无 WHERE 子句，删除所有记录
            rids_to_delete = all_rids;
        } else {
            // 有 WHERE 子句，需要过滤
            let where_clause = stmt.where_clause.as_ref().unwrap();
            
            let context = match self.get_context_mut() {
                Ok(ctx) => ctx,
                Err(e) => return Ok(e),
            };
            
            let handler = context.table_manager.open_tables.get_mut(&table_name)
                .expect("Table should be opened");
            
            for rid in all_rids {
                let record = match handler.get(rid) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                
                match Self::evaluate_where_condition(&schema, &record, where_clause) {
                    Ok(true) => rids_to_delete.push(rid),
                    Ok(false) => continue,
                    Err(e) => return Ok(ExecutionResult::Error(e)),
                }
            }
        }
        
        // 执行删除
        let mut deleted_count = 0;
        
        let context = match self.get_context_mut() {
            Ok(ctx) => ctx,
            Err(e) => return Ok(e),
        };
        
        let handler = context.table_manager.open_tables.get_mut(&table_name)
            .expect("Table should be opened");
        
        for rid in rids_to_delete {
            match handler.delete(rid) {
                Ok(()) => deleted_count += 1,
                Err(e) => return Ok(ExecutionResult::Error(format!("Failed to delete record: {}", e))),
            }
        }
        
        // 刷新到磁盘
        if let Err(e) = self.flush_table(&table_name) {
            return Ok(e);
        }
        
        Ok(ExecutionResult::Success(
            format!("{} row(s) deleted from '{}'", deleted_count, table_name)
        ))
    }
    
    // ==================== 记录序列化/反序列化 ====================
    
    // 将字面量列表转换为记录字节
    fn literals_to_record(
        schema: &TableSchema,
        columns: &Option<Vec<String>>,
        values: &[Literal],
    ) -> Result<Vec<u8>, String> {
        let mut record = Vec::new();
        
        if let Some(col_names) = columns {
            // 指定列名，需要按表结构顺序填充
            let mut value_map: std::collections::HashMap<&str, &Literal> = std::collections::HashMap::new();
            for (i, col_name) in col_names.iter().enumerate() {
                value_map.insert(col_name.as_str(), &values[i]);
            }
            
            for col_def in &schema.columns {
                if let Some(literal) = value_map.get(col_def.name.as_str()) {
                    Self::append_literal(&mut record, literal, &col_def.data_type)?;
                } else {
                    return Err(format!("Missing value for column '{}'", col_def.name));
                }
            }
        } else {
            // 按顺序填充
            if values.len() != schema.columns.len() {
                return Err(format!(
                    "Column count mismatch: table has {} columns, got {} values",
                    schema.columns.len(), values.len()
                ));
            }
            
            for (i, literal) in values.iter().enumerate() {
                Self::append_literal(&mut record, literal, &schema.columns[i].data_type)?;
            }
        }
        
        Ok(record)
    }
    
    // 将单个字面量序列化到记录中
    fn append_literal(record: &mut Vec<u8>, literal: &Literal, data_type: &DataType) -> Result<(), String> {
        match (literal, data_type) {
            (Literal::Integer(n), DataType::Int32) => {
                record.extend_from_slice(&(*n as i32).to_le_bytes());
                Ok(())
            }
            (Literal::Boolean(b), DataType::Int32) => {
                let val = if *b { 1i32 } else { 0i32 };
                record.extend_from_slice(&val.to_le_bytes());
                Ok(())
            }
            (Literal::String(s), DataType::Varchar) => {
                let bytes = s.as_bytes();
                record.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                record.extend_from_slice(bytes);
                Ok(())
            }
            (Literal::String(s), DataType::Char(n)) => {
                let mut char_bytes = vec![0u8; *n];
                let s_bytes = s.as_bytes();
                let copy_len = s_bytes.len().min(*n);
                char_bytes[..copy_len].copy_from_slice(&s_bytes[..copy_len]);
                record.extend_from_slice(&char_bytes);
                Ok(())
            }
            (Literal::Null, _) => {
                // TODO: 实现 NULL 值处理（需要 NULL bitmap）
                Err("NULL values not fully supported yet".to_string())
            }
            _ => Err(format!(
                "Type mismatch: cannot convert {:?} to {:?}",
                literal, data_type
            )),
        }
    }
    
    // ==================== WHERE 条件求值 ====================
    //
    // TODO:
    // - [ ] 支持 LIKE 模式匹配
    // - [ ] 支持 IN 列表
    // - [ ] 支持 BETWEEN 范围
    // - [ ] 支持 IS NULL / IS NOT NULL
    //
    
    // 求值 WHERE 条件
    fn evaluate_where_condition(
        schema: &TableSchema,
        record: &[u8],
        where_clause: &WhereClause,
    ) -> Result<bool, String> {
        Self::evaluate_expression_bool(schema, record, &where_clause.condition)
    }
    
    // 求值表达式并返回布尔值
    fn evaluate_expression_bool(
        schema: &TableSchema,
        record: &[u8],
        expr: &Expression,
    ) -> Result<bool, String> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        let l = Self::evaluate_expression_bool(schema, record, left)?;
                        let r = Self::evaluate_expression_bool(schema, record, right)?;
                        Ok(l && r)
                    }
                    BinaryOperator::Or => {
                        let l = Self::evaluate_expression_bool(schema, record, left)?;
                        let r = Self::evaluate_expression_bool(schema, record, right)?;
                        Ok(l || r)
                    }
                    BinaryOperator::Eq | BinaryOperator::Ne |
                    BinaryOperator::Lt | BinaryOperator::Le |
                    BinaryOperator::Gt | BinaryOperator::Ge => {
                        let left_val = Self::evaluate_expression_value(schema, record, left)?;
                        let right_val = Self::evaluate_expression_value(schema, record, right)?;
                        Self::compare_values(&left_val, &right_val, op)
                    }
                    _ => Err(format!("Unsupported operator in WHERE: {:?}", op)),
                }
            }
            Expression::Literal(Literal::Boolean(b)) => Ok(*b),
            _ => Err("Invalid WHERE condition expression".to_string()),
        }
    }
    
    // 求值表达式并返回值
    fn evaluate_expression_value(
        schema: &TableSchema,
        record: &[u8],
        expr: &Expression,
    ) -> Result<ExprValue, String> {
        match expr {
            Expression::Literal(lit) => Ok(ExprValue::from_literal(lit)),
            Expression::Column(col_name) => {
                Self::read_column_value(schema, record, col_name)
            }
            Expression::Parenthesized(inner) => {
                Self::evaluate_expression_value(schema, record, inner)
            }
            _ => Err(format!("Unsupported expression type: {:?}", expr)),
        }
    }
    
    // 从记录中读取列值
    fn read_column_value(
        schema: &TableSchema,
        record: &[u8],
        col_name: &str,
    ) -> Result<ExprValue, String> {
        // 找到列的位置
        let col_idx = schema.columns.iter().position(|c| c.name == col_name)
            .ok_or_else(|| format!("Column '{}' not found", col_name))?;
        
        let col_def = &schema.columns[col_idx];
        
        // 计算偏移量
        let mut offset = 0;
        for i in 0..col_idx {
            offset += Self::column_size(&schema.columns[i].data_type, record, offset);
        }
        
        // 读取值
        match &col_def.data_type {
            DataType::Int32 => {
                if offset + 4 <= record.len() {
                    let bytes = &record[offset..offset + 4];
                    let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    Ok(ExprValue::Integer(val as i64))
                } else {
                    Err("Record too short for INT32".to_string())
                }
            }
            DataType::Varchar => {
                if offset + 4 <= record.len() {
                    let len_bytes = &record[offset..offset + 4];
                    let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
                    if offset + 4 + len <= record.len() {
                        let s = String::from_utf8_lossy(&record[offset + 4..offset + 4 + len]).to_string();
                        Ok(ExprValue::String(s))
                    } else {
                        Err("Record too short for VARCHAR data".to_string())
                    }
                } else {
                    Err("Record too short for VARCHAR length".to_string())
                }
            }
            DataType::Char(n) => {
                if offset + n <= record.len() {
                    let s = String::from_utf8_lossy(&record[offset..offset + n])
                        .trim_end_matches('\0')
                        .to_string();
                    Ok(ExprValue::String(s))
                } else {
                    Err("Record too short for CHAR".to_string())
                }
            }
        }
    }
    
    // 计算列在记录中的大小
    fn column_size(data_type: &DataType, record: &[u8], offset: usize) -> usize {
        match data_type {
            DataType::Int32 => 4,
            DataType::Char(n) => *n,
            DataType::Varchar => {
                if offset + 4 <= record.len() {
                    let len_bytes = &record[offset..offset + 4];
                    let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
                    4 + len
                } else {
                    4
                }
            }
        }
    }
    
    // 比较两个值
    fn compare_values(left: &ExprValue, right: &ExprValue, op: &BinaryOperator) -> Result<bool, String> {
        match (left, right) {
            (ExprValue::Integer(l), ExprValue::Integer(r)) => {
                Ok(match op {
                    BinaryOperator::Eq => l == r,
                    BinaryOperator::Ne => l != r,
                    BinaryOperator::Lt => l < r,
                    BinaryOperator::Le => l <= r,
                    BinaryOperator::Gt => l > r,
                    BinaryOperator::Ge => l >= r,
                    _ => return Err(format!("Invalid comparison operator: {:?}", op)),
                })
            }
            (ExprValue::String(l), ExprValue::String(r)) => {
                Ok(match op {
                    BinaryOperator::Eq => l == r,
                    BinaryOperator::Ne => l != r,
                    BinaryOperator::Lt => l < r,
                    BinaryOperator::Le => l <= r,
                    BinaryOperator::Gt => l > r,
                    BinaryOperator::Ge => l >= r,
                    _ => return Err(format!("Invalid comparison operator: {:?}", op)),
                })
            }
            _ => Err("Cannot compare values of different types".to_string()),
        }
    }
    
    // ==================== UPDATE 辅助方法 ====================
    
    // 应用 UPDATE 的赋值列表
    fn apply_updates(
        schema: &TableSchema,
        record: &[u8],
        assignments: &[(String, Expression)],
    ) -> Result<Vec<u8>, String> {
        let mut new_record = record.to_vec();
        
        for (col_name, expr) in assignments {
            // 找到列
            let col_idx = schema.columns.iter().position(|c| c.name == *col_name)
                .ok_or_else(|| format!("Column '{}' not found", col_name))?;
            
            let col_def = &schema.columns[col_idx];
            
            // 计算偏移量
            let mut offset = 0;
            for i in 0..col_idx {
                offset += Self::column_size(&schema.columns[i].data_type, record, offset);
            }
            
            // 求值新值
            let new_value = Self::evaluate_expression_value(schema, record, expr)?;
            
            // 写入新值
            match (&col_def.data_type, &new_value) {
                (DataType::Int32, ExprValue::Integer(n)) => {
                    if offset + 4 <= new_record.len() {
                        new_record[offset..offset + 4].copy_from_slice(&(*n as i32).to_le_bytes());
                    }
                }
                (DataType::Varchar, ExprValue::String(_)) => {
                    // VARCHAR 更新复杂，需要重建记录
                    // TODO: 实现 VARCHAR 更新
                    return Err("VARCHAR update not yet supported".to_string());
                }
                (DataType::Char(n), ExprValue::String(s)) => {
                    let mut char_bytes = vec![0u8; *n];
                    let s_bytes = s.as_bytes();
                    let copy_len = s_bytes.len().min(*n);
                    char_bytes[..copy_len].copy_from_slice(&s_bytes[..copy_len]);
                    if offset + n <= new_record.len() {
                        new_record[offset..offset + n].copy_from_slice(&char_bytes);
                    }
                }
                _ => return Err(format!("Type mismatch in UPDATE for column '{}'", col_name)),
            }
        }
        
        Ok(new_record)
    }
}

// ==================== 表达式值类型 ====================

// 表达式求值结果
#[derive(Debug, Clone, PartialEq)]
enum ExprValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

impl ExprValue {
    fn from_literal(lit: &Literal) -> Self {
        match lit {
            Literal::Integer(n) => ExprValue::Integer(*n),
            Literal::Float(f) => ExprValue::Float(*f),
            Literal::String(s) => ExprValue::String(s.clone()),
            Literal::Boolean(b) => ExprValue::Boolean(*b),
            Literal::Null => ExprValue::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    
    #[test]
    fn test_where_clause_select() {
        // 清理旧数据
        let _ = std::fs::remove_dir_all("./test_data/executor_where_select");
        
        let mut db_mgr = DatabaseManager::new("./test_data/executor_where_select").unwrap();
        let mut executor = StatementExecutor::new(&mut db_mgr);

        // 设置环境
        executor.execute("CREATE DATABASE testdb").unwrap();
        executor.execute("USE testdb").unwrap();
        executor.execute("CREATE TABLE users (id INT NOT NULL, age INT NOT NULL)").unwrap();
        
        // 插入数据
        executor.execute("INSERT INTO users VALUES (1, 20)").unwrap();
        executor.execute("INSERT INTO users VALUES (2, 25)").unwrap();
        executor.execute("INSERT INTO users VALUES (3, 30)").unwrap();
        executor.execute("INSERT INTO users VALUES (4, 18)").unwrap();
        
        // 测试 WHERE 等于
        let result = executor.execute("SELECT * FROM users WHERE id = 2").unwrap();
        if let ExecutionResult::Query(rows, _) = result {
            assert_eq!(rows.len(), 1, "Should find 1 row with id = 2");
        } else {
            panic!("Expected Query result");
        }
        
        // 测试 WHERE 大于
        let result = executor.execute("SELECT * FROM users WHERE age > 20").unwrap();
        if let ExecutionResult::Query(rows, _) = result {
            assert_eq!(rows.len(), 2, "Should find 2 rows with age > 20");
        } else {
            panic!("Expected Query result");
        }
        
        // 测试 WHERE AND
        let result = executor.execute("SELECT * FROM users WHERE age >= 20 AND age <= 25").unwrap();
        if let ExecutionResult::Query(rows, _) = result {
            assert_eq!(rows.len(), 2, "Should find 2 rows with 20 <= age <= 25");
        } else {
            panic!("Expected Query result");
        }
        
        // 测试无 WHERE（返回所有）
        let result = executor.execute("SELECT * FROM users").unwrap();
        if let ExecutionResult::Query(rows, _) = result {
            assert_eq!(rows.len(), 4, "Should find all 4 rows");
        } else {
            panic!("Expected Query result");
        }

        // 清理
        let _ = std::fs::remove_dir_all("./test_data/executor_where_select");
    }
    
    #[test]
    fn test_where_clause_delete() {
        // 清理旧数据
        let _ = std::fs::remove_dir_all("./test_data/executor_where_delete");
        
        let mut db_mgr = DatabaseManager::new("./test_data/executor_where_delete").unwrap();
        let mut executor = StatementExecutor::new(&mut db_mgr);

        // 设置环境
        executor.execute("CREATE DATABASE testdb").unwrap();
        executor.execute("USE testdb").unwrap();
        executor.execute("CREATE TABLE users (id INT NOT NULL, age INT NOT NULL)").unwrap();
        
        // 插入数据
        executor.execute("INSERT INTO users VALUES (1, 20)").unwrap();
        executor.execute("INSERT INTO users VALUES (2, 25)").unwrap();
        executor.execute("INSERT INTO users VALUES (3, 30)").unwrap();
        
        // 删除 age = 25 的记录
        let result = executor.execute("DELETE FROM users WHERE age = 25").unwrap();
        if let ExecutionResult::Success(msg) = result {
            assert!(msg.contains("1 row"), "Should delete 1 row");
        } else {
            panic!("Expected Success result");
        }
        
        // 验证剩余记录
        let result = executor.execute("SELECT * FROM users").unwrap();
        if let ExecutionResult::Query(rows, _) = result {
            assert_eq!(rows.len(), 2, "Should have 2 rows remaining");
        } else {
            panic!("Expected Query result");
        }

        // 清理
        let _ = std::fs::remove_dir_all("./test_data/executor_where_delete");
    }
    
    #[test]
    fn test_where_clause_update() {
        // 清理旧数据
        let _ = std::fs::remove_dir_all("./test_data/executor_where_update");
        
        let mut db_mgr = DatabaseManager::new("./test_data/executor_where_update").unwrap();
        let mut executor = StatementExecutor::new(&mut db_mgr);

        // 设置环境
        executor.execute("CREATE DATABASE testdb").unwrap();
        executor.execute("USE testdb").unwrap();
        executor.execute("CREATE TABLE users (id INT NOT NULL, age INT NOT NULL)").unwrap();
        
        // 插入数据
        executor.execute("INSERT INTO users VALUES (1, 20)").unwrap();
        executor.execute("INSERT INTO users VALUES (2, 25)").unwrap();
        executor.execute("INSERT INTO users VALUES (3, 30)").unwrap();
        
        // 更新 id = 2 的记录
        let result = executor.execute("UPDATE users SET age = 26 WHERE id = 2").unwrap();
        if let ExecutionResult::Success(msg) = result {
            assert!(msg.contains("1 row"), "Should update 1 row");
        } else {
            panic!("Expected Success result, got {:?}", result);
        }
        
        // 验证更新结果
        let result = executor.execute("SELECT * FROM users WHERE age = 26").unwrap();
        if let ExecutionResult::Query(rows, _) = result {
            assert_eq!(rows.len(), 1, "Should find 1 row with age = 26");
        } else {
            panic!("Expected Query result");
        }

        // 清理
        let _ = std::fs::remove_dir_all("./test_data/executor_where_update");
    }
}
