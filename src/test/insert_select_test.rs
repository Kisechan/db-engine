/// 测试 INSERT + SELECT 数据持久化流程
/// 验证数据能够正确写入磁盘并读取

#[cfg(test)]
mod tests {
    use crate::rm::database_manager::DatabaseManager;
    use crate::sql::lexer::Lexer;
    use crate::sql::parser::Parser;
    use crate::exec::statement_executor::StatementExecutor;
    use std::fs;
    use std::path::Path;

    /// 测试基本的 INSERT → SELECT 工作流程
    #[test]
    fn test_insert_select_basic() {
        let test_db_path = "./test_data/insert_select_test";
        let test_db_name = "testdb";
        
        // 清理测试环境
        if Path::new(test_db_path).exists() {
            fs::remove_dir_all(test_db_path).ok();
        }
        fs::create_dir_all(test_db_path).expect("Failed to create test directory");

        // 初始化数据库管理器
        let mut db_manager = DatabaseManager::new(test_db_path.to_string())
            .expect("Failed to create DatabaseManager");
        
        // 创建数据库
        db_manager.create_database(test_db_name, false).expect("Failed to create database");
        db_manager.use_database(test_db_name).expect("Failed to use database");

        // 获取执行器
        let mut executor = StatementExecutor::new(&mut db_manager);

        // 1. 创建表
        let create_sql = "CREATE TABLE users (id INT, name VARCHAR(50), age INT)";
        let result = executor.execute(create_sql).expect("Failed to execute CREATE TABLE");
        println!("CREATE TABLE result: {:?}", result);

        // 2. 插入数据
        let insert_sql = "INSERT INTO users VALUES (1, 'kisechan', 28)";
        let result = executor.execute(insert_sql).expect("Failed to execute INSERT");
        println!("INSERT result: {:?}", result);
        
        // 验证插入成功
        match result {
            crate::exec::statement_executor::ExecutionResult::Success(msg) => {
                assert!(msg.contains("1 row(s) inserted"), "Expected '1 row(s) inserted', got: {}", msg);
            }
            _ => panic!("INSERT should return Success"),
        }

        // 3. 查询数据（验证数据已持久化）
        let select_sql = "SELECT * FROM users";
        let result = executor.execute(select_sql).expect("Failed to execute SELECT");
        println!("SELECT result: {:?}", result);

        // 验证查询结果
        match result {
            crate::exec::statement_executor::ExecutionResult::Query(rows, _schema) => {
                // 应该返回至少 1 行数据
                assert_eq!(rows.len(), 1, "Expected 1 row, got {}", rows.len());
                
                // 验证数据内容（字节序列）
                let record = &rows[0].data;
                println!("Record bytes: {:?}", record);
                
                // 简单验证：记录长度应该 > 0
                assert!(record.len() > 0, "Record data should not be empty");
            }
            _ => panic!("SELECT should return Query result with data, got: {:?}", result),
        }

        // 清理测试数据
        fs::remove_dir_all(test_db_path).ok();
    }

    /// 测试多行插入和查询
    #[test]
    fn test_insert_select_multiple_rows() {
        let test_db_path = "./test_data/insert_select_multi";
        let test_db_name = "testdb";
        
        // 清理测试环境
        if Path::new(test_db_path).exists() {
            fs::remove_dir_all(test_db_path).ok();
        }
        fs::create_dir_all(test_db_path).expect("Failed to create test directory");

        // 初始化
        let mut db_manager = DatabaseManager::new(test_db_path.to_string())
            .expect("Failed to create DatabaseManager");
        db_manager.create_database(test_db_name, false).expect("Failed to create database");
        db_manager.use_database(test_db_name).expect("Failed to use database");
        let mut executor = StatementExecutor::new(&mut db_manager);

        // 创建表
        let create_sql = "CREATE TABLE products (id INT, name VARCHAR(100), price INT)";
        executor.execute(create_sql).expect("Failed to execute CREATE TABLE");

        // 插入多条数据
        let inserts = vec![
            "INSERT INTO products VALUES (1, 'Apple', 10)",
            "INSERT INTO products VALUES (2, 'Banana', 5)",
            "INSERT INTO products VALUES (3, 'Orange', 8)",
        ];

        for insert_sql in inserts {
            let result = executor.execute(insert_sql).expect("Failed to execute INSERT");
            
            match result {
                crate::exec::statement_executor::ExecutionResult::Success(msg) => {
                    assert!(msg.contains("1 row(s) inserted"), "INSERT failed: {}", msg);
                }
                _ => panic!("INSERT should return Success"),
            }
        }

        // 查询所有数据
        let select_sql = "SELECT * FROM products";
        let result = executor.execute(select_sql).expect("Failed to execute SELECT");
        
        match result {
            crate::exec::statement_executor::ExecutionResult::Query(rows, _schema) => {
                // 应该返回 3 行数据
                assert_eq!(rows.len(), 3, "Expected 3 rows, got {}", rows.len());
            }
            _ => panic!("SELECT should return Query result with data"),
        }

        // 清理
        fs::remove_dir_all(test_db_path).ok();
    }

    /// 测试跨会话持久化（关闭后重新打开）
    #[test]
    fn test_insert_select_persistence_across_sessions() {
        let test_db_path = "./test_data/insert_select_persist";
        let test_db_name = "testdb";
        
        // 清理测试环境
        if Path::new(test_db_path).exists() {
            fs::remove_dir_all(test_db_path).ok();
        }
        fs::create_dir_all(test_db_path).expect("Failed to create test directory");

        // === 第一个会话：创建表并插入数据 ===
        {
            let mut db_manager = DatabaseManager::new(test_db_path.to_string())
                .expect("Failed to create DatabaseManager");
            db_manager.create_database(test_db_name, false).expect("Failed to create database");
            db_manager.use_database(test_db_name).expect("Failed to use database");
            let mut executor = StatementExecutor::new(&mut db_manager);

            // 创建表
            let create_sql = "CREATE TABLE settings (key VARCHAR(50), value VARCHAR(100))";
            executor.execute(create_sql).expect("Failed to execute CREATE TABLE");

            // 插入数据
            let insert_sql = "INSERT INTO settings VALUES ('theme', 'dark')";
            executor.execute(insert_sql).expect("Failed to execute INSERT");

            // 显式释放 db_manager（模拟关闭数据库）
        }

        // === 第二个会话：重新打开数据库并查询 ===
        {
            let mut db_manager = DatabaseManager::new(test_db_path.to_string())
                .expect("Failed to create DatabaseManager");
            db_manager.use_database(test_db_name).expect("Failed to use existing database");
            let mut executor = StatementExecutor::new(&mut db_manager);

            // 查询之前插入的数据
            let select_sql = "SELECT * FROM settings";
            let result = executor.execute(select_sql).expect("Failed to execute SELECT");

            // 验证数据仍然存在
            match result {
                crate::exec::statement_executor::ExecutionResult::Query(rows, _schema) => {
                    assert_eq!(rows.len(), 1, "Data not persisted across sessions, expected 1 row, got {}", rows.len());
                }
                crate::exec::statement_executor::ExecutionResult::Error(msg) => {
                    panic!("SELECT failed with error: {}", msg);
                }
                _ => panic!("SELECT should return Query result with persisted data, got: {:?}", result),
            }
        }

        // 清理
        fs::remove_dir_all(test_db_path).ok();
    }
}
