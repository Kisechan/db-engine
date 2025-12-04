// 交互式命令行界面 (REPL - Read-Eval-Print Loop)
//
// # 功能概述
//
// Repl 提供了一个用户友好的交互式数据库命令行界面：
// - 支持完整的 SQL 语句执行
// - 命令历史和自动补全（使用 rustyline）
// - 特殊命令（.help, .exit, .history 等）
// - 格式化的查询结果输出（使用 prettytable）
// - 多行 SQL 输入支持
//
// # 使用示例
//
// ```rust,ignore
// use db_engine::repl::Repl;
// use db_engine::rm::DatabaseManager;
//
// let db_mgr = DatabaseManager::new("./data")?;
// let mut repl = Repl::new(db_mgr);
// repl.start()?;
// ```
//

use crate::rm::database_manager::DatabaseManager;
use crate::exec::statement_executor::{StatementExecutor, ExecutionResult};
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result as RustylineResult};
use rustyline::history::History;
use prettytable::{Table, Row, Cell, format};

// REPL 结构体
pub struct Repl {
    db_manager: DatabaseManager,
    editor: DefaultEditor,
}

impl Repl {
    // 创建新的 REPL 实例
    pub fn new(db_manager: DatabaseManager) -> RustylineResult<Self> {
        let editor = DefaultEditor::new()?;
        
        Ok(Repl {
            db_manager,
            editor,
        })
    }

    // 启动 REPL 主循环
    pub fn start(&mut self) -> RustylineResult<()> {
        self.print_welcome();
        
        let mut buffer = String::new();
        
        loop {
            // 构建提示符
            let prompt = self.build_prompt(&buffer);
            
            // 读取用户输入
            match self.editor.readline(&prompt) {
                Ok(line) => {
                    // 添加到历史
                    let _ = self.editor.add_history_entry(&line);
                    
                    // 处理输入
                    if let Err(should_exit) = self.handle_input(&line, &mut buffer) {
                        if should_exit {
                            break;
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    // Ctrl+C: 清空当前缓冲区
                    if !buffer.is_empty() {
                        println!("^C");
                        buffer.clear();
                    } else {
                        println!("(To exit, press Ctrl+D or type .exit)");
                    }
                }
                Err(ReadlineError::Eof) => {
                    // Ctrl+D: 退出
                    println!("Goodbye!");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    break;
                }
            }
        }
        
        // 关闭所有数据库
        if let Err(e) = self.db_manager.close_all() {
            eprintln!("Warning: Failed to close databases: {}", e);
        }
        
        Ok(())
    }

    // 构建提示符
    fn build_prompt(&self, buffer: &str) -> String {
        if !buffer.is_empty() {
            // 多行输入时显示续行提示符
            "      -> ".to_string()
        } else {
            // 显示当前数据库名称
            match self.db_manager.current_database_name() {
                Some(db_name) => format!("Kisechan's DB-Engine [{}]> ", db_name),
                None => "Kisechan's DB-Engine> ".to_string(),
            }
        }
    }

    // 处理用户输入
    //
    // # 返回
    // - `Ok(())`: 继续执行
    // - `Err(true)`: 应该退出
    // - `Err(false)`: 清空缓冲区并继续
    fn handle_input(&mut self, line: &str, buffer: &mut String) -> Result<(), bool> {
        let trimmed = line.trim();
        
        // 空行
        if trimmed.is_empty() {
            return Ok(());
        }
        
        // 检查特殊命令（只在缓冲区为空时）
        if buffer.is_empty() && trimmed.starts_with('.') {
            return self.handle_special_command(trimmed);
        }
        
        // 累积到缓冲区
        if !buffer.is_empty() {
            buffer.push(' ');
        }
        buffer.push_str(trimmed);
        
        // 检查是否以分号结尾（完整语句）
        if buffer.ends_with(';') {
            // 移除尾部分号
            let sql = buffer.trim_end_matches(';').trim().to_string();
            buffer.clear();
            
            // 执行 SQL
            self.execute_sql(&sql);
        }
        
        Ok(())
    }

    // 处理特殊命令
    fn handle_special_command(&mut self, cmd: &str) -> Result<(), bool> {
        match cmd {
            ".help" | ".h" => {
                self.print_help();
                Ok(())
            }
            ".exit" | ".quit" | ".q" => {
                println!("Goodbye!");
                Err(true) // 退出
            }
            ".clear" | ".c" => {
                // 清屏
                print!("\x1B[2J\x1B[1;1H");
                Ok(())
            }
            ".history" => {
                self.print_history();
                Ok(())
            }
            ".databases" | ".dbs" => {
                self.execute_sql("SHOW DATABASES");
                Ok(())
            }
            ".tables" => {
                self.execute_sql("SHOW TABLES");
                Ok(())
            }
            _ => {
                eprintln!("Unknown command: {}. Type .help for available commands.", cmd);
                Ok(())
            }
        }
    }

    // 执行 SQL 语句
    fn execute_sql(&mut self, sql: &str) {
        if sql.is_empty() {
            return;
        }
        
        let mut executor = StatementExecutor::new(&mut self.db_manager);
        
        match executor.execute(sql) {
            Ok(result) => self.print_result(result),
            Err(e) => self.print_error(&format!("Execution error: {}", e)),
        }
    }

    // 打印执行结果
    fn print_result(&self, result: ExecutionResult) {
        match result {
            ExecutionResult::Success(msg) => {
                println!("\x1b[32m✓\x1b[0m {}", msg);
            }
            ExecutionResult::Query(records) => {
                if records.is_empty() {
                    println!("(0 rows)");
                } else {
                    self.print_query_result(&records);
                    println!("\n({} row{})", records.len(), if records.len() == 1 { "" } else { "s" });
                }
            }
            ExecutionResult::RowsAffected(count) => {
                println!("\x1b[32m✓\x1b[0m {} row{} affected", count, if count == 1 { "" } else { "s" });
            }
            ExecutionResult::Error(msg) => {
                self.print_error(&msg);
            }
        }
    }

    // 打印查询结果（表格格式）
    fn print_query_result(&self, records: &[crate::exec::iterator::ExecutorRecord]) {
        let mut table = Table::new();
        
        // 设置表格格式
        table.set_format(*format::consts::FORMAT_BOX_CHARS);
        
        // 添加表头（RID + Data 列）
        table.add_row(Row::new(vec![
            Cell::new("RID"),
            Cell::new("Data (hex)"),
        ]));
        
        // 添加数据行
        for record in records {
            let rid_str = format!("({}, {})", record.rid.page_id, record.rid.slot_id);
            let data_str = record.data.iter()
                .take(32) // 限制显示前32字节
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            
            let data_display = if record.data.len() > 32 {
                format!("{} ... ({} bytes)", data_str, record.data.len())
            } else {
                format!("{} ({} bytes)", data_str, record.data.len())
            };
            
            table.add_row(Row::new(vec![
                Cell::new(&rid_str),
                Cell::new(&data_display),
            ]));
        }
        
        table.printstd();
    }

    // 打印错误信息（红色）
    fn print_error(&self, msg: &str) {
        eprintln!("\x1b[31m✗ Error:\x1b[0m {}", msg);
    }

    // 打印欢迎信息
    fn print_welcome(&self) {
        println!("\x1b[1;36m"); // 青色粗体
        println!("╔═══════════════════════════════════════════════════════════════╗");
        println!("║                                                               ║");
        println!("║                 \x1b[1;33mKisechan's DB-Engine v1.3.0\x1b[1;36m                   ║");
        println!("║                                                               ║");
        println!("║           A relational database engine written in Rust        ║");
        println!("║                                                               ║");
        println!("╠═══════════════════════════════════════════════════════════════╣");
        println!("║                                                               ║");
        println!("║  \x1b[1;32m      GitHub:\x1b[1;36m https://github.com/Kisechan/db-engine          ║");
        println!("║                                                               ║");
        println!("║  \x1b[1;35m      Type .help to see available commands\x1b[1;36m                   ║");
        println!("║  \x1b[1;35m      Type .exit or press Ctrl+D to quit\x1b[1;36m                     ║");
        println!("║                                                               ║");
        println!("╚═══════════════════════════════════════════════════════════════╝");
        println!("\x1b[0m"); // 重置颜色
        println!();
    }

    // 打印帮助信息
    fn print_help(&self) {
        println!("\n\x1b[1;36mKisechan's DB-Engine Help\x1b[0m\n");
        
        println!("\x1b[1;33mSpecial Commands:\x1b[0m");
        println!("  .help, .h         Show this help message");
        println!("  .exit, .quit, .q  Exit the program");
        println!("  .clear, .c        Clear the screen");
        println!("  .history          Show command history");
        println!("  .databases, .dbs  List all databases");
        println!("  .tables           List tables in current database");
        
        println!("\n\x1b[1;33mDatabase Management:\x1b[0m");
        println!("  CREATE DATABASE [IF NOT EXISTS] <name>");
        println!("  DROP DATABASE [IF EXISTS] <name>");
        println!("  USE <database_name>");
        println!("  SHOW DATABASES");
        println!("  SHOW TABLES");
        
        println!("\n\x1b[1;33mTable Management:\x1b[0m");
        println!("  CREATE TABLE <name> (col1 TYPE, col2 TYPE, ...)");
        println!("  DROP TABLE [IF EXISTS] <name>");
        
        println!("\n\x1b[1;33mSupported Data Types:\x1b[0m");
        println!("  INT, INTEGER       32-bit integer");
        println!("  FLOAT, REAL        Floating point number");
        println!("  VARCHAR(n)         Variable-length string");
        println!("  CHAR(n)            Fixed-length string");
        
        println!("\n\x1b[1;33mQuery:\x1b[0m");
        println!("  SELECT [DISTINCT] <columns> FROM <table>");
        println!("         [WHERE <condition>]");
        println!("         [ORDER BY <column> [ASC|DESC]]");
        println!("         [LIMIT <n>]");
        
        println!("\n\x1b[1;33mExamples:\x1b[0m");
        println!("  CREATE DATABASE KisechansDB;");
        println!("  USE KisechansDB;");
        println!("  CREATE TABLE users (id INT, name VARCHAR(50));");
        println!("  SELECT * FROM users WHERE id > 10;");
        
        println!("\n\x1b[1;33mTips:\x1b[0m");
        println!("  • SQL statements must end with a semicolon (;)");
        println!("  • Multi-line input is supported");
        println!("  • Press Ctrl+C to cancel current input");
        println!("  • Press Ctrl+D or type .exit to quit");
        println!("  • Use arrow keys to navigate history");
        
        println!();
    }

    // 打印命令历史
    fn print_history(&self) {
        println!("\n\x1b[1;36m=== Command History ===\x1b[0m\n");
        
        let history = self.editor.history();
        if history.is_empty() {
            println!("(No history yet)");
        } else {
            for (i, entry) in history.iter().enumerate() {
                println!("  {:4}: {}", i + 1, entry);
            }
        }
        
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output() {
        let db_mgr = DatabaseManager::new("./test_data/repl_test2").unwrap();
        let repl = Repl::new(db_mgr).unwrap();
        repl.print_welcome();
    }

    #[test]
    fn test_repl_creation() {
        let db_mgr = DatabaseManager::new("./test_data/repl_test").unwrap();
        let repl = Repl::new(db_mgr);
        assert!(repl.is_ok());
        
        // 清理
        let _ = std::fs::remove_dir_all("./test_data/repl_test");
    }

    #[test]
    fn test_prompt_building() {
        let db_mgr = DatabaseManager::new("./test_data/repl_test2").unwrap();
        let repl = Repl::new(db_mgr).unwrap();
        
        // 空缓冲区，无数据库
        let prompt = repl.build_prompt("");
        assert_eq!(prompt, "Kisechan's DB-Engine> ");
        
        // 多行输入
        let prompt = repl.build_prompt("SELECT * FROM");
        assert_eq!(prompt, "      -> ");
        
        // 清理
        let _ = std::fs::remove_dir_all("./test_data/repl_test2");
    }
}
