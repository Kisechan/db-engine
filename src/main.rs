mod fm;
mod mm;
mod rm;
mod pm;
mod ix;
mod common;
mod test;
mod sql;
mod exec;
mod plan;
mod repl;

use repl::Repl;
use rm::database_manager::DatabaseManager;

fn main() -> Result<(), String> {
    // 创建数据库管理器
    let db_manager = DatabaseManager::new("./data")
        .map_err(|e| format!("Failed to initialize database manager: {}", e))?;
    
    // 启动 REPL
    let mut repl = Repl::new(db_manager)
        .map_err(|e| format!("Failed to initialize REPL: {}", e))?;
    
    repl.start()
        .map_err(|e| format!("REPL error: {}", e))?;
    
    Ok(())
}