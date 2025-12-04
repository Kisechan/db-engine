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
use std::env;

fn main() -> Result<(), String> {
    // 初始化日志系统
    env_logger::init();
    
    let args: Vec<String> = env::args().collect();
    
    // 解析命令行参数
    if args.len() > 1 {
        match args[1].as_str() {
            "--test" => {
                // 运行测试
                println!("Running tests...\n");
                println!("Running Test1\n:");
                test::task1::task1()
                    .map_err(|e| format!("Test failed: {}", e))?;
                println!("\n\n");
                println!("Running Test2\n:");
                test::task2::task2()
                    .map_err(|e| format!("Test failed: {}", e))?;
                println!("\n\n");
                println!("Running Test3\n:");
                test::task3::task3()
                    .map_err(|e| format!("Test failed: {}", e))?;
                println!("\n\n");
                println!("Running Test4\n:");
                test::task4::task4()
                    .map_err(|e| format!("Test failed: {}", e))?;
                return Ok(());
            }
            "--help" | "-h" => {
                // 显示帮助信息
                print_help();
                return Ok(());
            }
            _ => {
                eprintln!("Unknown option: {}", args[1]);
                eprintln!("Use --help to see available options\n");
                print_help();
                return Err(format!("Invalid argument: {}", args[1]));
            }
        }
    }
    
    // 默认启动 REPL
    let db_manager = DatabaseManager::new("./data")
        .map_err(|e| format!("Failed to initialize database manager: {}", e))?;
    
    let mut repl = Repl::new(db_manager)
        .map_err(|e| format!("Failed to initialize REPL: {}", e))?;
    
    repl.start()
        .map_err(|e| format!("REPL error: {}", e))?;
    
    Ok(())
}

fn print_help() {
    println!("Kisechan's DB-Engine v1.3.0");
    println!("A relational database engine written in Rust\n");
    println!("USAGE:");
    println!("    cargo run [OPTIONS]\n");
    println!("OPTIONS:");
    println!("    (none)        Start interactive database REPL (default)");
    println!("    --test        Run system tests");
    println!("    --help, -h    Display this help message\n");
    println!("EXAMPLES:");
    println!("    cargo run                # Start interactive database");
    println!("    cargo run -- --test      # Run tests");
    println!("    cargo run -- --help      # Show this help\n");
    println!("For more information, see REPL_GUIDE.md");
}