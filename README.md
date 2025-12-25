# db-engine

这是吉林大学软件工程卓越工程师班《*数据库系统应用程序开发*》和《*系统软件综合实践*》两门课程（共 3 学分）综合起来共同完成的课程项目。课程的内容主要是了解 [Stanford CS346 课程](https://web.stanford.edu/class/cs346/2015/redbase-pf.html)的 RedBase 框架，仿照实现一个**数据库引擎**的部分功能。

使用 [![Static Badge](https://img.shields.io/badge/Rust-%23000000?style=flat&logo=rust)](https://rust-lang.org/) 实现。

## 功能实现

《*数据库系统应用程序开发*》课程要求实现的数据库主要有**四个基本功能**，具体介绍可以参考我[**博客**](https://blog.kisechan.space/)中的文章，文章中也提供了对应部分的**课程具体要求**，每一部分我也给出了对应的**测试代码**（在 `./src/test/` 路径下）：
1. [**内存、外存和记录管理**](https://blog.kisechan.space/2025/db-engine-1/)，对应 [task1](./src/test/task1.rs) 和 [*task2*](./src/test/task2.rs)（*没有要求做实验验证，所以没有对应代码*）。
2. [**索引**](https://blog.kisechan.space/2025/db-engine-2/)，对应 [task3](./src/test/task3.rs)。
3. [**SQL 处理**](https://blog.kisechan.space/2025/db-engine-3/)，对应 [task4](./src/test/task4.rs)。

在命令行中输入：
```bash
cargo run -- --test
```

可以运行这几个功能对应的测试代码。

---

《*系统软件综合实践*》课程则要求使用**面向对象**的方法将项目重新分析一遍，因此我们也给出了每个环节对本项目的分析，以 Markdown 文档的形式呈现（在 `./docs/` 路径下）：
1. [*分析类的定义*](./docs/1-分析类的定义.md) （2025/12/16 更新）

## 快速开始

### 环境配置

本项目依赖 ![Static Badge](https://img.shields.io/badge/Rust-2021_edition-%23000000?style=flat&logo=rust&logoColor=%23000000&labelColor=%23DCDCDC) 开发，安装 Rust 工具链（`cargo` / `rustc`，stable）即可。
 
 开发本项目使用的系统是 ![Static Badge](https://img.shields.io/badge/WSL%2FUbuntu-22.04-%23E95420?style=flat&logo=ubuntu&labelColor=%23DCDCDC) （前期）和 ![Static Badge](https://img.shields.io/badge/macOS-Tahoe_26-%23000000?style=flat&logo=macos&labelColor=%23DCDCDC&logoColor=%23000000) (后期)。

推荐使用 Rust 官方提供的 [rustup](https://rustup.rs) 进行安装配置。

安装完成后可输入下列命令确认安装完成：
```bash
rustc --version
cargo --version
```

### 启动交互式数据库（REPL）

```bash
# 开发模式
cargo run

# 生产模式
cargo run --release

# 启用日志（开发调试）
RUST_LOG=debug cargo run

# 启用日志（生产环境）
RUST_LOG=info cargo run --release
```

启动后会看到欢迎界面，可以输入 SQL 命令进行数据库操作。

### 运行测试

使用下面的命令，可以运行 `task1` ~ `task4` 的测试代码，检查测试结果。

```bash
cargo run -- --test

# 运行测试并查看日志
RUST_LOG=debug cargo run -- --test
```

### 查看帮助

```bash
cargo run -- --help
```

## 日志系统

项目集成了完整的日志系统，支持通过 `RUST_LOG` 环境变量控制日志级别：

```bash
# 开发调试 - 显示详细日志
RUST_LOG=debug cargo run

# 生产环境 - 只显示重要信息
RUST_LOG=info cargo run --release

# 只显示错误
RUST_LOG=error cargo run

# 针对特定模块
RUST_LOG=db_engine::rm=debug cargo run
```

日志级别说明：
- **`ERROR`**: 错误信息（数据库操作失败、解析错误）
- **`WARN`**: 警告信息（资源已存在等）
- **`INFO`**: 重要操作（创建/删除数据库、切换数据库）
- **`DEBUG`**: 调试信息（SQL 执行、查询计划、优化过程）

## 程序运行示例

交互式界面样式示例如下（截图版本 `v1.3.2`）：

![](./docs/images/image_v1.3.2.png)

### 交互示例

使用下面的 SQL 语句进行测试：

```sql
CREATE DATABASE testdb;
USE testdb;
SHOW TABLES;
CREATE TABLE users (id INT, name CHAR(50), age INT);
INSERT INTO users VALUES (1, 'kisechan', 28);
SELECT * FROM users;
.exit
```

可以得到下面的结果：

```
[DatabaseManager] Created base directory: "./data"
[DatabaseManager] Found 0 existing database(s): []

╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                 Kisechan's DB-Engine v1.3.3                   ║
║                                                               ║
║           A relational database engine written in Rust        ║
║                                                               ║
╠═══════════════════════════════════════════════════════════════╣
║                                                               ║
║        GitHub: https://github.com/Kisechan/db-engine          ║
║                                                               ║
║        Type .help to see available commands                   ║
║        Type .exit or press Ctrl+D to quit                     ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝


Kisechan's DB-Engine> CREATE DATABASE testdb;
[DatabaseManager] Created database directory: "./data/testdb"
[DatabaseManager] Created empty catalog file: "./data/testdb/catalog.tbl"
[DatabaseManager] Database 'testdb' created successfully
✓ Database 'testdb' created successfully
Kisechan's DB-Engine> USE testdb;
[DatabaseManager] Loading database 'testdb' from "./data/testdb"
[CatalogManager] Catalog page is all zeros, starting with empty schema
[CatalogManager] Initialized with 0 tables, next_table_id=1 (path: "./data/testdb/catalog.tbl")
[CatalogManager] Catalog page is all zeros, starting with empty schema
[CatalogManager] Initialized with 0 tables, next_table_id=1 (path: "./data/testdb/catalog.tbl")
[DatabaseManager] Switched to database 'testdb'
✓ Switched to database 'testdb'
Kisechan's DB-Engine [testdb]> SHOW TABLES;
✓ No tables found in current database
Kisechan's DB-Engine [testdb]> CREATE TABLE users (id INT, name CHAR(50), age INT);
[CatalogManager] Added table 'users' to memory cache with table_id=1
[CatalogManager] Flushed 1 tables to disk (126 bytes) at "./data/testdb/catalog.tbl"
[FileHandler] Flushed header to ./data/testdb/users.tbl: size=12 bytes, total_pages=1, free_list_len=0
[TableManager] Created table file: ./data/testdb/users.tbl with initialized header
✓ Table 'users' created successfully
Kisechan's DB-Engine [testdb]> INSERT INTO users VALUES (1, 'kisechan', 28);
[FileHandler] Loaded header from ./data/testdb/users.tbl: total_pages=1, free_list_len=0
[TableManager] Opened table: users
[FileHandler] Allocated new page 1 (total_pages now: 2)
[FileHandler] Flushed header to ./data/testdb/users.tbl: size=12 bytes, total_pages=2, free_list_len=0
[DiskManager] Page 1 beyond file size (4096), returning zero-filled page
✓ 1 row(s) inserted into 'users'
Kisechan's DB-Engine [testdb]> SELECT * FROM users;
[SELECT] Found 1 rows from table 'users'
┌────────┬────┬──────────┬─────┐
│ RID    │ id │ name     │ age │
├────────┼────┼──────────┼─────┤
│ (1, 0) │ 1  │ kisechan │ 28  │
└────────┴────┴──────────┴─────┘

(1 row)
Kisechan's DB-Engine [testdb]> .exit
Goodbye!
[DatabaseManager] Closing all databases...
[CatalogManager] Flushed 1 tables to disk (126 bytes) at "./data/testdb/catalog.tbl"
[DatabaseManager] All databases closed
[DatabaseManager] Closing all databases...
[DatabaseManager] All databases closed
```

## TODO 和未完成的部分

下面的功能*也许在可见的未来不会实现*，但是暂时先标出来：

### SQL 解析和执行

#### DDL（数据定义语言）
- [x] `CREATE DATABASE` / `DROP DATABASE`
- [x] `USE DATABASE`
- [x] `SHOW DATABASES` / `SHOW TABLES`
- [x] `CREATE TABLE` / `DROP TABLE`
- [ ] `ALTER TABLE`（添加/删除列、修改列类型）
- [ ] `CREATE INDEX` / `DROP INDEX`
- [ ] `TRUNCATE TABLE`（快速清空表）

#### DML（数据操作语言）

**`SELECT` 查询**
- [x] 基本查询：`SELECT * FROM table`
- [x] `WHERE` 子句（支持 `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`）
- [ ] 列投影：`SELECT col1, col2 FROM table`
- [ ] `ORDER BY`（排序）
- [ ] `LIMIT` / `OFFSET`（分页）
- [ ] `DISTINCT`（去重）
- [ ] `JOIN`（`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`）
- [ ] 聚合函数：`COUNT`, `SUM`, `AVG`, `MAX`, `MIN`
- [ ] `GROUP BY` / `HAVING`（分组和分组过滤）
- [ ] 子查询（`WHERE col IN (SELECT ...)`）
- [ ] `UNION` / `INTERSECT` / `EXCEPT`（集合操作）

**`INSERT` 插入**
- [x] 基本插入：`INSERT INTO table VALUES (...)`
- [x] 指定列插入：`INSERT INTO table (col1, col2) VALUES (...)`
- [ ] `INSERT ... SELECT`（从查询结果插入）
- [ ] `DEFAULT` 值支持
- [ ] 批量插入优化
- [ ] 约束检查（`PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, `CHECK`）

**`UPDATE` 更新**
- [x] 基本更新：`UPDATE table SET col = value`
- [x] `WHERE` 条件过滤
- [x] 支持 `INT` 类型列更新
- [ ] 支持 `VARCHAR` 类型列更新
- [ ] 算术表达式：`UPDATE table SET age = age + 1`
- [ ] 子查询作为值：`UPDATE table SET col = (SELECT ...)`
- [ ] 多列更新优化

**`DELETE` 删除**
- [x] 基本删除：`DELETE FROM table`
- [x] `WHERE` 条件过滤（支持 `AND` / `OR`）
- [ ] 子查询条件：`DELETE FROM table WHERE id IN (SELECT ...)`
- [ ] 级联删除（`FOREIGN KEY` 约束）

#### `WHERE` 条件表达式
- [x] 比较操作符：`=`, `!=`, `<`, `<=`, `>`, `>=`
- [x] 逻辑操作符：`AND`, `OR`
- [x] 括号表达式
- [ ] `NOT` 操作符（AST 已支持，求值需扩展）
- [ ] `LIKE` / `NOT LIKE`（模式匹配）
- [ ] `IN` / `NOT IN`（列表匹配）
- [ ] `BETWEEN ... AND ...`（范围）
- [ ] `IS NULL` / `IS NOT NULL`
- [ ] 算术表达式：`WHERE age + 10 > 30`
- [ ] 字符串函数：`CONCAT`, `SUBSTRING`, `UPPER`, `LOWER`

### 数据类型

- [x] `INT` / `INT32`
- [x] `VARCHAR`
- [x] `CHAR(n)`
- [ ] `BIGINT` / `INT64`
- [ ] `FLOAT` / `DOUBLE`
- [ ] `DECIMAL(p, s)`（精确数值）
- [ ] `DATE` / `TIME` / `DATETIME` / `TIMESTAMP`
- [ ] `BOOLEAN`
- [ ] `BLOB` / `TEXT`（大对象）
- [ ] `NULL`

### 索引

- [x] B+ 树索引结构
- [x] 索引插入、查询
- [x] 多索引支持
- [ ] SQL 语句创建/删除索引（`CREATE INDEX`, `DROP INDEX`）
- [ ] 查询优化器自动使用索引
- [ ] 唯一索引（`UNIQUE INDEX`）
- [ ] 复合索引（多列索引）
- [ ] 全文索引
- [ ] 索引统计信息（用于查询优化）

### 查询优化

#### 逻辑优化

- [x] 逻辑计划生成
- [x] 谓词下推（Predicate Pushdown）

#### 物理优化

- [x] 基本物理计划生成
- [ ] 基于代价的优化
- [ ] 利用索引的查询计划
- [ ] `JOIN` 优化

#### 其他

- [ ] 查询结果缓存
- [ ] 预编译语句（Prepared Statements）
- [ ] 查询计划缓存

### 事务和并发控制

#### 事务日志

- [x] 事务日志 `TransactionLogger`
- [ ] `BEGIN`, `COMMIT`, `ABORT` 操作
- [ ] SQL 语句支持：`BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`
- [ ] 崩溃恢复（Crash Recovery）
- [ ] 重做日志（Redo Log）
- [ ] 撤销日志（Undo Log）
- [ ] 检查点（Checkpoint）

#### 并发控制

- [ ] 两阶段锁（2PL）
- [ ] 死锁检测和处理
- [ ] 多版本并发控制（MVCC）
- [ ] 隔离级别支持（`READ UNCOMMITTED`, `READ COMMITTED`, `REPEATABLE READ`, `SERIALIZABLE`）

### 系统管理

#### 存储管理

- [x] 页面管理（`BufferManager`, `DiskManager`）
- [x] 文件管理（`FileHandler`, `FileManager`）
- [ ] 自动扩展数据文件
- [ ] 空间回收和碎片整理
- [ ] 表空间管理

#### 元数据管理

- [x] 目录管理（`CatalogManager`）
- [x] 表结构持久化
- [ ] 统计信息收集
- [ ] 约束信息管理
- [ ] 视图（`VIEW`）

#### 数据库管理

- [ ] 数据库备份和恢复
- [ ] 导入/导出（`COPY`, `LOAD DATA`）
- [ ] 权限管理（用户、角色、权限）
- [ ] 审计日志

### 性能和扩展

- [ ] 批量操作优化

### 客户端和接口

- [x] 命令行 REPL
- [x] 日志系统
