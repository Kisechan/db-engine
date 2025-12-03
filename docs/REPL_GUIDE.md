# Kisechan's DB-Engine - Interactive REPL Guide

## 🚀 快速开始

### 启动 REPL

```bash
cargo run --release
```

或者直接运行编译后的二进制文件：

```bash
./target/release/db-engine
```

## 📖 使用指南

### 欢迎界面

启动后会看到欢迎界面：

```
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║            Kisechan's DB-Engine v1.2.0                        ║
║                                                               ║
║   A lightweight relational database engine written in Rust   ║
║                                                               ║
╠═══════════════════════════════════════════════════════════════╣
║                                                               ║
║  🔗 GitHub: https://github.com/Kisechan/db-engine            ║
║                                                               ║
║  💡 Type .help to see available commands                     ║
║  💡 Type .exit or press Ctrl+D to quit                       ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

Kisechan's DB-Engine> 
```

### 特殊命令

| 命令 | 说明 |
|------|------|
| `.help`, `.h` | 显示帮助信息 |
| `.exit`, `.quit`, `.q` | 退出程序 |
| `.clear`, `.c` | 清屏 |
| `.history` | 显示命令历史 |
| `.databases`, `.dbs` | 列出所有数据库 |
| `.tables` | 列出当前数据库的所有表 |

### 快捷键

| 快捷键 | 功能 |
|--------|------|
| `Ctrl+C` | 取消当前输入（不退出程序） |
| `Ctrl+D` | 退出程序 |
| `↑/↓` | 浏览命令历史 |
| `Tab` | 自动补全（如果配置） |

## 💻 SQL 命令示例

### 1. 数据库管理

```sql
-- 创建数据库
CREATE DATABASE KisechansDB;

-- 如果不存在则创建
CREATE DATABASE IF NOT EXISTS KisechansDB;

-- 切换数据库
USE KisechansDB;

-- 显示所有数据库
SHOW DATABASES;
-- 或使用快捷命令
.databases

-- 删除数据库
DROP DATABASE KisechansDB;

-- 如果存在则删除
DROP DATABASE IF EXISTS KisechansDB;
```

### 2. 表管理

```sql
-- 创建表
CREATE TABLE users (
    id INT NOT NULL,
    name VARCHAR(50),
    age INT,
    email VARCHAR(100)
);

-- 显示当前数据库的所有表
SHOW TABLES;
-- 或使用快捷命令
.tables

-- 删除表
DROP TABLE users;

-- 如果存在则删除
DROP TABLE IF EXISTS users;
```

### 3. 查询数据

```sql
-- 查询所有列
SELECT * FROM users;

-- 查询特定列
SELECT id, name FROM users;

-- 带条件查询
SELECT * FROM users WHERE age > 18;

-- 排序
SELECT * FROM users ORDER BY age DESC;

-- 限制返回行数
SELECT * FROM users LIMIT 10;

-- DISTINCT 去重
SELECT DISTINCT name FROM users;
```

## 🎯 完整使用示例

```sql
-- 1. 创建数据库
Kisechan's DB-Engine> CREATE DATABASE school;
✓ Database 'school' created successfully

-- 2. 切换到数据库（提示符会显示当前数据库）
Kisechan's DB-Engine> USE school;
✓ Switched to database 'school'

-- 3. 创建学生表
Kisechan's DB-Engine [school]> CREATE TABLE students (
      ->     id INT NOT NULL,
      ->     name VARCHAR(50),
      ->     age INT,
      ->     grade VARCHAR(10)
      -> );
✓ Table 'students' created successfully

-- 4. 查看所有表
Kisechan's DB-Engine [school]> SHOW TABLES;
✓ Tables:
students

-- 5. 查询数据（假设已插入数据）
Kisechan's DB-Engine [school]> SELECT * FROM students WHERE age >= 18;
╔═══════════╦══════════════════════════════════════════════════════╗
║ RID       ║ Data (hex)                                           ║
╠═══════════╬══════════════════════════════════════════════════════╣
║ (1, 0)    ║ 01 00 00 00 12 00 00 00 4a 6f 68 6e ... (32 bytes) ║
║ (1, 1)    ║ 02 00 00 00 13 00 00 00 4a 61 6e 65 ... (32 bytes) ║
╚═══════════╩══════════════════════════════════════════════════════╝

(2 rows)
```

## 🌟 特性说明

### 多行 SQL 输入

SQL 语句必须以分号 (`;`) 结尾。支持多行输入：

```sql
Kisechan's DB-Engine> SELECT id, name
      -> FROM students
      -> WHERE age > 18
      -> ORDER BY name;
```

### 命令历史

- 使用 `↑/↓` 箭头键浏览历史命令
- 使用 `.history` 查看所有历史记录
- 历史记录在会话期间保持

### 彩色输出

- ✅ **绿色**：成功消息
- ❌ **红色**：错误消息
- 🔵 **青色**：系统信息和表格边框
- 🟡 **黄色**：高亮文本

### 当前数据库指示

提示符会显示当前选择的数据库：

```
Kisechan's DB-Engine> USE KisechansDB;
Kisechan's DB-Engine [KisechansDB]> 
```

## 🔧 高级用法

### 复杂查询示例

```sql
-- 带多个条件的查询
SELECT * FROM users 
WHERE age > 18 AND name LIKE 'A%'
ORDER BY age DESC
LIMIT 5;

-- 分组查询
SELECT department 
FROM employees 
GROUP BY department;

-- DISTINCT 查询
SELECT DISTINCT city 
FROM addresses;
```

### 表达式支持

WHERE 子句支持的操作符：

- **比较操作符**：`=`, `!=`, `<`, `<=`, `>`, `>=`
- **逻辑操作符**：`AND`, `OR`, `NOT`
- **算术操作符**：`+`, `-`, `*`, `/`, `%`
- **其他操作符**：`LIKE`, `IN`, `IS NULL`

### 数据类型

支持的数据类型：

| 类型 | 说明 | 示例 |
|------|------|------|
| `INT`, `INTEGER` | 32位整数 | `id INT NOT NULL` |
| `FLOAT`, `REAL` | 浮点数 | `price FLOAT` |
| `VARCHAR(n)` | 可变长度字符串 | `name VARCHAR(50)` |
| `CHAR(n)` | 固定长度字符串 | `code CHAR(10)` |

## 🐛 故障排除

### 常见问题

1. **"No database selected" 错误**
   - 解决：使用 `USE database_name;` 选择数据库

2. **"Parse error" 错误**
   - 检查 SQL 语法是否正确
   - 确保语句以分号结尾

3. **"Table not found" 错误**
   - 使用 `SHOW TABLES;` 确认表是否存在
   - 检查是否在正确的数据库中

4. **想要取消当前输入**
   - 按 `Ctrl+C` 清空当前输入缓冲区

## 📝 注意事项

1. **SQL 语句必须以分号结尾**
2. **关键字不区分大小写**（`SELECT` 等同于 `select`）
3. **标识符（表名、列名）区分大小写**
4. **字符串使用单引号** `'string'`
5. **数字不需要引号** `123`, `3.14`

## 🎓 学习资源

- GitHub 仓库：https://github.com/Kisechan/db-engine
- 文档目录：`docs/`
- 测试用例：`src/test/`

## 📞 获取帮助

在 REPL 中输入 `.help` 获取内置帮助信息：

```
Kisechan's DB-Engine> .help
```

祝您使用愉快！🎉
