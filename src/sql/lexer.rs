// SQL 词法分析器 (Lexer)

use std::fmt;

// Token 类型
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // 关键字
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    CreateTable,
    DropTable,
    Distinct,
    And,
    Or,
    Not,
    Like,
    In,
    Is,
    Null,
    True,
    False,
    OrderBy,
    GroupBy,
    Having,
    Limit,
    Offset,
    Asc,
    Desc,
    If,
    Exists,
    Join,
    On,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    
    // 数据库管理关键字
    CreateDatabase,
    DropDatabase,
    Use,
    Show,
    Database,
    Databases,
    Tables,

    // 标识符和字面量
    Identifier(String),
    Integer(i64),
    Float(f64),
    String(String),

    // 操作符
    Equal,           // =
    NotEqual,        // != 或 <>
    LessThan,        // <
    LessThanEqual,   // <=
    GreaterThan,     // >
    GreaterThanEqual,// >=
    Plus,            // +
    Minus,           // -
    Star,            // *
    Slash,           // /
    Percent,         // %
    Dot,             // .

    // 分隔符
    LeftParen,       // (
    RightParen,      // )
    Comma,           // ,
    Semicolon,       // ;

    // 特殊
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Select => write!(f, "SELECT"),
            Token::From => write!(f, "FROM"),
            Token::Where => write!(f, "WHERE"),
            Token::Insert => write!(f, "INSERT"),
            Token::Into => write!(f, "INTO"),
            Token::Values => write!(f, "VALUES"),
            Token::Update => write!(f, "UPDATE"),
            Token::Set => write!(f, "SET"),
            Token::Delete => write!(f, "DELETE"),
            Token::CreateTable => write!(f, "CREATE TABLE"),
            Token::DropTable => write!(f, "DROP TABLE"),
            Token::Distinct => write!(f, "DISTINCT"),
            Token::And => write!(f, "AND"),
            Token::Or => write!(f, "OR"),
            Token::Not => write!(f, "NOT"),
            Token::Like => write!(f, "LIKE"),
            Token::In => write!(f, "IN"),
            Token::Is => write!(f, "IS"),
            Token::Null => write!(f, "NULL"),
            Token::True => write!(f, "TRUE"),
            Token::False => write!(f, "FALSE"),
            Token::OrderBy => write!(f, "ORDER BY"),
            Token::GroupBy => write!(f, "GROUP BY"),
            Token::Having => write!(f, "HAVING"),
            Token::Limit => write!(f, "LIMIT"),
            Token::Offset => write!(f, "OFFSET"),
            Token::Asc => write!(f, "ASC"),
            Token::Desc => write!(f, "DESC"),
            Token::If => write!(f, "IF"),
            Token::Exists => write!(f, "EXISTS"),
            Token::Join => write!(f, "JOIN"),
            Token::On => write!(f, "ON"),
            Token::Inner => write!(f, "INNER"),
            Token::Left => write!(f, "LEFT"),
            Token::Right => write!(f, "RIGHT"),
            Token::Full => write!(f, "FULL"),
            Token::Outer => write!(f, "OUTER"),
            Token::CreateDatabase => write!(f, "CREATE DATABASE"),
            Token::DropDatabase => write!(f, "DROP DATABASE"),
            Token::Use => write!(f, "USE"),
            Token::Show => write!(f, "SHOW"),
            Token::Database => write!(f, "DATABASE"),
            Token::Databases => write!(f, "DATABASES"),
            Token::Tables => write!(f, "TABLES"),
            Token::Identifier(s) => write!(f, "{}", s),
            Token::Integer(n) => write!(f, "{}", n),
            Token::Float(n) => write!(f, "{}", n),
            Token::String(s) => write!(f, "'{}'", s),
            Token::Equal => write!(f, "="),
            Token::NotEqual => write!(f, "!="),
            Token::LessThan => write!(f, "<"),
            Token::LessThanEqual => write!(f, "<="),
            Token::GreaterThan => write!(f, ">"),
            Token::GreaterThanEqual => write!(f, ">="),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Star => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::Percent => write!(f, "%"),
            Token::Dot => write!(f, "."),
            Token::LeftParen => write!(f, "("),
            Token::RightParen => write!(f, ")"),
            Token::Comma => write!(f, ","),
            Token::Semicolon => write!(f, ";"),
            Token::Eof => write!(f, "EOF"),
        }
    }
}

// 词法分析器
pub struct Lexer {
    input: Vec<char>,
    position: usize,
    current_char: Option<char>,
}

impl Lexer {
    // 创建新的词法分析器
    pub fn new(input: &str) -> Self {
        let chars: Vec<char> = input.chars().collect();
        let current_char = if chars.is_empty() { None } else { Some(chars[0]) };
        Lexer {
            input: chars,
            position: 0,
            current_char,
        }
    }

    // 前进到下一个字符
    fn advance(&mut self) {
        self.position += 1;
        self.current_char = if self.position < self.input.len() {
            Some(self.input[self.position])
        } else {
            None
        };
    }

    // 查看下一个字符而不前进
    fn peek(&self) -> Option<char> {
        if self.position + 1 < self.input.len() {
            Some(self.input[self.position + 1])
        } else {
            None
        }
    }

    // 跳过空白符
    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.current_char {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    // 读取数字（整数或浮点数）
    fn read_number(&mut self) -> Token {
        let mut number_str = String::new();
        let mut is_float = false;

        while let Some(ch) = self.current_char {
            if ch.is_numeric() {
                number_str.push(ch);
                self.advance();
            } else if ch == '.' && !is_float {
                is_float = true;
                number_str.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        if is_float {
            Token::Float(number_str.parse().unwrap_or(0.0))
        } else {
            Token::Integer(number_str.parse().unwrap_or(0))
        }
    }

    // 读取字符串字面量（单引号）
    fn read_string(&mut self) -> Token {
        let mut string = String::new();
        self.advance(); // 跳过开始的单引号

        while let Some(ch) = self.current_char {
            if ch == '\'' {
                self.advance(); // 跳过结束的单引号
                break;
            } else if ch == '\\' {
                self.advance();
                if let Some(escaped) = self.current_char {
                    string.push(escaped);
                    self.advance();
                }
            } else {
                string.push(ch);
                self.advance();
            }
        }

        Token::String(string)
    }

    // 读取标识符或关键字
    fn read_identifier(&mut self) -> Token {
        let mut ident = String::new();

        while let Some(ch) = self.current_char {
            if ch.is_alphanumeric() || ch == '_' {
                ident.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        let upper_ident = ident.to_uppercase();
        match upper_ident.as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "DELETE" => Token::Delete,
            "CREATE" => {
                // 需要查看下一个关键字是否是 TABLE 或 DATABASE
                self.skip_whitespace();
                let mut next_ident = String::new();
                while let Some(ch) = self.current_char {
                    if ch.is_alphanumeric() || ch == '_' {
                        next_ident.push(ch);
                        self.advance();
                    } else {
                        break;
                    }
                }
                match next_ident.to_uppercase().as_str() {
                    "TABLE" => Token::CreateTable,
                    "DATABASE" => Token::CreateDatabase,
                    _ => {
                        // 退回处理
                        for _ in 0..next_ident.len() {
                            self.position = self.position.saturating_sub(1);
                        }
                        self.current_char = if self.position < self.input.len() {
                            Some(self.input[self.position])
                        } else {
                            None
                        };
                        Token::Identifier(ident)
                    }
                }
            }
            "DROP" => {
                self.skip_whitespace();
                let mut next_ident = String::new();
                while let Some(ch) = self.current_char {
                    if ch.is_alphanumeric() || ch == '_' {
                        next_ident.push(ch);
                        self.advance();
                    } else {
                        break;
                    }
                }
                match next_ident.to_uppercase().as_str() {
                    "TABLE" => Token::DropTable,
                    "DATABASE" => Token::DropDatabase,
                    _ => {
                        for _ in 0..next_ident.len() {
                            self.position = self.position.saturating_sub(1);
                        }
                        self.current_char = if self.position < self.input.len() {
                            Some(self.input[self.position])
                        } else {
                            None
                        };
                        Token::Identifier(ident)
                    }
                }
            }
            "DISTINCT" => Token::Distinct,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "LIKE" => Token::Like,
            "IN" => Token::In,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "ORDER" => {
                self.skip_whitespace();
                let mut next_ident = String::new();
                while let Some(ch) = self.current_char {
                    if ch.is_alphanumeric() || ch == '_' {
                        next_ident.push(ch);
                        self.advance();
                    } else {
                        break;
                    }
                }
                if next_ident.to_uppercase() == "BY" {
                    Token::OrderBy
                } else {
                    for _ in 0..next_ident.len() {
                        self.position = self.position.saturating_sub(1);
                    }
                    self.current_char = if self.position < self.input.len() {
                        Some(self.input[self.position])
                    } else {
                        None
                    };
                    Token::Identifier(ident)
                }
            }
            "GROUP" => {
                self.skip_whitespace();
                let mut next_ident = String::new();
                while let Some(ch) = self.current_char {
                    if ch.is_alphanumeric() || ch == '_' {
                        next_ident.push(ch);
                        self.advance();
                    } else {
                        break;
                    }
                }
                if next_ident.to_uppercase() == "BY" {
                    Token::GroupBy
                } else {
                    for _ in 0..next_ident.len() {
                        self.position = self.position.saturating_sub(1);
                    }
                    self.current_char = if self.position < self.input.len() {
                        Some(self.input[self.position])
                    } else {
                        None
                    };
                    Token::Identifier(ident)
                }
            }
            "HAVING" => Token::Having,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "IF" => Token::If,
            "EXISTS" => Token::Exists,
            "JOIN" => Token::Join,
            "ON" => Token::On,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "FULL" => Token::Full,
            "OUTER" => Token::Outer,
            "USE" => Token::Use,
            "SHOW" => Token::Show,
            "DATABASE" => Token::Database,
            "DATABASES" => Token::Databases,
            "TABLES" => Token::Tables,
            _ => Token::Identifier(ident),
        }
    }

    // 获取下一个 token
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        match self.current_char {
            None => Token::Eof,
            Some(ch) => {
                if ch.is_numeric() {
                    self.read_number()
                } else if ch == '\'' {
                    self.read_string()
                } else if ch.is_alphabetic() || ch == '_' {
                    self.read_identifier()
                } else {
                    match ch {
                        '=' => {
                            self.advance();
                            Token::Equal
                        }
                        '!' => {
                            self.advance();
                            if self.current_char == Some('=') {
                                self.advance();
                                Token::NotEqual
                            } else {
                                Token::Identifier("!".to_string())
                            }
                        }
                        '<' => {
                            self.advance();
                            if self.current_char == Some('=') {
                                self.advance();
                                Token::LessThanEqual
                            } else if self.current_char == Some('>') {
                                self.advance();
                                Token::NotEqual
                            } else {
                                Token::LessThan
                            }
                        }
                        '>' => {
                            self.advance();
                            if self.current_char == Some('=') {
                                self.advance();
                                Token::GreaterThanEqual
                            } else {
                                Token::GreaterThan
                            }
                        }
                        '+' => {
                            self.advance();
                            Token::Plus
                        }
                        '-' => {
                            self.advance();
                            Token::Minus
                        }
                        '*' => {
                            self.advance();
                            Token::Star
                        }
                        '/' => {
                            self.advance();
                            Token::Slash
                        }
                        '%' => {
                            self.advance();
                            Token::Percent
                        }
                        '.' => {
                            self.advance();
                            Token::Dot
                        }
                        '(' => {
                            self.advance();
                            Token::LeftParen
                        }
                        ')' => {
                            self.advance();
                            Token::RightParen
                        }
                        ',' => {
                            self.advance();
                            Token::Comma
                        }
                        ';' => {
                            self.advance();
                            Token::Semicolon
                        }
                        _ => {
                            self.advance();
                            Token::Identifier(ch.to_string())
                        }
                    }
                }
            }
        }
    }

    // 一次性获取所有 tokens
    pub fn tokenize(mut self) -> Vec<Token> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token();
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }
        tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keywords() {
        let lexer = Lexer::new("SELECT FROM WHERE INSERT");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::From);
        assert_eq!(tokens[2], Token::Where);
        assert_eq!(tokens[3], Token::Insert);
    }

    #[test]
    fn test_identifiers() {
        let lexer = Lexer::new("table1 column_name _private");
        let tokens = lexer.tokenize();
        assert!(matches!(tokens[0], Token::Identifier(_)));
        assert!(matches!(tokens[1], Token::Identifier(_)));
        assert!(matches!(tokens[2], Token::Identifier(_)));
    }

    #[test]
    fn test_numbers() {
        let lexer = Lexer::new("123 45.67 0");
        let tokens = lexer.tokenize();
        assert!(matches!(tokens[0], Token::Integer(123)));
        assert!(matches!(tokens[1], Token::Float(_)));
        assert!(matches!(tokens[2], Token::Integer(0)));
    }

    #[test]
    fn test_strings() {
        let lexer = Lexer::new("'hello' 'world'");
        let tokens = lexer.tokenize();
        assert!(matches!(tokens[0], Token::String(_)));
        assert!(matches!(tokens[1], Token::String(_)));
    }

    #[test]
    fn test_operators() {
        let lexer = Lexer::new("= != < <= > >= + - * / %");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Equal);
        assert_eq!(tokens[1], Token::NotEqual);
        assert_eq!(tokens[2], Token::LessThan);
        assert_eq!(tokens[3], Token::LessThanEqual);
    }

    #[test]
    fn test_delimiters() {
        let lexer = Lexer::new("( ) , ;");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::LeftParen);
        assert_eq!(tokens[1], Token::RightParen);
        assert_eq!(tokens[2], Token::Comma);
        assert_eq!(tokens[3], Token::Semicolon);
    }

    #[test]
    fn test_select_query() {
        let lexer = Lexer::new("SELECT * FROM users WHERE age > 18");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
        assert!(matches!(tokens[3], Token::Identifier(_)));
        assert_eq!(tokens[4], Token::Where);
    }

    #[test]
    fn test_case_insensitive() {
        let lexer1 = Lexer::new("SELECT");
        let tokens1 = lexer1.tokenize();
        let lexer2 = Lexer::new("select");
        let tokens2 = lexer2.tokenize();
        assert_eq!(tokens1[0], tokens2[0]);
    }

    #[test]
    fn test_whitespace_handling() {
        let lexer = Lexer::new("  SELECT   *   FROM   users  ");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
    }

    #[test]
    fn test_create_table_keyword() {
        let lexer = Lexer::new("CREATE TABLE users (id INT)");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::CreateTable);
    }

    #[test]
    fn test_drop_table_keyword() {
        let lexer = Lexer::new("DROP TABLE users");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::DropTable);
    }

    #[test]
    fn test_order_by_keyword() {
        let lexer = Lexer::new("ORDER BY name");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::OrderBy);
    }

    #[test]
    fn test_group_by_keyword() {
        let lexer = Lexer::new("GROUP BY department");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::GroupBy);
    }

    #[test]
    fn test_complex_query() {
        let query = "SELECT id, name FROM users WHERE age > 18 AND status = 'active' ORDER BY name DESC";
        let lexer = Lexer::new(query);
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Select);
        assert!(matches!(tokens[1], Token::Identifier(_)));
        assert_eq!(tokens[2], Token::Comma);
    }

    #[test]
    fn test_create_database_keyword() {
        let lexer = Lexer::new("CREATE DATABASE mydb");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::CreateDatabase);
        assert!(matches!(tokens[1], Token::Identifier(_)));
    }

    #[test]
    fn test_drop_database_keyword() {
        let lexer = Lexer::new("DROP DATABASE mydb");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::DropDatabase);
    }

    #[test]
    fn test_use_database_keyword() {
        let lexer = Lexer::new("USE mydb");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Use);
        assert!(matches!(tokens[1], Token::Identifier(_)));
    }

    #[test]
    fn test_show_databases_keyword() {
        let lexer = Lexer::new("SHOW DATABASES");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Show);
        assert_eq!(tokens[1], Token::Databases);
    }

    #[test]
    fn test_show_tables_keyword() {
        let lexer = Lexer::new("SHOW TABLES");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Show);
        assert_eq!(tokens[1], Token::Tables);
    }

    #[test]
    fn test_create_database_if_not_exists() {
        let lexer = Lexer::new("CREATE DATABASE IF NOT EXISTS testdb");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::CreateDatabase);
        assert_eq!(tokens[1], Token::If);
        assert_eq!(tokens[2], Token::Not);
        assert_eq!(tokens[3], Token::Exists);
        assert!(matches!(tokens[4], Token::Identifier(_)));
    }

    #[test]
    fn test_drop_database_if_exists() {
        let lexer = Lexer::new("DROP DATABASE IF EXISTS testdb");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::DropDatabase);
        assert_eq!(tokens[1], Token::If);
        assert_eq!(tokens[2], Token::Exists);
        assert!(matches!(tokens[3], Token::Identifier(_)));
    }
}
