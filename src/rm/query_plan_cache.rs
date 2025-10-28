use std::collections::{HashMap, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// SQL 计划 = 这里只保存"执行指令结果"（之后可存执行树）
// 简化为字符串：基于 SQL 的解析结果
#[derive(Clone, Debug)]
pub struct QueryPlan {
    pub sql: String,
    pub plan_repr: String,
}

impl QueryPlan {
    pub fn new(sql: String, plan_repr: String) -> Self {
        QueryPlan { sql, plan_repr }
    }
}

// LRU Cache for Query Plan
pub struct QueryPlanCache {
    capacity: usize,
    queue: VecDeque<String>,   // LRU queue: SQL keys
    map: HashMap<String, QueryPlan>,
    hits: u64,                 // 缓存命中次数
    misses: u64,               // 缓存未命中次数
}

impl QueryPlanCache {
    pub fn new(cap: usize) -> Self {
        QueryPlanCache {
            capacity: cap,
            queue: VecDeque::new(),
            map: HashMap::new(),
            hits: 0,
            misses: 0,
        }
    }

    // 计算 SQL 字符串的 hash
    fn hash_sql(sql: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        hasher.finish()
    }

    // 从缓存获取执行计划
    pub fn get(&mut self, sql: &str) -> Option<QueryPlan> {
        if let Some(plan) = self.map.get(sql).cloned() {
            self.touch(sql);
            self.hits += 1;
            return Some(plan);
        }
        self.misses += 1;
        None
    }

    // 将执行计划放入缓存
    pub fn put(&mut self, plan: QueryPlan) {
        let sql = plan.sql.clone();
        
        // 如果已存在，更新并移到末尾（最近使用）
        if self.map.contains_key(&sql) {
            self.touch(&sql);
            self.map.insert(sql, plan);
            return;
        }

        // 如果缓存满，删除最久未使用的（队列前端）
        if self.queue.len() >= self.capacity {
            if let Some(old_sql) = self.queue.pop_front() {
                self.map.remove(&old_sql);
                println!("[Cache] Evicted plan for SQL: {}", 
                    self.truncate_sql(&old_sql, 50));
            }
        }

        // 添加新计划
        self.queue.push_back(sql.clone());
        self.map.insert(sql, plan);
    }

    // 更新 LRU 顺序（将 SQL 移到队列末尾）
    fn touch(&mut self, sql: &str) {
        if let Some(pos) = self.queue.iter().position(|x| x == sql) {
            self.queue.remove(pos);
            self.queue.push_back(sql.to_string());
        }
    }

    // 执行 SQL 并返回计划表示
    pub fn execute_sql(&mut self, sql: &str) -> Result<String, String> {
        // 规范化 SQL（删除多余空格、转小写）
        let normalized_sql = self.normalize_sql(sql);
        
        // 1. 尝试从缓存获取
        if let Some(plan) = self.get(&normalized_sql) {
            let hash = Self::hash_sql(&normalized_sql);
            println!("[Cache] Using cached plan (hash: {}) for SQL: {}", 
                hash, self.truncate_sql(&normalized_sql, 50));
            return Ok(plan.plan_repr);
        }

        // 2. 缓存未命中，解析 SQL 构造计划
        println!("[Cache] Cache miss, parsing SQL: {}", 
            self.truncate_sql(&normalized_sql, 50));
        
        let plan_repr = self.parse_sql(&normalized_sql)?;
        let hash = Self::hash_sql(&normalized_sql);
        
        println!("[Cache] Generated plan (hash: {}) for SQL: {}", 
            hash, self.truncate_sql(&normalized_sql, 50));

        // 3. 将计划放入缓存
        let plan = QueryPlan::new(normalized_sql, plan_repr.clone());
        self.put(plan);

        Ok(plan_repr)
    }

    // 解析 SQL 并生成执行计划表示
    fn parse_sql(&self, sql: &str) -> Result<String, String> {
        // 基础 SQL 验证
        if sql.is_empty() {
            return Err("SQL cannot be empty".to_string());
        }

        // 简化的 SQL 解析：根据关键字识别操作类型
        let upper_sql = sql.to_uppercase();

        let plan_type = if upper_sql.starts_with("SELECT") {
            "SELECT"
        } else if upper_sql.starts_with("INSERT") {
            "INSERT"
        } else if upper_sql.starts_with("DELETE") {
            "DELETE"
        } else if upper_sql.starts_with("UPDATE") {
            "UPDATE"
        } else if upper_sql.starts_with("CREATE") {
            "CREATE"
        } else if upper_sql.starts_with("DROP") {
            "DROP"
        } else {
            return Err(format!("Unsupported SQL type: {}", sql));
        };

        // 生成执行计划表示
        Ok(format!("Parsed({}:{})", plan_type, sql))
    }

    // 规范化 SQL（删除多余空格、转小写关键字）
    fn normalize_sql(&self, sql: &str) -> String {
        // 删除多余空格和换行
        sql.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string()
    }

    // 截断 SQL 用于日志显示
    fn truncate_sql(&self, sql: &str, max_len: usize) -> String {
        if sql.len() > max_len {
            format!("{}...", &sql[..max_len])
        } else {
            sql.to_string()
        }
    }

    // 获取缓存统计信息
    pub fn get_stats(&self) -> (u64, u64, f64) {
        let total = self.hits + self.misses;
        let hit_rate = if total > 0 {
            (self.hits as f64) / (total as f64) * 100.0
        } else {
            0.0
        };
        (self.hits, self.misses, hit_rate)
    }

    // 清空缓存
    pub fn clear(&mut self) {
        self.queue.clear();
        self.map.clear();
        self.hits = 0;
        self.misses = 0;
        println!("[Cache] Cache cleared");
    }

    // 获取缓存大小
    pub fn size(&self) -> usize {
        self.map.len()
    }

    // 获取缓存容量
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    // 打印缓存内容（用于调试）
    pub fn print_cache(&self) {
        println!("\n[Cache] Current cache state:");
        println!("  Capacity: {}/{}", self.size(), self.capacity);
        println!("  Hits: {}, Misses: {}, Hit Rate: {:.2}%", 
            self.hits, self.misses, {
                let total = self.hits + self.misses;
                if total > 0 {
                    (self.hits as f64) / (total as f64) * 100.0
                } else {
                    0.0
                }
            });
        
        println!("  Cached Plans (LRU order):");
        for (i, sql) in self.queue.iter().enumerate() {
            let plan = &self.map[sql];
            println!("    [{}] {} -> {}", i + 1, 
                self.truncate_sql(sql, 40), 
                self.truncate_sql(&plan.plan_repr, 40));
        }
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_plan_cache() {
        let mut cache = QueryPlanCache::new(3);

        // 测试 1：执行 SQL 并缓存
        let sql1 = "SELECT * FROM users";
        let plan1 = cache.execute_sql(sql1).unwrap();
        assert!(plan1.contains("SELECT"));
        println!("Plan 1: {}", plan1);

        // 测试 2：相同 SQL 从缓存获取
        let plan1_cached = cache.execute_sql(sql1).unwrap();
        assert_eq!(plan1, plan1_cached);

        // 测试 3：不同 SQL
        let sql2 = "INSERT INTO users VALUES (1, 'John')";
        let plan2 = cache.execute_sql(sql2).unwrap();
        assert!(plan2.contains("INSERT"));

        // 测试 4：查看缓存统计
        let (hits, misses, rate) = cache.get_stats();
        println!("Stats: hits={}, misses={}, rate={:.2}%", hits, misses, rate);

        // 测试 5：缓存满时驱逐
        cache.execute_sql("DELETE FROM users").unwrap();
        cache.execute_sql("UPDATE users SET name='Jane'").unwrap();
        cache.execute_sql("CREATE TABLE orders").unwrap();
        
        println!("Cache size: {}/{}", cache.size(), cache.capacity());
        cache.print_cache();
    }
}
