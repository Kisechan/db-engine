use crate::ix::errors::IXResult;

// 索引范围扫描迭代器
// 用于遍历指定范围内的所有记录 ID
#[allow(dead_code)]
pub struct IXScan {
    results: Vec<(u32, u16)>,  // 扫描结果（RID 列表）
    cursor: usize,              // 当前迭代位置
    lower: Vec<u8>,             // 范围下界
    upper: Vec<u8>,             // 范围上界
}

#[allow(dead_code)]
impl IXScan {
    // 创建一个新的扫描实例（直接从结果列表）
    // 
    // # 参数
    // - `results`: 范围扫描得到的 RID 列表
    pub fn new(results: Vec<(u32, u16)>) -> Self {
        Self { 
            results, 
            cursor: 0,
            lower: vec![],
            upper: vec![],
        }
    }

    // 从 BPTree 进行范围扫描
    // 包含扫描结果的 IXScan 实例
    pub fn from_tree(
        tree: &crate::ix::bplustree::BPTree,
        lower: &[u8],
        upper: &[u8],
    ) -> IXResult<Self> {
        println!("[IXScan] Starting range scan: lower_len={}, upper_len={}", 
            lower.len(), upper.len());

        // 调用 B+Tree 的 scan_range 方法
        let results = tree.scan_range(lower, upper)?;

        println!("[IXScan] Scan completed, found {} results", results.len());

        Ok(Self {
            results,
            cursor: 0,
            lower: lower.to_vec(),
            upper: upper.to_vec(),
        })
    }

    // 获取下一个结果 RID
    // 
    // # 返回
    // - Some((page_id, slot_id)) - 下一个 RID
    // - None - 没有更多结果
    pub fn next(&mut self) -> IXResult<Option<(u32, u16)>> {
        if self.cursor >= self.results.len() {
            println!("[IXScan] Scan exhausted, no more results");
            return Ok(None);
        }

        let rid = self.results[self.cursor];
        self.cursor += 1;

        println!("[IXScan] Returning RID[{}]: {:?}", self.cursor - 1, rid);
        Ok(Some(rid))
    }

    // 获取扫描统计信息
    pub fn get_stats(&self) -> ScanStats {
        ScanStats {
            total_results: self.results.len(),
            fetched_count: self.cursor,
            remaining: self.results.len().saturating_sub(self.cursor),
        }
    }

    // 重置扫描游标到开始
    pub fn reset(&mut self) {
        println!("[IXScan] Resetting scan cursor");
        self.cursor = 0;
    }

    // 关闭扫描
    pub fn close(&mut self) -> IXResult<()> {
        let stats = self.get_stats();
        println!("[IXScan] Closed scan. Stats: {:?}", stats);
        Ok(())
    }
}

// 扫描统计信息
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ScanStats {
    pub total_results: usize,  // 总结果数
    pub fetched_count: usize,  // 已取出数量
    pub remaining: usize,       // 剩余数量
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ix_scan_creation() {
        let results = vec![(1, 0), (2, 0), (3, 0)];
        let scan = IXScan::new(results.clone());

        let stats = scan.get_stats();
        assert_eq!(stats.total_results, 3);
        assert_eq!(stats.fetched_count, 0);
        assert_eq!(stats.remaining, 3);

        println!("✓ IXScan creation test passed");
    }

    #[test]
    fn test_ix_scan_iteration() {
        let results = vec![
            (100u32, 1u16),
            (200u32, 2u16),
            (300u32, 3u16),
        ];
        let mut scan = IXScan::new(results.clone());

        println!("Starting iteration");

        // 获取第一个结果
        let rid1 = scan.next().expect("Failed to get first result").expect("Expected RID");
        assert_eq!(rid1, (100, 1));
        println!("Got first RID: {:?}", rid1);

        // 获取第二个结果
        let rid2 = scan.next().expect("Failed to get second result").expect("Expected RID");
        assert_eq!(rid2, (200, 2));
        println!("Got second RID: {:?}", rid2);

        // 获取第三个结果
        let rid3 = scan.next().expect("Failed to get third result").expect("Expected RID");
        assert_eq!(rid3, (300, 3));
        println!("Got third RID: {:?}", rid3);

        // 尝试获取不存在的第四个结果
        let rid4 = scan.next().expect("Failed to call next");
        assert_eq!(rid4, None);
        println!("Got None for fourth RID (correct)");

        let stats = scan.get_stats();
        assert_eq!(stats.fetched_count, 3);
        assert_eq!(stats.remaining, 0);

        scan.close().expect("Failed to close scan");
        println!("✓ IXScan iteration test passed");
    }

    #[test]
    fn test_ix_scan_empty_results() {
        let results: Vec<(u32, u16)> = vec![];
        let mut scan = IXScan::new(results);

        let stats = scan.get_stats();
        assert_eq!(stats.total_results, 0);
        assert_eq!(stats.remaining, 0);

        let rid = scan.next().expect("Failed to call next");
        assert_eq!(rid, None);

        scan.close().expect("Failed to close scan");
        println!("✓ IXScan empty results test passed");
    }

    #[test]
    fn test_ix_scan_reset() {
        let results = vec![(1, 0), (2, 0), (3, 0)];
        let mut scan = IXScan::new(results);

        // 取出两个结果
        scan.next().expect("Failed to get first result");
        scan.next().expect("Failed to get second result");

        let stats = scan.get_stats();
        assert_eq!(stats.fetched_count, 2);
        assert_eq!(stats.remaining, 1);

        // 重置
        scan.reset();

        let stats = scan.get_stats();
        assert_eq!(stats.fetched_count, 0);
        assert_eq!(stats.remaining, 3);

        // 再次取出第一个结果
        let rid = scan.next().expect("Failed to get first result after reset").expect("Expected RID");
        assert_eq!(rid, (1, 0));

        scan.close().expect("Failed to close scan");
        println!("✓ IXScan reset test passed");
    }
}
