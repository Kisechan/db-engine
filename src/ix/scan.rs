use crate::ix::errors::IXResult;

#[allow(dead_code)]
pub struct IXScan {
    // handler: IXHandler,  // TODO: 需要定义正确的 handler 类型
    results: Vec<(u32, u16)>,
    cursor: usize,
}

#[allow(dead_code)]
impl IXScan {
    pub fn new(results: Vec<(u32, u16)>) -> Self {
        Self { results, cursor: 0 }
    }

    pub fn open_scan(_lower: &[u8], _upper: &[u8]) -> IXResult<Self> {
        // TODO: 实现范围扫描
        Ok(Self {
            results: vec![],
            cursor: 0,
        })
    }

    pub fn next(&mut self) -> IXResult<Option<(u32, u16)>> {
        if self.cursor >= self.results.len() {
            return Ok(None);
        }
        let r = self.results[self.cursor];
        self.cursor += 1;
        Ok(Some(r))
    }

    pub fn close(&mut self) -> IXResult<()> {
        Ok(())
    }
}
