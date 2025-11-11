use crate::ix::handler::IXhandler;
use crate::ix::errors::IXResult;

pub struct IXScan {
    handler: IXhandlerr,
    results: Vec<(u32, u16)>,
    cursor: usize,
}

impl IXScan {
    pub fn open_scan(handler: IXhandler, lower: &[u8], upper: &[u8]) -> IXResult<Self> {
        let results = handler.tree.as_ref().unwrap().scan_range(lower, upper)?;
        Ok(Self { handler, results, cursor: 0 })
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
