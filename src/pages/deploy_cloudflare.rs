use anyhow::Result;

pub struct CloudflareDeployer {
    // Config
}

impl Default for CloudflareDeployer {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudflareDeployer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn deploy(&self) -> Result<()> {
        Ok(())
    }
}
