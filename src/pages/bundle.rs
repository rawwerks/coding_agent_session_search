use anyhow::Result;
use std::path::Path;

pub struct BundleBuilder {
    // Config
}

impl Default for BundleBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BundleBuilder {
    pub fn new() -> Self {
        Self {}
    }

    pub fn build(&self, _output_dir: &Path) -> Result<()> {
        Ok(())
    }
}
