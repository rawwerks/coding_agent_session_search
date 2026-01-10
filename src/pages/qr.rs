use anyhow::Result;
use std::path::Path;

pub struct QrGenerator {
    // Config
}

impl Default for QrGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl QrGenerator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn generate(&self, _data: &str, _output_path: &Path) -> Result<()> {
        Ok(())
    }
}
