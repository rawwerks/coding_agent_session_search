use anyhow::Result;
use std::path::Path;

pub struct EncryptionModule {
    // Config
}

impl Default for EncryptionModule {
    fn default() -> Self {
        Self::new()
    }
}

impl EncryptionModule {
    pub fn new() -> Self {
        Self {}
    }

    pub fn encrypt_file(&self, _input: &Path, _output: &Path) -> Result<()> {
        Ok(())
    }
}
