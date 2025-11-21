use std::path::PathBuf;

use anyhow::Result;

use crate::connectors::{Connector, DetectionResult, NormalizedConversation, ScanContext};

pub struct AmpConnector;
impl Default for AmpConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl AmpConnector {
    pub fn new() -> Self {
        Self
    }

    fn cache_root() -> PathBuf {
        dirs::data_dir().unwrap_or_else(|| PathBuf::from("."))
    }
}

impl Connector for AmpConnector {
    fn detect(&self) -> DetectionResult {
        let root = Self::cache_root().join("amp");
        if root.exists() {
            DetectionResult {
                detected: true,
                evidence: vec![format!("found {}", root.display())],
            }
        } else {
            DetectionResult::not_found()
        }
    }

    fn scan(&self, _ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        // Local cache often minimal; leave empty until real sample is available.
        Ok(Vec::new())
    }
}
