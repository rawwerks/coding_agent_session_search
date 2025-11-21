use std::path::PathBuf;

use anyhow::Result;

use crate::connectors::{Connector, DetectionResult, NormalizedConversation, ScanContext};

pub struct OpenCodeConnector;
impl Default for OpenCodeConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenCodeConnector {
    pub fn new() -> Self {
        Self
    }

    fn dir_candidates() -> Vec<PathBuf> {
        let cwd = std::env::current_dir().unwrap_or_default();
        let mut dirs = vec![cwd.join(".opencode")];
        if let Some(home) = dirs::home_dir() {
            dirs.push(home.join(".opencode"));
        }
        dirs
    }
}

impl Connector for OpenCodeConnector {
    fn detect(&self) -> DetectionResult {
        for d in Self::dir_candidates() {
            if d.exists() {
                return DetectionResult {
                    detected: true,
                    evidence: vec![format!("found {}", d.display())],
                };
            }
        }
        DetectionResult::not_found()
    }

    fn scan(&self, _ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        // TODO: read SQLite `.opencode` DB; placeholder empty until sample available.
        Ok(Vec::new())
    }
}
