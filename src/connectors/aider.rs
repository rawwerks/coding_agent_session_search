use super::{Connector, DetectionResult, NormalizedConversation, NormalizedMessage, ScanContext};
use anyhow::Result;
use serde_json::json;
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

pub struct AiderConnector;

impl AiderConnector {
    pub fn new() -> Self {
        Self
    }

    fn parse_chat_history(&self, path: &Path) -> Result<NormalizedConversation> {
        let content = fs::read_to_string(path)?;
        let mut messages = Vec::new();
        let mut current_role = "system";
        let mut current_content = String::new();
        let mut msg_idx = 0;

        for line in content.lines() {
            if line.trim().starts_with("> ") {
                if !current_content.trim().is_empty() {
                    messages.push(NormalizedMessage {
                        idx: msg_idx,
                        role: current_role.to_string(),
                        author: Some(current_role.to_string()),
                        created_at: None,
                        content: current_content.trim().to_string(),
                        extra: json!({}),
                        snippets: Vec::new(),
                    });
                    msg_idx += 1;
                    current_content.clear();
                }
                current_role = "user";
                current_content.push_str(line.trim_start_matches("> ").trim());
                current_content.push('\n');
            } else {
                if current_role == "user" && !line.trim().is_empty() && !line.starts_with('>') {
                    if !current_content.trim().is_empty() {
                        messages.push(NormalizedMessage {
                            idx: msg_idx,
                            role: "user".to_string(),
                            author: Some("user".to_string()),
                            created_at: None,
                            content: current_content.trim().to_string(),
                            extra: json!({}),
                            snippets: Vec::new(),
                        });
                        msg_idx += 1;
                        current_content.clear();
                    }
                    current_role = "assistant";
                }
                current_content.push_str(line);
                current_content.push('\n');
            }
        }

        if !current_content.trim().is_empty() {
            messages.push(NormalizedMessage {
                idx: msg_idx,
                role: current_role.to_string(),
                author: Some(current_role.to_string()),
                created_at: None,
                content: current_content.trim().to_string(),
                extra: json!({}),
                snippets: Vec::new(),
            });
        }

        let mtime = fs::metadata(path)?.modified()?;
        let ts = mtime
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Ok(NormalizedConversation {
            agent_slug: "aider".to_string(),
            external_id: Some(path.file_name().unwrap().to_string_lossy().to_string()),
            title: Some(format!("Aider Chat: {}", path.display())),
            workspace: path.parent().map(|p| p.to_path_buf()),
            source_path: path.to_path_buf(),
            started_at: Some(ts),
            ended_at: Some(ts),
            metadata: json!({}),
            messages,
        })
    }
}

impl Connector for AiderConnector {
    fn detect(&self) -> DetectionResult {
        DetectionResult {
            detected: true,
            evidence: vec!["aider-connector".into()],
        }
    }

    fn scan(&self, ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        let mut conversations = Vec::new();

        for entry in WalkDir::new(&ctx.data_root)
            .follow_links(true)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            if path.is_file()
                && path
                    .file_name()
                    .is_some_and(|n| n == ".aider.chat.history.md")
                && super::file_modified_since(path, ctx.since_ts)
                && let Ok(conv) = self.parse_chat_history(path)
            {
                conversations.push(conv);
            }
        }

        Ok(conversations)
    }
}

impl Default for AiderConnector {
    fn default() -> Self {
        Self::new()
    }
}
