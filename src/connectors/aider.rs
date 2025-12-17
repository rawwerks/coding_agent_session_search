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

    /// Find aider chat history files under the provided roots (limited depth to avoid wide scans).
    fn find_chat_files(roots: &[&Path]) -> Vec<std::path::PathBuf> {
        let mut files = Vec::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            for entry in WalkDir::new(root)
                .max_depth(5)
                .into_iter()
                .flatten()
                .filter(|e| e.file_type().is_file())
            {
                if entry
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n == ".aider.chat.history.md")
                {
                    files.push(entry.path().to_path_buf());
                }
            }
        }
        files
    }

    fn parse_chat_history(&self, path: &Path) -> Result<NormalizedConversation> {
        let content = fs::read_to_string(path)?;
        let mut messages = Vec::new();
        let mut current_role = "system";
        let mut current_content = String::new();
        let mut msg_idx = 0;

        for line in content.lines() {
            if line.trim().starts_with("> ") {
                // Only push previous content if switching from non-user role
                if current_role != "user" && !current_content.trim().is_empty() {
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
            workspace: path.parent().map(std::path::Path::to_path_buf),
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
        // Fast detection: only check for .aider.chat.history.md in CWD (no recursive scan).
        // The expensive WalkDir scan is deferred to scan() where it's actually needed.
        // Also check for CASS_AIDER_DATA_ROOT env var as a signal.
        let cwd = std::env::current_dir().unwrap_or_default();
        let cwd_history = cwd.join(".aider.chat.history.md");

        if cwd_history.exists() {
            return DetectionResult {
                detected: true,
                evidence: vec![format!("found {}", cwd_history.display())],
            };
        }

        if let Some(override_root) = std::env::var_os("CASS_AIDER_DATA_ROOT") {
            let override_path = std::path::PathBuf::from(&override_root);
            let override_history = override_path.join(".aider.chat.history.md");
            if override_history.exists() {
                return DetectionResult {
                    detected: true,
                    evidence: vec![format!("found {}", override_history.display())],
                };
            }
            // Even if file not found, user explicitly set the env var
            return DetectionResult {
                detected: true,
                evidence: vec![format!(
                    "CASS_AIDER_DATA_ROOT set to {}",
                    override_path.display()
                )],
            };
        }

        DetectionResult {
            detected: false,
            evidence: vec![],
        }
    }

    fn scan(&self, ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        let files = Self::find_chat_files(std::slice::from_ref(&ctx.data_dir.as_path()));

        let mut conversations = Vec::new();
        for path in files {
            if !super::file_modified_since(&path, ctx.since_ts) {
                continue;
            }
            if let Ok(conv) = self.parse_chat_history(&path) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    // =====================================================
    // Constructor Tests
    // =====================================================

    #[test]
    fn new_creates_connector() {
        let connector = AiderConnector::new();
        let _ = connector;
    }

    #[test]
    fn default_creates_connector() {
        let connector = AiderConnector::default();
        let _ = connector;
    }

    // =====================================================
    // find_chat_files() Tests
    // =====================================================

    #[test]
    fn find_chat_files_finds_aider_history() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "# Chat\n> Hello").unwrap();

        let files = AiderConnector::find_chat_files(&[dir.path()]);
        assert_eq!(files.len(), 1);
        assert!(files[0].to_str().unwrap().contains(".aider.chat.history.md"));
    }

    #[test]
    fn find_chat_files_finds_nested_history() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("project").join("subdir");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join(".aider.chat.history.md"), "# Chat").unwrap();

        let files = AiderConnector::find_chat_files(&[dir.path()]);
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn find_chat_files_ignores_other_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join(".aider.conf.yml"), "config").unwrap();
        fs::write(dir.path().join("chat.md"), "# Chat").unwrap();
        fs::write(dir.path().join("README.md"), "# README").unwrap();

        let files = AiderConnector::find_chat_files(&[dir.path()]);
        assert_eq!(files.len(), 0);
    }

    #[test]
    fn find_chat_files_respects_max_depth() {
        let dir = TempDir::new().unwrap();
        // Create deeply nested structure (6 levels deep - beyond max_depth=5)
        let deep = dir
            .path()
            .join("a")
            .join("b")
            .join("c")
            .join("d")
            .join("e")
            .join("f");
        fs::create_dir_all(&deep).unwrap();
        fs::write(deep.join(".aider.chat.history.md"), "# Deep").unwrap();

        let files = AiderConnector::find_chat_files(&[dir.path()]);
        // Should not find file beyond max_depth=5
        assert_eq!(files.len(), 0);
    }

    #[test]
    fn find_chat_files_returns_empty_for_nonexistent_root() {
        let nonexistent = PathBuf::from("/nonexistent/path/to/aider");
        let files = AiderConnector::find_chat_files(&[&nonexistent]);
        assert_eq!(files.len(), 0);
    }

    #[test]
    fn find_chat_files_searches_multiple_roots() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();

        fs::write(dir1.path().join(".aider.chat.history.md"), "# Chat 1").unwrap();
        fs::write(dir2.path().join(".aider.chat.history.md"), "# Chat 2").unwrap();

        let files = AiderConnector::find_chat_files(&[dir1.path(), dir2.path()]);
        assert_eq!(files.len(), 2);
    }

    // =====================================================
    // parse_chat_history() Tests
    // =====================================================

    #[test]
    fn parse_chat_history_parses_user_messages() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "> Hello, Aider!\n> How are you?").unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.messages.len(), 1);
        assert_eq!(conv.messages[0].role, "user");
        assert!(conv.messages[0].content.contains("Hello, Aider!"));
        assert!(conv.messages[0].content.contains("How are you?"));
    }

    #[test]
    fn parse_chat_history_parses_assistant_messages() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        let content = "> User message\nAssistant response here.\nMore response.";
        fs::write(&history_file, content).unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.messages.len(), 2);
        assert_eq!(conv.messages[0].role, "user");
        assert_eq!(conv.messages[1].role, "assistant");
        assert!(conv.messages[1].content.contains("Assistant response"));
    }

    #[test]
    fn parse_chat_history_handles_conversation_flow() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        let content = r#"> First user message
First assistant response

> Second user message
Second assistant response"#;
        fs::write(&history_file, content).unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.messages.len(), 4);
        assert_eq!(conv.messages[0].role, "user");
        assert_eq!(conv.messages[1].role, "assistant");
        assert_eq!(conv.messages[2].role, "user");
        assert_eq!(conv.messages[3].role, "assistant");
    }

    #[test]
    fn parse_chat_history_sets_agent_slug() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "> Hello").unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.agent_slug, "aider");
    }

    #[test]
    fn parse_chat_history_sets_workspace_to_parent() {
        let dir = TempDir::new().unwrap();
        let project = dir.path().join("my-project");
        fs::create_dir_all(&project).unwrap();
        let history_file = project.join(".aider.chat.history.md");
        fs::write(&history_file, "> Hello").unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.workspace, Some(project));
    }

    #[test]
    fn parse_chat_history_sets_title() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "> Hello").unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert!(conv.title.is_some());
        assert!(conv.title.unwrap().contains("Aider Chat"));
    }

    #[test]
    fn parse_chat_history_sets_external_id() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "> Hello").unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(
            conv.external_id,
            Some(".aider.chat.history.md".to_string())
        );
    }

    #[test]
    fn parse_chat_history_assigns_sequential_indices() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        let content = "> First\nResponse 1\n> Second\nResponse 2";
        fs::write(&history_file, content).unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.messages[0].idx, 0);
        assert_eq!(conv.messages[1].idx, 1);
        assert_eq!(conv.messages[2].idx, 2);
        assert_eq!(conv.messages[3].idx, 3);
    }

    #[test]
    fn parse_chat_history_sets_author_to_role() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        let content = "> User says\nAssistant says";
        fs::write(&history_file, content).unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.messages[0].author, Some("user".to_string()));
        assert_eq!(conv.messages[1].author, Some("assistant".to_string()));
    }

    #[test]
    fn parse_chat_history_handles_empty_file() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "").unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.messages.len(), 0);
    }

    #[test]
    fn parse_chat_history_handles_only_whitespace() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "   \n\n   \n").unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        // Should handle gracefully (system message with whitespace may be produced)
        // The important thing is it doesn't crash
        assert!(conv.messages.is_empty() || conv.messages[0].content.trim().is_empty());
    }

    #[test]
    fn parse_chat_history_strips_quote_prefix() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "> Hello world").unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.messages[0].content.trim(), "Hello world");
        assert!(!conv.messages[0].content.contains("> "));
    }

    #[test]
    fn parse_chat_history_uses_file_mtime_for_timestamp() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "> Hello").unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert!(conv.started_at.is_some());
        assert!(conv.ended_at.is_some());
        // Timestamp should be recent (within last minute)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(conv.started_at.unwrap() > now - 60000);
    }

    // =====================================================
    // scan() Tests
    // =====================================================

    #[test]
    fn scan_finds_and_parses_history_files() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "> Hello Aider\nHello! How can I help?").unwrap();

        let connector = AiderConnector::new();
        let ctx = ScanContext::local_default(dir.path().to_path_buf(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs.len(), 1);
        assert_eq!(convs[0].agent_slug, "aider");
        assert_eq!(convs[0].messages.len(), 2);
    }

    #[test]
    fn scan_handles_empty_directory() {
        let dir = TempDir::new().unwrap();

        let connector = AiderConnector::new();
        let ctx = ScanContext::local_default(dir.path().to_path_buf(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn scan_finds_multiple_history_files() {
        let dir = TempDir::new().unwrap();
        let proj1 = dir.path().join("project1");
        let proj2 = dir.path().join("project2");
        fs::create_dir_all(&proj1).unwrap();
        fs::create_dir_all(&proj2).unwrap();

        fs::write(proj1.join(".aider.chat.history.md"), "> Hello 1").unwrap();
        fs::write(proj2.join(".aider.chat.history.md"), "> Hello 2").unwrap();

        let connector = AiderConnector::new();
        let ctx = ScanContext::local_default(dir.path().to_path_buf(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs.len(), 2);
    }

    #[test]
    fn scan_sets_source_path() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        fs::write(&history_file, "> Hello").unwrap();

        let connector = AiderConnector::new();
        let ctx = ScanContext::local_default(dir.path().to_path_buf(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs[0].source_path, history_file);
    }

    // =====================================================
    // Edge Cases
    // =====================================================

    #[test]
    fn parse_chat_history_handles_multiline_user_message() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        let content = "> Line 1\n> Line 2\n> Line 3\nAssistant response";
        fs::write(&history_file, content).unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        // User message should contain all three lines
        assert!(conv.messages[0].content.contains("Line 1"));
        assert!(conv.messages[0].content.contains("Line 2"));
        assert!(conv.messages[0].content.contains("Line 3"));
    }

    #[test]
    fn parse_chat_history_handles_code_blocks() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        let content = r#"> Add a function
Here's the code:
```python
def hello():
    print("Hello")
```
Done!"#;
        fs::write(&history_file, content).unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert_eq!(conv.messages.len(), 2);
        assert!(conv.messages[1].content.contains("```python"));
        assert!(conv.messages[1].content.contains("def hello"));
    }

    #[test]
    fn parse_chat_history_handles_special_characters() {
        let dir = TempDir::new().unwrap();
        let history_file = dir.path().join(".aider.chat.history.md");
        let content = "> What does `foo()` do?\nThe function `foo()` returns \"bar\".";
        fs::write(&history_file, content).unwrap();

        let connector = AiderConnector::new();
        let conv = connector.parse_chat_history(&history_file).unwrap();

        assert!(conv.messages[0].content.contains("`foo()`"));
        assert!(conv.messages[1].content.contains("\"bar\""));
    }
}
