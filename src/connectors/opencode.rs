//! OpenCode connector for JSON file-based storage.
//!
//! OpenCode stores data at `~/.local/share/opencode/storage/` using a hierarchical
//! JSON file structure:
//!   - session/{projectID}/{sessionID}.json  - Session metadata
//!   - message/{sessionID}/{messageID}.json  - Message metadata
//!   - part/{messageID}/{partID}.json        - Actual message content

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use walkdir::WalkDir;

use crate::connectors::{
    Connector, DetectionResult, NormalizedConversation, NormalizedMessage, ScanContext,
};

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

    /// Get the OpenCode storage directory.
    /// OpenCode stores sessions in ~/.local/share/opencode/storage/
    fn storage_root() -> Option<PathBuf> {
        // Check for env override first (useful for testing)
        if let Ok(path) = dotenvy::var("OPENCODE_STORAGE_ROOT") {
            let p = PathBuf::from(path);
            if p.exists() {
                return Some(p);
            }
        }

        // Primary location: XDG data directory (Linux/macOS)
        if let Some(data) = dirs::data_local_dir() {
            let storage_dir = data.join("opencode/storage");
            if storage_dir.exists() {
                return Some(storage_dir);
            }
        }

        // Fallback: ~/.local/share/opencode/storage
        if let Some(home) = dirs::home_dir() {
            let storage_dir = home.join(".local/share/opencode/storage");
            if storage_dir.exists() {
                return Some(storage_dir);
            }
        }

        None
    }
}

// ============================================================================
// JSON Structures for OpenCode Storage
// ============================================================================

/// Session info from session/{projectID}/{sessionID}.json
#[derive(Debug, Deserialize)]
struct SessionInfo {
    id: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    directory: Option<String>,
    #[serde(rename = "projectID", default)]
    project_id: Option<String>,
    #[serde(default)]
    time: Option<SessionTime>,
}

#[derive(Debug, Deserialize)]
struct SessionTime {
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    updated: Option<i64>,
}

/// Message info from message/{sessionID}/{messageID}.json
#[derive(Debug, Deserialize)]
struct MessageInfo {
    id: String,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    time: Option<MessageTime>,
    #[serde(rename = "modelID", default)]
    model_id: Option<String>,
    #[serde(rename = "sessionID", default)]
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageTime {
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    #[allow(dead_code)]
    completed: Option<i64>,
}

/// Part info from part/{messageID}/{partID}.json
#[derive(Debug, Clone, Deserialize)]
struct PartInfo {
    #[serde(default)]
    #[allow(dead_code)]
    id: Option<String>,
    #[serde(default, alias = "order", alias = "sequence")]
    index: Option<i64>,
    #[serde(rename = "messageID", default)]
    #[allow(dead_code)]
    message_id: Option<String>,
    #[serde(rename = "type", default)]
    part_type: Option<String>,
    #[serde(default)]
    text: Option<String>,
    // Tool state for tool parts
    #[serde(default)]
    state: Option<ToolState>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolState {
    #[serde(default)]
    output: Option<String>,
}

impl Connector for OpenCodeConnector {
    fn detect(&self) -> DetectionResult {
        if let Some(storage) = Self::storage_root() {
            DetectionResult {
                detected: true,
                evidence: vec![format!("found {}", storage.display())],
                root_paths: vec![storage],
            }
        } else {
            DetectionResult::not_found()
        }
    }

    fn scan(&self, ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        // Determine the storage root
        let storage_root = if ctx.use_default_detection() {
            if ctx.data_dir.exists() && looks_like_opencode_storage(&ctx.data_dir) {
                ctx.data_dir.clone()
            } else {
                match Self::storage_root() {
                    Some(root) => root,
                    None => return Ok(Vec::new()),
                }
            }
        } else if ctx.data_dir.exists() && looks_like_opencode_storage(&ctx.data_dir) {
            ctx.data_dir.clone()
        } else {
            return Ok(Vec::new());
        };

        let session_dir = storage_root.join("session");
        let message_dir = storage_root.join("message");
        let part_dir = storage_root.join("part");

        if !session_dir.exists() {
            return Ok(Vec::new());
        }

        // Collect all session files
        let session_files: Vec<PathBuf> = WalkDir::new(&session_dir)
            .into_iter()
            .flatten()
            .filter(|e| e.file_type().is_file())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "json")
                    .unwrap_or(false)
            })
            .map(|e| e.path().to_path_buf())
            .collect();

        let mut convs = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();

        for session_file in session_files {
            if !session_has_updates(&session_file, &message_dir, &part_dir, ctx.since_ts) {
                continue;
            }

            // Parse session
            let session = match parse_session_file(&session_file) {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(
                        "opencode: failed to parse session {}: {e}",
                        session_file.display()
                    );
                    continue;
                }
            };

            // Deduplicate by session ID
            if !seen_ids.insert(session.id.clone()) {
                continue;
            }

            // Load messages for this session
            let session_msg_dir = message_dir.join(&session.id);
            let messages = if session_msg_dir.exists() {
                load_messages(&session_msg_dir, &part_dir)?
            } else {
                Vec::new()
            };

            if messages.is_empty() {
                continue;
            }

            // Build normalized conversation
            let started_at = session
                .time
                .as_ref()
                .and_then(|t| t.created)
                .or_else(|| messages.first().and_then(|m| m.created_at));
            let ended_at = session
                .time
                .as_ref()
                .and_then(|t| t.updated)
                .or_else(|| messages.last().and_then(|m| m.created_at));

            let workspace = session.directory.map(PathBuf::from);
            let title = session.title.or_else(|| {
                messages
                    .first()
                    .and_then(|m| m.content.lines().next())
                    .map(|s| s.chars().take(100).collect())
            });

            convs.push(NormalizedConversation {
                agent_slug: "opencode".into(),
                external_id: Some(session.id.clone()),
                title,
                workspace,
                source_path: session_file.clone(),
                started_at,
                ended_at,
                metadata: serde_json::json!({
                    "session_id": session.id,
                    "project_id": session.project_id,
                }),
                messages,
            });
        }

        Ok(convs)
    }
}

/// Check if a directory looks like OpenCode storage
fn looks_like_opencode_storage(path: &std::path::Path) -> bool {
    let path_str = path.to_string_lossy().to_lowercase();
    path_str.contains("opencode")
        || path.join("session").exists()
        || path.join("message").exists()
        || path.join("part").exists()
}

fn session_has_updates(
    session_file: &Path,
    message_root: &Path,
    part_root: &Path,
    since_ts: Option<i64>,
) -> bool {
    if since_ts.is_none() {
        return true;
    }

    if crate::connectors::file_modified_since(session_file, since_ts) {
        return true;
    }

    let session_id = session_file
        .file_stem()
        .and_then(|s| s.to_str())
        .map(str::to_string);
    let Some(session_id) = session_id else {
        return true;
    };

    let session_msg_dir = message_root.join(&session_id);
    if !session_msg_dir.exists() {
        return false;
    }

    let mut message_ids = Vec::new();
    if let Ok(entries) = fs::read_dir(&session_msg_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if path.extension().map(|ext| ext == "json").unwrap_or(false) {
                if crate::connectors::file_modified_since(&path, since_ts) {
                    return true;
                }
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    message_ids.push(stem.to_string());
                }
            }
        }
    }

    for message_id in message_ids {
        let part_dir = part_root.join(&message_id);
        if !part_dir.exists() {
            continue;
        }
        if let Ok(entries) = fs::read_dir(&part_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                if crate::connectors::file_modified_since(&path, since_ts) {
                    return true;
                }
            }
        }
    }

    false
}

/// Parse a session JSON file
fn parse_session_file(path: &Path) -> Result<SessionInfo> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("read session file {}", path.display()))?;
    let session: SessionInfo = serde_json::from_str(&content)
        .with_context(|| format!("parse session JSON {}", path.display()))?;
    Ok(session)
}

/// Load all messages for a session
fn load_messages(session_msg_dir: &Path, part_dir: &Path) -> Result<Vec<NormalizedMessage>> {
    let mut pending: Vec<(Option<i64>, String, NormalizedMessage)> = Vec::new();

    // Find all message files for this session
    let msg_files: Vec<PathBuf> = WalkDir::new(session_msg_dir)
        .max_depth(1)
        .into_iter()
        .flatten()
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .map(|e| e.path().to_path_buf())
        .collect();

    for msg_file in msg_files {
        let content = match fs::read_to_string(&msg_file) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let msg_info: MessageInfo = match serde_json::from_str(&content) {
            Ok(m) => m,
            Err(_) => continue,
        };

        // Load parts for this specific message
        let mut parts = Vec::new();
        let msg_part_dir = part_dir.join(&msg_info.id);

        if msg_part_dir.exists() {
            for entry in WalkDir::new(&msg_part_dir)
                .max_depth(1)
                .into_iter()
                .flatten()
            {
                if !entry.file_type().is_file() {
                    continue;
                }
                let path = entry.path();
                if path.extension().map(|e| e == "json").unwrap_or(false)
                    && let Ok(content) = fs::read_to_string(path)
                    && let Ok(part) = serde_json::from_str::<PartInfo>(&content)
                {
                    parts.push(part);
                }
            }
        }
        sort_parts_for_message(&mut parts);

        // Assemble message content from parts
        let content_text = assemble_content_from_parts(&parts);
        if content_text.trim().is_empty() {
            continue;
        }

        // Determine role
        let role = msg_info
            .role
            .clone()
            .unwrap_or_else(|| "assistant".to_string());

        // Determine timestamp
        let created_at = msg_info.time.as_ref().and_then(|t| t.created);

        // Author from model_id for assistant messages
        let author = if role == "assistant" {
            msg_info.model_id.clone()
        } else {
            Some("user".to_string())
        };

        let message_id = msg_info.id.clone();
        pending.push((
            created_at,
            message_id.clone(),
            NormalizedMessage {
                idx: 0, // Will be assigned later
                role,
                author,
                created_at,
                content: content_text,
                extra: serde_json::json!({
                    "message_id": message_id,
                    "session_id": msg_info.session_id,
                }),
                snippets: Vec::new(),
            },
        ));
    }

    // Sort by timestamp, then by message id to ensure deterministic ordering.
    pending.sort_by(|a, b| {
        let a_ts = a.0.unwrap_or(i64::MAX);
        let b_ts = b.0.unwrap_or(i64::MAX);
        a_ts.cmp(&b_ts).then_with(|| a.1.cmp(&b.1))
    });
    let mut messages: Vec<NormalizedMessage> = pending.into_iter().map(|(_, _, msg)| msg).collect();
    super::reindex_messages(&mut messages);

    Ok(messages)
}

fn sort_parts_for_message(parts: &mut [PartInfo]) {
    parts.sort_by(|a, b| {
        let a_idx = a.index.unwrap_or(i64::MAX);
        let b_idx = b.index.unwrap_or(i64::MAX);
        a_idx
            .cmp(&b_idx)
            .then_with(|| {
                a.id.as_deref()
                    .unwrap_or("")
                    .cmp(b.id.as_deref().unwrap_or(""))
            })
            .then_with(|| {
                a.part_type
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.part_type.as_deref().unwrap_or(""))
            })
            .then_with(|| {
                a.text
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.text.as_deref().unwrap_or(""))
            })
    });
}

/// Assemble message content from parts
fn assemble_content_from_parts(parts: &[PartInfo]) -> String {
    let mut content_pieces: Vec<String> = Vec::new();

    for part in parts {
        match part.part_type.as_deref() {
            Some("text") => {
                if let Some(text) = &part.text
                    && !text.trim().is_empty()
                {
                    content_pieces.push(text.clone());
                }
            }
            Some("tool") => {
                // Include tool output if available
                if let Some(state) = &part.state
                    && let Some(output) = &state.output
                    && !output.trim().is_empty()
                {
                    content_pieces.push(format!("[Tool Output]\n{}", output));
                }
            }
            Some("reasoning") => {
                if let Some(text) = &part.text
                    && !text.trim().is_empty()
                {
                    content_pieces.push(format!("[Reasoning]\n{}", text));
                }
            }
            Some("patch") => {
                if let Some(text) = &part.text
                    && !text.trim().is_empty()
                {
                    content_pieces.push(format!("[Patch]\n{}", text));
                }
            }
            // Ignore step-start, step-finish, and other control parts
            _ => {}
        }
    }

    content_pieces.join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    // =====================================================
    // Constructor Tests
    // =====================================================

    #[test]
    fn new_creates_connector() {
        let connector = OpenCodeConnector::new();
        let _ = connector;
    }

    #[test]
    fn default_creates_connector() {
        let connector = OpenCodeConnector;
        let _ = connector;
    }

    // =====================================================
    // looks_like_opencode_storage() Tests
    // =====================================================

    #[test]
    fn looks_like_opencode_storage_with_opencode_in_path() {
        let dir = TempDir::new().unwrap();
        let opencode_path = dir.path().join("opencode").join("test");
        fs::create_dir_all(&opencode_path).unwrap();
        assert!(looks_like_opencode_storage(&opencode_path));
    }

    #[test]
    fn looks_like_opencode_storage_with_session_dir() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("session")).unwrap();
        assert!(looks_like_opencode_storage(dir.path()));
    }

    #[test]
    fn looks_like_opencode_storage_with_message_dir() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("message")).unwrap();
        assert!(looks_like_opencode_storage(dir.path()));
    }

    #[test]
    fn looks_like_opencode_storage_with_part_dir() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("part")).unwrap();
        assert!(looks_like_opencode_storage(dir.path()));
    }

    #[test]
    fn looks_like_opencode_storage_returns_false_for_random_dir() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("random")).unwrap();
        assert!(!looks_like_opencode_storage(dir.path()));
    }

    // =====================================================
    // session_has_updates() Tests
    // =====================================================

    #[test]
    fn session_has_updates_detects_message_file_change() {
        let dir = TempDir::new().unwrap();
        let storage = dir.path();
        let session_dir = storage.join("session/proj");
        let message_dir = storage.join("message/session-1");
        let part_dir = storage.join("part");
        fs::create_dir_all(&session_dir).unwrap();
        fs::create_dir_all(&message_dir).unwrap();
        fs::create_dir_all(&part_dir).unwrap();

        let session_file = session_dir.join("session-1.json");
        fs::write(&session_file, r#"{"id":"session-1"}"#).unwrap();

        let message_file = message_dir.join("msg-1.json");
        fs::write(&message_file, r#"{"id":"msg-1","role":"user"}"#).unwrap();

        let since_ts = file_mtime_ms(&message_file);

        let updated_message_file = message_dir.join("msg-2.json");
        fs::write(&updated_message_file, r#"{"id":"msg-2","role":"user"}"#).unwrap();

        assert!(session_has_updates(
            &session_file,
            &storage.join("message"),
            &storage.join("part"),
            Some(since_ts)
        ));
    }

    #[test]
    fn session_has_updates_detects_part_file_change() {
        let dir = TempDir::new().unwrap();
        let storage = dir.path();
        let session_dir = storage.join("session/proj");
        let message_dir = storage.join("message/session-1");
        let part_dir = storage.join("part");
        fs::create_dir_all(&session_dir).unwrap();
        fs::create_dir_all(&message_dir).unwrap();
        fs::create_dir_all(&part_dir).unwrap();

        let session_file = session_dir.join("session-1.json");
        fs::write(&session_file, r#"{"id":"session-1"}"#).unwrap();

        let message_file = message_dir.join("msg-1.json");
        fs::write(&message_file, r#"{"id":"msg-1","role":"assistant"}"#).unwrap();

        let since_ts = file_mtime_ms(&message_file);

        let part_dir_for_message = part_dir.join("msg-1");
        fs::create_dir_all(&part_dir_for_message).unwrap();
        fs::write(part_dir_for_message.join("part-1.json"), r#"{"text":"hi"}"#).unwrap();

        assert!(session_has_updates(
            &session_file,
            &storage.join("message"),
            &storage.join("part"),
            Some(since_ts)
        ));
    }

    fn file_mtime_ms(path: &Path) -> i64 {
        std::fs::metadata(path)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    // =====================================================
    // assemble_content_from_parts() Tests
    // =====================================================

    #[test]
    fn assemble_content_from_text_parts() {
        let parts = vec![
            PartInfo {
                id: Some("p1".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("text".into()),
                text: Some("Hello, world!".into()),
                state: None,
            },
            PartInfo {
                id: Some("p2".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("text".into()),
                text: Some("Second part".into()),
                state: None,
            },
        ];
        let content = assemble_content_from_parts(&parts);
        assert!(content.contains("Hello, world!"));
        assert!(content.contains("Second part"));
    }

    #[test]
    fn assemble_content_from_tool_parts() {
        let parts = vec![PartInfo {
            id: Some("p1".into()),
            index: None,
            message_id: Some("m1".into()),
            part_type: Some("tool".into()),
            text: None,
            state: Some(ToolState {
                output: Some("Tool executed successfully".into()),
            }),
        }];
        let content = assemble_content_from_parts(&parts);
        assert!(content.contains("[Tool Output]"));
        assert!(content.contains("Tool executed successfully"));
    }

    #[test]
    fn assemble_content_from_reasoning_parts() {
        let parts = vec![PartInfo {
            id: Some("p1".into()),
            index: None,
            message_id: Some("m1".into()),
            part_type: Some("reasoning".into()),
            text: Some("Let me think about this...".into()),
            state: None,
        }];
        let content = assemble_content_from_parts(&parts);
        assert!(content.contains("[Reasoning]"));
        assert!(content.contains("Let me think about this..."));
    }

    #[test]
    fn assemble_content_from_patch_parts() {
        let parts = vec![PartInfo {
            id: Some("p1".into()),
            index: None,
            message_id: Some("m1".into()),
            part_type: Some("patch".into()),
            text: Some("@@ -1,3 +1,4 @@".into()),
            state: None,
        }];
        let content = assemble_content_from_parts(&parts);
        assert!(content.contains("[Patch]"));
        assert!(content.contains("@@ -1,3 +1,4 @@"));
    }

    #[test]
    fn assemble_content_skips_empty_text() {
        let parts = vec![
            PartInfo {
                id: Some("p1".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("text".into()),
                text: Some("".into()),
                state: None,
            },
            PartInfo {
                id: Some("p2".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("text".into()),
                text: Some("   ".into()),
                state: None,
            },
            PartInfo {
                id: Some("p3".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("text".into()),
                text: Some("Actual content".into()),
                state: None,
            },
        ];
        let content = assemble_content_from_parts(&parts);
        assert_eq!(content, "Actual content");
    }

    #[test]
    fn assemble_content_skips_unknown_part_types() {
        let parts = vec![
            PartInfo {
                id: Some("p1".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("step-start".into()),
                text: Some("Starting...".into()),
                state: None,
            },
            PartInfo {
                id: Some("p2".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("step-finish".into()),
                text: Some("Done".into()),
                state: None,
            },
        ];
        let content = assemble_content_from_parts(&parts);
        assert!(content.is_empty());
    }

    #[test]
    fn assemble_content_mixed_parts() {
        let parts = vec![
            PartInfo {
                id: Some("p1".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("text".into()),
                text: Some("Here's my analysis:".into()),
                state: None,
            },
            PartInfo {
                id: Some("p2".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("reasoning".into()),
                text: Some("Thinking...".into()),
                state: None,
            },
            PartInfo {
                id: Some("p3".into()),
                index: None,
                message_id: Some("m1".into()),
                part_type: Some("tool".into()),
                text: None,
                state: Some(ToolState {
                    output: Some("Result: 42".into()),
                }),
            },
        ];
        let content = assemble_content_from_parts(&parts);
        assert!(content.contains("Here's my analysis:"));
        assert!(content.contains("[Reasoning]"));
        assert!(content.contains("[Tool Output]"));
    }

    #[test]
    fn sort_parts_for_message_orders_by_index_then_id() {
        let mut parts = vec![
            PartInfo {
                id: Some("b".into()),
                index: Some(2),
                message_id: Some("m1".into()),
                part_type: Some("text".into()),
                text: Some("second".into()),
                state: None,
            },
            PartInfo {
                id: Some("a".into()),
                index: Some(1),
                message_id: Some("m1".into()),
                part_type: Some("text".into()),
                text: Some("first".into()),
                state: None,
            },
        ];

        sort_parts_for_message(&mut parts);
        assert_eq!(parts[0].text.as_deref(), Some("first"));
        assert_eq!(parts[1].text.as_deref(), Some("second"));
    }

    // =====================================================
    // Helper: Create OpenCode storage structure
    // =====================================================

    fn create_opencode_storage(dir: &TempDir) -> PathBuf {
        let storage = dir.path().join("opencode").join("storage");
        fs::create_dir_all(storage.join("session")).unwrap();
        fs::create_dir_all(storage.join("message")).unwrap();
        fs::create_dir_all(storage.join("part")).unwrap();
        storage
    }

    fn write_session(storage: &Path, project_id: &str, session: &serde_json::Value) {
        let session_id = session["id"].as_str().unwrap();
        let session_dir = storage.join("session").join(project_id);
        fs::create_dir_all(&session_dir).unwrap();
        fs::write(
            session_dir.join(format!("{session_id}.json")),
            session.to_string(),
        )
        .unwrap();
    }

    fn write_message(storage: &Path, session_id: &str, message: &serde_json::Value) {
        let message_id = message["id"].as_str().unwrap();
        let message_dir = storage.join("message").join(session_id);
        fs::create_dir_all(&message_dir).unwrap();
        fs::write(
            message_dir.join(format!("{message_id}.json")),
            message.to_string(),
        )
        .unwrap();
    }

    fn write_part(storage: &Path, message_id: &str, part: &serde_json::Value) {
        let part_id = part["id"].as_str().unwrap();
        let part_dir = storage.join("part").join(message_id);
        fs::create_dir_all(&part_dir).unwrap();
        fs::write(part_dir.join(format!("{part_id}.json")), part.to_string()).unwrap();
    }

    // =====================================================
    // scan() Tests
    // =====================================================

    #[test]
    fn scan_parses_simple_conversation() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        // Create session
        let session = json!({
            "id": "sess-001",
            "title": "Test Session",
            "directory": "/home/user/project",
            "projectID": "proj-001",
            "time": {
                "created": 1733000000,
                "updated": 1733000100
            }
        });
        write_session(&storage, "proj-001", &session);

        // Create message
        let message = json!({
            "id": "msg-001",
            "role": "user",
            "sessionID": "sess-001",
            "time": {
                "created": 1733000000,
                "completed": 1733000001
            }
        });
        write_message(&storage, "sess-001", &message);

        // Create part
        let part = json!({
            "id": "part-001",
            "messageID": "msg-001",
            "type": "text",
            "text": "Hello, OpenCode!"
        });
        write_part(&storage, "msg-001", &part);

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs.len(), 1);
        assert_eq!(convs[0].title, Some("Test Session".to_string()));
        assert_eq!(
            convs[0].workspace,
            Some(PathBuf::from("/home/user/project"))
        );
        assert_eq!(convs[0].messages.len(), 1);
        assert_eq!(convs[0].messages[0].role, "user");
        assert!(convs[0].messages[0].content.contains("Hello, OpenCode!"));
    }

    #[test]
    fn scan_parses_multiple_messages() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-002",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);

        // User message
        let user_msg = json!({
            "id": "msg-u1",
            "role": "user",
            "sessionID": "sess-002",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-002", &user_msg);
        write_part(
            &storage,
            "msg-u1",
            &json!({
                "id": "p1",
                "messageID": "msg-u1",
                "type": "text",
                "text": "What is 2+2?"
            }),
        );

        // Assistant message
        let assistant_msg = json!({
            "id": "msg-a1",
            "role": "assistant",
            "sessionID": "sess-002",
            "modelID": "gpt-4",
            "time": {"created": 1733000001}
        });
        write_message(&storage, "sess-002", &assistant_msg);
        write_part(
            &storage,
            "msg-a1",
            &json!({
                "id": "p2",
                "messageID": "msg-a1",
                "type": "text",
                "text": "2 + 2 = 4"
            }),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs.len(), 1);
        assert_eq!(convs[0].messages.len(), 2);
        assert_eq!(convs[0].messages[0].role, "user");
        assert_eq!(convs[0].messages[1].role, "assistant");
        assert_eq!(convs[0].messages[1].author, Some("gpt-4".to_string()));
    }

    #[test]
    fn scan_handles_empty_storage() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn scan_skips_sessions_without_messages() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-empty",
            "title": "Empty Session",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);
        // Don't create any messages

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn scan_extracts_title_from_first_message_if_no_session_title() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-no-title",
            "projectID": "proj-001"
            // No title field
        });
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-001",
            "role": "user",
            "sessionID": "sess-no-title",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-no-title", &message);
        write_part(
            &storage,
            "msg-001",
            &json!({
                "id": "p1",
                "messageID": "msg-001",
                "type": "text",
                "text": "This is the first line\nSecond line\nThird line"
            }),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs[0].title, Some("This is the first line".to_string()));
    }

    #[test]
    fn scan_sets_agent_slug_to_opencode() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-slug",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-001",
            "role": "user",
            "sessionID": "sess-slug",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-slug", &message);
        write_part(
            &storage,
            "msg-001",
            &json!({"id": "p1", "messageID": "msg-001", "type": "text", "text": "Test"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs[0].agent_slug, "opencode");
    }

    #[test]
    fn scan_sets_metadata_with_session_and_project_id() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-meta",
            "projectID": "proj-meta-001"
        });
        write_session(&storage, "proj-meta-001", &session);

        let message = json!({
            "id": "msg-001",
            "role": "user",
            "sessionID": "sess-meta",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-meta", &message);
        write_part(
            &storage,
            "msg-001",
            &json!({"id": "p1", "messageID": "msg-001", "type": "text", "text": "Test"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs[0].metadata["session_id"], "sess-meta");
        assert_eq!(convs[0].metadata["project_id"], "proj-meta-001");
    }

    #[test]
    fn scan_sorts_messages_by_timestamp() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-sort",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);

        // Create messages out of order
        let msg_later = json!({
            "id": "msg-later",
            "role": "assistant",
            "sessionID": "sess-sort",
            "time": {"created": 1733000100}
        });
        let msg_earlier = json!({
            "id": "msg-earlier",
            "role": "user",
            "sessionID": "sess-sort",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-sort", &msg_later);
        write_message(&storage, "sess-sort", &msg_earlier);

        write_part(
            &storage,
            "msg-later",
            &json!({"id": "p1", "messageID": "msg-later", "type": "text", "text": "Later"}),
        );
        write_part(
            &storage,
            "msg-earlier",
            &json!({"id": "p2", "messageID": "msg-earlier", "type": "text", "text": "Earlier"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs[0].messages.len(), 2);
        // Earlier message should be first due to sorting
        assert!(convs[0].messages[0].content.contains("Earlier"));
        assert!(convs[0].messages[1].content.contains("Later"));
    }

    #[test]
    fn scan_assigns_sequential_indices() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-idx",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);

        for i in 0..3 {
            let msg = json!({
                "id": format!("msg-{i}"),
                "role": "user",
                "sessionID": "sess-idx",
                "time": {"created": 1733000000 + i}
            });
            write_message(&storage, "sess-idx", &msg);
            write_part(
                &storage,
                &format!("msg-{i}"),
                &json!({
                    "id": format!("p{i}"),
                    "messageID": format!("msg-{i}"),
                    "type": "text",
                    "text": format!("Message {i}")
                }),
            );
        }

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs[0].messages[0].idx, 0);
        assert_eq!(convs[0].messages[1].idx, 1);
        assert_eq!(convs[0].messages[2].idx, 2);
    }

    #[test]
    fn scan_handles_messages_without_parts() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-no-parts",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-no-parts",
            "role": "user",
            "sessionID": "sess-no-parts",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-no-parts", &message);
        // Don't create any parts

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        // Session should be skipped because message has no content
        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn scan_deduplicates_sessions_by_id() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        // Create same session in two project directories
        let session = json!({
            "id": "sess-dupe",
            "title": "Duplicate Session",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);
        write_session(&storage, "proj-002", &session);

        let message = json!({
            "id": "msg-001",
            "role": "user",
            "sessionID": "sess-dupe",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-dupe", &message);
        write_part(
            &storage,
            "msg-001",
            &json!({"id": "p1", "messageID": "msg-001", "type": "text", "text": "Test"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        // Should only have one conversation (deduplicated)
        assert_eq!(convs.len(), 1);
    }

    #[test]
    fn scan_uses_default_role_when_missing() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-no-role",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);

        // Message without role field
        let message = json!({
            "id": "msg-no-role",
            "sessionID": "sess-no-role",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-no-role", &message);
        write_part(
            &storage,
            "msg-no-role",
            &json!({"id": "p1", "messageID": "msg-no-role", "type": "text", "text": "Test"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        // Default role should be "assistant"
        assert_eq!(convs[0].messages[0].role, "assistant");
    }

    #[test]
    fn scan_handles_multiple_parts_per_message() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-multi-part",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-multi",
            "role": "assistant",
            "sessionID": "sess-multi-part",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-multi-part", &message);

        // Multiple parts for one message
        write_part(
            &storage,
            "msg-multi",
            &json!({"id": "p1", "messageID": "msg-multi", "type": "text", "text": "First part"}),
        );
        write_part(
            &storage,
            "msg-multi",
            &json!({"id": "p2", "messageID": "msg-multi", "type": "reasoning", "text": "Reasoning part"}),
        );
        write_part(
            &storage,
            "msg-multi",
            &json!({"id": "p3", "messageID": "msg-multi", "type": "text", "text": "Third part"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        let content = &convs[0].messages[0].content;
        assert!(content.contains("First part"));
        assert!(content.contains("[Reasoning]"));
        assert!(content.contains("Third part"));
    }

    #[test]
    fn scan_extracts_timestamps() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-ts",
            "projectID": "proj-001",
            "time": {
                "created": 1733000000,
                "updated": 1733000200
            }
        });
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-ts",
            "role": "user",
            "sessionID": "sess-ts",
            "time": {"created": 1733000050}
        });
        write_message(&storage, "sess-ts", &message);
        write_part(
            &storage,
            "msg-ts",
            &json!({"id": "p1", "messageID": "msg-ts", "type": "text", "text": "Test"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs[0].started_at, Some(1733000000));
        assert_eq!(convs[0].ended_at, Some(1733000200));
        assert_eq!(convs[0].messages[0].created_at, Some(1733000050));
    }

    #[test]
    fn scan_uses_external_id_from_session_id() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "unique-session-id-123",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-001",
            "role": "user",
            "sessionID": "unique-session-id-123",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "unique-session-id-123", &message);
        write_part(
            &storage,
            "msg-001",
            &json!({"id": "p1", "messageID": "msg-001", "type": "text", "text": "Test"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(
            convs[0].external_id,
            Some("unique-session-id-123".to_string())
        );
    }

    #[test]
    fn scan_skips_invalid_session_json() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        // Create invalid session file
        let session_dir = storage.join("session").join("proj-001");
        fs::create_dir_all(&session_dir).unwrap();
        fs::write(session_dir.join("invalid.json"), "not valid json").unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn scan_skips_invalid_message_json() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({
            "id": "sess-invalid-msg",
            "projectID": "proj-001"
        });
        write_session(&storage, "proj-001", &session);

        // Create invalid message file
        let msg_dir = storage.join("message").join("sess-invalid-msg");
        fs::create_dir_all(&msg_dir).unwrap();
        fs::write(msg_dir.join("bad.json"), "not valid json").unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();

        // Should skip the session because no valid messages
        assert_eq!(convs.len(), 0);
    }

    // =====================================================
    // parse_session_file() Tests
    // =====================================================

    #[test]
    fn parse_session_file_parses_complete_session() {
        let dir = TempDir::new().unwrap();
        let session = json!({
            "id": "sess-parse",
            "title": "Parse Test",
            "directory": "/test/dir",
            "projectID": "proj-parse",
            "time": {
                "created": 1733000000,
                "updated": 1733000100
            }
        });
        let path = dir.path().join("session.json");
        fs::write(&path, session.to_string()).unwrap();

        let result = parse_session_file(&path).unwrap();
        assert_eq!(result.id, "sess-parse");
        assert_eq!(result.title, Some("Parse Test".to_string()));
        assert_eq!(result.directory, Some("/test/dir".to_string()));
        assert_eq!(result.project_id, Some("proj-parse".to_string()));
        assert!(result.time.is_some());
    }

    #[test]
    fn parse_session_file_handles_minimal_session() {
        let dir = TempDir::new().unwrap();
        let session = json!({"id": "minimal"});
        let path = dir.path().join("minimal.json");
        fs::write(&path, session.to_string()).unwrap();

        let result = parse_session_file(&path).unwrap();
        assert_eq!(result.id, "minimal");
        assert!(result.title.is_none());
        assert!(result.directory.is_none());
    }

    // =========================================================================
    // Edge case tests — malformed input robustness (br-2w98)
    // =========================================================================

    #[test]
    fn edge_empty_session_file_returns_no_conversations() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);
        let session_dir = storage.join("session").join("proj-001");
        fs::create_dir_all(&session_dir).unwrap();
        fs::write(session_dir.join("sess-empty.json"), "").unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn edge_whitespace_only_session_file_skipped() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);
        let session_dir = storage.join("session").join("proj-001");
        fs::create_dir_all(&session_dir).unwrap();
        fs::write(session_dir.join("sess-ws.json"), "   \n\t  ").unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn edge_truncated_session_json_handled() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);
        let session_dir = storage.join("session").join("proj-001");
        fs::create_dir_all(&session_dir).unwrap();
        fs::write(
            session_dir.join("sess-trunc.json"),
            r#"{"id": "sess-trunc", "title": "Trun"#,
        )
        .unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn edge_invalid_utf8_session_skipped() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);
        let session_dir = storage.join("session").join("proj-001");
        fs::create_dir_all(&session_dir).unwrap();
        std::fs::write(
            session_dir.join("sess-bad-utf8.json"),
            b"\xff\xfe{\"id\":\"bad\"}",
        )
        .unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn edge_bom_marker_at_session_file_handled() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);
        let session_dir = storage.join("session").join("proj-001");
        fs::create_dir_all(&session_dir).unwrap();

        let mut data = vec![0xEF, 0xBB, 0xBF];
        data.extend_from_slice(br#"{"id":"sess-bom","projectID":"proj-001"}"#);
        std::fs::write(session_dir.join("sess-bom.json"), &data).unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        // BOM may cause parse failure; connector should skip gracefully
        let convs = connector.scan(&ctx).unwrap();
        assert!(convs.len() <= 1);
    }

    #[test]
    fn edge_json_type_mismatch_in_session_file() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);
        let session_dir = storage.join("session").join("proj-001");
        fs::create_dir_all(&session_dir).unwrap();
        // id should be a string, give it a number
        fs::write(session_dir.join("sess-bad.json"), r#"{"id": 12345}"#).unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        // Should skip since id is not a string (serde will fail)
        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn edge_deeply_nested_part_json() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({"id": "sess-deep", "projectID": "proj-001"});
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-deep",
            "role": "user",
            "sessionID": "sess-deep",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-deep", &message);

        // Create a part with deeply nested extra data
        let mut nested = String::from(r#"{"id":"p-deep","messageID":"msg-deep","type":"text","text":"deep test","extra":"#);
        for _ in 0..200 {
            nested.push_str(r#"{"a":"#);
        }
        nested.push_str(r#""leaf""#);
        for _ in 0..200 {
            nested.push('}');
        }
        nested.push('}');
        let part_dir = storage.join("part").join("msg-deep");
        fs::create_dir_all(&part_dir).unwrap();
        fs::write(part_dir.join("p-deep.json"), &nested).unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        // Should not stack overflow
        let result = connector.scan(&ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn edge_large_part_text_handled() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({"id": "sess-large", "projectID": "proj-001"});
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-large",
            "role": "user",
            "sessionID": "sess-large",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-large", &message);

        let large_text = "x".repeat(1_000_000);
        write_part(
            &storage,
            "msg-large",
            &json!({"id": "p-large", "messageID": "msg-large", "type": "text", "text": large_text}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        assert_eq!(convs.len(), 1);
        assert!(convs[0].messages[0].content.len() >= 1_000_000);
    }

    #[test]
    fn edge_null_bytes_in_part_content() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({"id": "sess-null", "projectID": "proj-001"});
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-null",
            "role": "user",
            "sessionID": "sess-null",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-null", &message);

        write_part(
            &storage,
            "msg-null",
            &json!({"id": "p-null", "messageID": "msg-null", "type": "text", "text": "hello\u{0000}world"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        assert_eq!(convs.len(), 1);
        assert!(convs[0].messages[0].content.contains("hello"));
    }

    #[test]
    fn edge_whitespace_only_part_text_skipped() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({"id": "sess-ws-part", "projectID": "proj-001"});
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-ws",
            "role": "assistant",
            "sessionID": "sess-ws-part",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-ws-part", &message);

        // Part with only whitespace text
        write_part(
            &storage,
            "msg-ws",
            &json!({"id": "p-ws", "messageID": "msg-ws", "type": "text", "text": "   \n\t  "}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        // Message with only whitespace content should be skipped
        assert_eq!(convs.len(), 0);
    }

    // ---- OpenCode-specific edge cases ----

    #[test]
    fn edge_corrupted_message_file_skipped() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({"id": "sess-corrupt", "projectID": "proj-001"});
        write_session(&storage, "proj-001", &session);

        // Write a valid message and a corrupted one
        let valid_msg = json!({
            "id": "msg-valid",
            "role": "user",
            "sessionID": "sess-corrupt",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-corrupt", &valid_msg);
        write_part(
            &storage,
            "msg-valid",
            &json!({"id": "p1", "messageID": "msg-valid", "type": "text", "text": "Valid message"}),
        );

        // Corrupted message file
        let msg_dir = storage.join("message").join("sess-corrupt");
        fs::write(msg_dir.join("msg-corrupt.json"), "{{{{not json").unwrap();

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        // Valid message should still be parsed; corrupted one skipped
        assert_eq!(convs.len(), 1);
        assert_eq!(convs[0].messages.len(), 1);
        assert!(convs[0].messages[0].content.contains("Valid message"));
    }

    #[test]
    fn edge_missing_part_directory_handled() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({"id": "sess-nopart", "projectID": "proj-001"});
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-nopartdir",
            "role": "user",
            "sessionID": "sess-nopart",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-nopart", &message);
        // Don't create part directory at all (not even the part/msg-nopartdir/ dir)

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        // Message without parts should be skipped (empty content)
        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn edge_part_with_no_type_field_ignored() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({"id": "sess-notype", "projectID": "proj-001"});
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-notype",
            "role": "assistant",
            "sessionID": "sess-notype",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-notype", &message);

        // Part without "type" field (falls through to _ => {} in match)
        write_part(
            &storage,
            "msg-notype",
            &json!({"id": "p-notype", "messageID": "msg-notype", "text": "No type field"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        // Part without type is ignored, message has no content, so session skipped
        assert_eq!(convs.len(), 0);
    }

    #[test]
    fn edge_part_ordering_preserves_index_order() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        let session = json!({"id": "sess-order", "projectID": "proj-001"});
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-order",
            "role": "assistant",
            "sessionID": "sess-order",
            "time": {"created": 1733000000}
        });
        write_message(&storage, "sess-order", &message);

        // Parts with explicit indices out of order
        write_part(
            &storage,
            "msg-order",
            &json!({"id": "p-c", "messageID": "msg-order", "type": "text", "text": "Third", "index": 3}),
        );
        write_part(
            &storage,
            "msg-order",
            &json!({"id": "p-a", "messageID": "msg-order", "type": "text", "text": "First", "index": 1}),
        );
        write_part(
            &storage,
            "msg-order",
            &json!({"id": "p-b", "messageID": "msg-order", "type": "text", "text": "Second", "index": 2}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        assert_eq!(convs.len(), 1);
        let content = &convs[0].messages[0].content;
        // Verify order: First before Second before Third
        let first_pos = content.find("First").unwrap();
        let second_pos = content.find("Second").unwrap();
        let third_pos = content.find("Third").unwrap();
        assert!(first_pos < second_pos);
        assert!(second_pos < third_pos);
    }

    #[test]
    fn edge_session_without_time_field() {
        let dir = TempDir::new().unwrap();
        let storage = create_opencode_storage(&dir);

        // Session with no time field at all
        let session = json!({"id": "sess-notime", "projectID": "proj-001"});
        write_session(&storage, "proj-001", &session);

        let message = json!({
            "id": "msg-notime",
            "role": "user",
            "sessionID": "sess-notime"
            // No time field
        });
        write_message(&storage, "sess-notime", &message);
        write_part(
            &storage,
            "msg-notime",
            &json!({"id": "p1", "messageID": "msg-notime", "type": "text", "text": "No timestamps"}),
        );

        let connector = OpenCodeConnector::new();
        let ctx = ScanContext::local_default(storage.clone(), None);
        let convs = connector.scan(&ctx).unwrap();
        assert_eq!(convs.len(), 1);
        // Timestamps should be None
        assert!(convs[0].started_at.is_none());
        assert!(convs[0].ended_at.is_none());
    }
}
