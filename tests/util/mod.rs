use coding_agent_search::connectors::{
    NormalizedConversation, NormalizedMessage, NormalizedSnippet,
};
use coding_agent_search::model::types::{Conversation, Message, MessageRole, Snippet};
use coding_agent_search::search::query::{MatchType, SearchHit};
use serde_json::json;
use std::path::PathBuf;
use tempfile::TempDir;

/// Captures tracing output for tests.
#[allow(dead_code)]
pub struct TestTracing {
    buffer: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

#[allow(dead_code)]
impl TestTracing {
    pub fn new() -> Self {
        Self {
            buffer: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        let writer = self.buffer.clone();
        let make_writer = move || TestWriter(writer.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(make_writer)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    pub fn output(&self) -> String {
        let buf = self.buffer.lock().unwrap();
        String::from_utf8_lossy(&buf).to_string()
    }

    /// Assert that the captured log output contains the provided substring.
    pub fn assert_contains(&self, needle: &str) {
        let out = self.output();
        assert!(
            out.contains(needle),
            "expected logs to contain `{needle}`, got:\n{out}"
        );
    }

    /// Return captured log lines (trimmed of trailing newline) for fine-grained checks.
    pub fn lines(&self) -> Vec<String> {
        self.output()
            .lines()
            .map(std::string::ToString::to_string)
            .collect()
    }
}

#[allow(dead_code)]
pub struct EnvGuard {
    key: String,
    prev: Option<String>,
}

#[allow(dead_code)]
impl EnvGuard {
    pub fn set(key: &str, val: impl AsRef<str>) -> Self {
        let prev = std::env::var(key).ok();
        unsafe { std::env::set_var(key, val.as_ref()) };
        Self {
            key: key.to_string(),
            prev,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => unsafe { std::env::set_var(&self.key, v) },
            None => unsafe { std::env::remove_var(&self.key) },
        }
    }
}

/// RAII guard for changing the current working directory.
/// Automatically restores the previous directory on drop, even if a test panics.
#[allow(dead_code)]
pub struct CwdGuard {
    prev: PathBuf,
}

#[allow(dead_code)]
impl CwdGuard {
    /// Change to the given directory and return a guard that restores the previous directory on drop.
    pub fn change_to(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let prev = std::env::current_dir()?;
        std::env::set_current_dir(path.as_ref())?;
        Ok(Self { prev })
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        // Best effort restore - ignore errors during drop
        let _ = std::env::set_current_dir(&self.prev);
    }
}

struct TestWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.lock().unwrap();
        guard.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[allow(dead_code)]
pub struct TempFixtureDir {
    pub dir: TempDir,
}

#[allow(dead_code)]
impl TempFixtureDir {
    pub fn new() -> Self {
        Self {
            dir: TempDir::new().expect("tempdir"),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.dir.path().to_path_buf()
    }
}

use std::collections::HashMap;

/// Deterministic conversation/message generator for tests.
#[derive(Debug, Clone)]
pub struct ConversationFixtureBuilder {
    agent_slug: String,
    external_id: Option<String>,
    workspace: Option<PathBuf>,
    source_path: PathBuf,
    base_ts: i64,
    content_prefix: String,
    message_count: usize,
    snippets: Vec<SnippetSpec>,
    custom_content: HashMap<usize, String>,
    title: Option<String>,
}

#[allow(dead_code)]
impl ConversationFixtureBuilder {
    pub fn new(agent_slug: impl Into<String>) -> Self {
        let agent_slug = agent_slug.into();
        let source_path = PathBuf::from(format!("/tmp/{agent_slug}/session-0.jsonl"));
        Self {
            agent_slug,
            external_id: None,
            workspace: None,
            source_path,
            base_ts: 1_700_000_000_000, // stable timestamp for deterministic tests
            content_prefix: "msg".into(),
            message_count: 2,
            snippets: Vec::new(),
            custom_content: HashMap::new(),
            title: None,
        }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn external_id(mut self, id: impl Into<String>) -> Self {
        self.external_id = Some(id.into());
        self
    }

    pub fn workspace(mut self, path: impl Into<PathBuf>) -> Self {
        self.workspace = Some(path.into());
        self
    }

    pub fn source_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.source_path = path.into();
        self
    }

    pub fn base_ts(mut self, ts: i64) -> Self {
        self.base_ts = ts;
        self
    }

    pub fn content_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.content_prefix = prefix.into();
        self
    }

    pub fn messages(mut self, count: usize) -> Self {
        self.message_count = count.max(1);
        self
    }

    pub fn with_content(mut self, idx: usize, content: impl Into<String>) -> Self {
        self.custom_content.insert(idx, content.into());
        // Ensure message count covers this index
        if idx >= self.message_count {
            self.message_count = idx + 1;
        }
        self
    }

    /// Attach a snippet to a specific message index (0-based).
    pub fn with_snippet(mut self, spec: SnippetSpec) -> Self {
        self.snippets.push(spec);
        self
    }

    /// Convenience: attach a snippet with text/language to the first message.
    pub fn with_snippet_text(self, text: impl Into<String>, language: impl Into<String>) -> Self {
        self.with_snippet(
            SnippetSpec::new(0)
                .text(text)
                .language(language)
                .lines(1, 1),
        )
    }

    /// Build a `NormalizedConversation` (connector-facing).
    pub fn build_normalized(self) -> NormalizedConversation {
        let messages: Vec<NormalizedMessage> = (0..self.message_count)
            .map(|i| {
                let is_user = i % 2 == 0;
                let snippets: Vec<NormalizedSnippet> = self
                    .snippets
                    .iter()
                    .filter(|s| s.msg_idx == i)
                    .map(|s| NormalizedSnippet {
                        file_path: s.file_path.clone(),
                        start_line: s.start_line,
                        end_line: s.end_line,
                        language: s.language.clone(),
                        snippet_text: s.text.clone(),
                    })
                    .collect();

                let content = self
                    .custom_content
                    .get(&i)
                    .cloned()
                    .unwrap_or_else(|| format!("{}-{}", self.content_prefix, i));

                NormalizedMessage {
                    idx: i as i64,
                    role: if is_user { "user" } else { "assistant" }.into(),
                    author: if is_user {
                        Some("user".into())
                    } else {
                        Some("agent".into())
                    },
                    created_at: Some(self.base_ts + i as i64),
                    content,
                    extra: json!({"seed": i}),
                    snippets,
                }
            })
            .collect();

        NormalizedConversation {
            agent_slug: self.agent_slug.clone(),
            external_id: self.external_id.clone(),
            title: self
                .title
                .or_else(|| Some(format!("{} conversation", self.agent_slug))),
            workspace: self.workspace.clone(),
            source_path: self.source_path.clone(),
            started_at: messages.first().and_then(|m| m.created_at),
            ended_at: messages.last().and_then(|m| m.created_at),
            metadata: json!({"fixture": true}),
            messages,
        }
    }

    /// Build a Conversation (storage-facing).
    pub fn build_conversation(self) -> Conversation {
        let messages: Vec<Message> = (0..self.message_count)
            .map(|i| {
                let role = if i % 2 == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                };
                let snippets: Vec<Snippet> = self
                    .snippets
                    .iter()
                    .filter(|s| s.msg_idx == i)
                    .map(|s| Snippet {
                        id: None,
                        file_path: s.file_path.clone(),
                        start_line: s.start_line,
                        end_line: s.end_line,
                        language: s.language.clone(),
                        snippet_text: s.text.clone(),
                    })
                    .collect();

                let content = self
                    .custom_content
                    .get(&i)
                    .cloned()
                    .unwrap_or_else(|| format!("{}-{}", self.content_prefix, i));

                Message {
                    id: None,
                    idx: i as i64,
                    role,
                    author: if i % 2 == 0 {
                        Some("user".into())
                    } else {
                        Some("agent".into())
                    },
                    created_at: Some(self.base_ts + i as i64),
                    content,
                    extra_json: json!({"seed": i}),
                    snippets,
                }
            })
            .collect();

        Conversation {
            id: None,
            agent_slug: self.agent_slug.clone(),
            workspace: self.workspace.clone(),
            external_id: self.external_id.clone(),
            title: self
                .title
                .or_else(|| Some(format!("{} conversation", self.agent_slug))),
            source_path: self.source_path.clone(),
            started_at: messages.first().and_then(|m| m.created_at),
            ended_at: messages.last().and_then(|m| m.created_at),
            approx_tokens: Some((self.message_count * 12) as i64),
            metadata_json: json!({"fixture": true}),
            messages,
            source_id: "local".to_string(),
            origin_host: None,
        }
    }
}

/// Helper to fluently assert `SearchHit` fields in tests.
pub struct SearchHitAssert<'a> {
    hit: &'a SearchHit,
}

#[allow(dead_code)]
pub fn assert_hit(hit: &SearchHit) -> SearchHitAssert<'_> {
    SearchHitAssert { hit }
}

#[allow(dead_code)]
impl SearchHitAssert<'_> {
    pub fn title(self, expected: impl AsRef<str>) -> Self {
        assert_eq!(
            self.hit.title,
            expected.as_ref(),
            "title mismatch for hit {:?}",
            self.hit.source_path
        );
        self
    }

    pub fn agent(self, expected: impl AsRef<str>) -> Self {
        assert_eq!(
            self.hit.agent,
            expected.as_ref(),
            "agent mismatch for hit {:?}",
            self.hit.source_path
        );
        self
    }

    pub fn workspace(self, expected: impl AsRef<str>) -> Self {
        assert_eq!(
            self.hit.workspace,
            expected.as_ref(),
            "workspace mismatch for hit {:?}",
            self.hit.source_path
        );
        self
    }

    pub fn snippet_contains(self, needle: impl AsRef<str>) -> Self {
        let needle = needle.as_ref();
        assert!(
            self.hit.snippet.contains(needle),
            "snippet missing `{}` in hit {:?}",
            needle,
            self.hit.source_path
        );
        self
    }

    pub fn content_contains(self, needle: impl AsRef<str>) -> Self {
        let needle = needle.as_ref();
        assert!(
            self.hit.content.contains(needle),
            "content missing `{}` in hit {:?}",
            needle,
            self.hit.source_path
        );
        self
    }

    pub fn line(self, expected: usize) -> Self {
        assert_eq!(
            self.hit.line_number,
            Some(expected),
            "line number mismatch for hit {:?}",
            self.hit.source_path
        );
        self
    }

    pub fn match_type(self, expected: MatchType) -> Self {
        assert_eq!(
            self.hit.match_type, expected,
            "match type mismatch for hit {:?}",
            self.hit.source_path
        );
        self
    }
}

// -------- Macros & connector presets --------

#[macro_export]
macro_rules! assert_logs_contain {
    ($tracing:expr, $needle:expr) => {{
        let out = $tracing.output();
        assert!(
            out.contains($needle),
            "expected logs to contain `{}` but were:\n{}",
            $needle,
            out
        );
    }};
}

#[macro_export]
macro_rules! assert_logs_not_contain {
    ($tracing:expr, $needle:expr) => {{
        let out = $tracing.output();
        assert!(
            !out.contains($needle),
            "expected logs NOT to contain `{}` but were:\n{}",
            $needle,
            out
        );
    }};
}

/// Typical fixture shapes for each connector. Paths mirror real connectors but live in /tmp.
#[allow(dead_code)]
pub fn fixture_codex() -> ConversationFixtureBuilder {
    ConversationFixtureBuilder::new("codex")
        .workspace("/tmp/workspaces/codex")
        .source_path("/tmp/.codex/sessions/rollout-1.jsonl")
        .external_id("rollout-1")
}

#[allow(dead_code)]
pub fn fixture_cline() -> ConversationFixtureBuilder {
    ConversationFixtureBuilder::new("cline")
        .workspace("/tmp/workspaces/cline")
        .source_path(
            "/tmp/.config/Code/User/globalStorage/saoudrizwan.claude-dev/task/ui_messages.json",
        )
        .external_id("cline-task-1")
}

#[allow(dead_code)]
pub fn fixture_claude_code() -> ConversationFixtureBuilder {
    ConversationFixtureBuilder::new("claude_code")
        .workspace("/tmp/.claude/projects/demo")
        .source_path("/tmp/.claude/projects/demo/session.jsonl")
        .external_id("claude-session-1")
}

#[allow(dead_code)]
pub fn fixture_gemini() -> ConversationFixtureBuilder {
    ConversationFixtureBuilder::new("gemini")
        .workspace("/tmp/.gemini/tmp/project-hash")
        .source_path("/tmp/.gemini/tmp/project-hash/chats/session-1.json")
        .external_id("session-1")
}

#[allow(dead_code)]
pub fn fixture_opencode() -> ConversationFixtureBuilder {
    ConversationFixtureBuilder::new("opencode")
        .workspace("/tmp/opencode/workspace")
        .source_path("/tmp/opencode/database.db")
        .external_id("db-session-1")
}

#[allow(dead_code)]
pub fn fixture_amp() -> ConversationFixtureBuilder {
    ConversationFixtureBuilder::new("amp")
        .workspace("/tmp/sourcegraph.amp/ws")
        .source_path("/tmp/sourcegraph.amp/cache/session.json")
        .external_id("amp-1")
}

/// Snippet specification for attaching code fragments to generated messages.
#[derive(Debug, Clone)]
pub struct SnippetSpec {
    pub msg_idx: usize,
    pub file_path: Option<PathBuf>,
    pub start_line: Option<i64>,
    pub end_line: Option<i64>,
    pub language: Option<String>,
    pub text: Option<String>,
}

impl SnippetSpec {
    pub fn new(msg_idx: usize) -> Self {
        Self {
            msg_idx,
            file_path: None,
            start_line: None,
            end_line: None,
            language: None,
            text: None,
        }
    }

    #[allow(dead_code)]
    pub fn file(mut self, path: impl Into<PathBuf>) -> Self {
        self.file_path = Some(path.into());
        self
    }

    pub fn lines(mut self, start: i64, end: i64) -> Self {
        self.start_line = Some(start);
        self.end_line = Some(end);
        self
    }

    pub fn language(mut self, lang: impl Into<String>) -> Self {
        self.language = Some(lang.into());
        self
    }

    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }
}
