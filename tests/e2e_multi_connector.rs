//! E2E tests for multi-connector scenarios.
//!
//! These tests verify that multiple connectors work together correctly:
//! - Multiple connectors can be indexed in a single run
//! - Search returns results from all indexed connectors
//! - Agent filtering correctly isolates connector results
//! - Results are properly attributed to their source connector

use assert_cmd::cargo::cargo_bin_cmd;
use std::fs;
use std::path::Path;
use std::time::Instant;

mod util;
use util::EnvGuard;
use util::e2e_log::{E2eError, E2eLogger, E2eTestInfo};

fn e2e_logging_enabled() -> bool {
    std::env::var("E2E_LOG").is_ok()
}

fn run_logged_test<F>(name: &str, suite: &str, file: &str, line: u32, test_fn: F)
where
    F: FnOnce() -> Result<(), Box<dyn std::error::Error>>,
{
    let logger = if e2e_logging_enabled() {
        E2eLogger::new("rust").ok()
    } else {
        None
    };

    let test_info = E2eTestInfo::new(name, suite, file, line);
    if let Some(ref lg) = logger {
        let _ = lg.test_start(&test_info);
    }

    let start = Instant::now();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test_fn));
    let duration_ms = start.elapsed().as_millis() as u64;

    let (is_pass, error_msg, panic_type) = match &result {
        Ok(Ok(())) => (true, None, None),
        Ok(Err(e)) => (false, Some(e.to_string()), None),
        Err(panic) => {
            let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_string()
            };
            (false, Some(msg), Some("Panic"))
        }
    };

    if let Some(ref lg) = logger {
        if is_pass {
            let _ = lg.test_pass(&test_info, duration_ms, None);
        } else {
            let _ = lg.test_fail(
                &test_info,
                duration_ms,
                None,
                E2eError {
                    message: error_msg.unwrap_or_default(),
                    error_type: panic_type.map(String::from),
                    stack: None,
                    context: None,
                },
            );
        }
        let _ = lg.flush();
    }

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

macro_rules! logged_test {
    ($name:expr, $suite:expr, $body:block) => {{
        run_logged_test($name, $suite, file!(), line!(), || {
            $body
            Ok(())
        })
    }};
}

fn make_codex_fixture(root: &Path) {
    let sessions = root.join("sessions/2025/11/21");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-1.jsonl");
    // Modern Codex JSONL format (envelope)
    let sample = r#"{"type": "event_msg", "timestamp": 1700000000000, "payload": {"type": "user_message", "message": "codex_user"}}
{"type": "response_item", "timestamp": 1700000001000, "payload": {"role": "assistant", "content": "codex_assistant"}}
"#;
    fs::write(file, sample).unwrap();
}

fn make_claude_fixture(root: &Path) {
    let project = root.join("projects/test-project");
    fs::create_dir_all(&project).unwrap();
    let file = project.join("session.jsonl");
    // Claude Code format
    let sample = r#"{"type": "user", "timestamp": "2023-11-21T10:00:00Z", "message": {"role": "user", "content": "claude_user"}}
{"type": "assistant", "timestamp": "2023-11-21T10:00:05Z", "message": {"role": "assistant", "content": "claude_assistant"}}
"#;
    fs::write(file, sample).unwrap();
}

fn make_gemini_fixture(root: &Path) {
    let project_hash = root.join("tmp/hash123/chats");
    fs::create_dir_all(&project_hash).unwrap();
    let file = project_hash.join("session-1.json"); // Must start with session-
    // Gemini CLI format
    let sample = r#"{
  "messages": [
    {"role": "user", "timestamp": 1700000000000, "content": "gemini_user"},
    {"role": "model", "timestamp": 1700000001000, "content": "gemini_assistant"}
  ]
}"#;
    fs::write(file, sample).unwrap();
}

fn make_cline_fixture(root: &Path) {
    let task_dir = root.join("Code/User/globalStorage/saoudrizwan.claude-dev/task_123");
    fs::create_dir_all(&task_dir).unwrap();

    let ui_messages = task_dir.join("ui_messages.json");
    let sample = r#"[
  {"role": "user", "ts": 1700000000000, "content": "cline_user"},
  {"role": "assistant", "ts": 1700000001000, "content": "cline_assistant"}
]"#;
    fs::write(ui_messages, sample).unwrap();

    let metadata = task_dir.join("task_metadata.json");
    fs::write(metadata, r#"{"id": "task_123", "title": "Cline Task"}"#).unwrap();
}

fn make_amp_fixture(root: &Path) {
    let amp_dir = root.join("amp/cache");
    fs::create_dir_all(&amp_dir).unwrap();
    let file = amp_dir.join("thread_abc.json");
    let sample = r#"{"messages": [
        {"role": "user", "created_at": 1700000000000, "content": "amp_user"},
        {"role": "assistant", "created_at": 1700000001000, "content": "amp_assistant"}
    ]}"#;
    fs::write(file, sample).unwrap();
}

#[test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "Linux-specific test (XDG_DATA_HOME paths)"
)]
fn multi_connector_pipeline() {
    logged_test!("multi_connector_pipeline", "e2e_multi_connector", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let xdg_data = home.join("xdg_data");
        let _config_home = home.join(".config"); // For Cline on Linux usually, but our fixture path was mostly hardcoded in the connector? 
        // ClineConnector uses:
        // dirs::home_dir().join(".config/Code/User/globalStorage/saoudrizwan.claude-dev")
        // So we just need HOME set correctly.

        fs::create_dir_all(&xdg_data).unwrap();

        // Override env vars
        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_xdg = EnvGuard::set("XDG_DATA_HOME", xdg_data.to_string_lossy());

        // Setup fixture roots
        let dot_codex = home.join(".codex");
        let dot_claude = home.join(".claude");
        let dot_gemini = home.join(".gemini");
        let dot_config = home.join(".config"); // for cline
        // Amp uses XDG_DATA_HOME/amp which is xdg_data/amp

        // Specific env overrides for connectors that support it
        let _guard_codex = EnvGuard::set("CODEX_HOME", dot_codex.to_string_lossy());
        let _guard_gemini = EnvGuard::set("GEMINI_HOME", dot_gemini.to_string_lossy());

        // Create fixtures
        make_codex_fixture(&dot_codex);
        make_claude_fixture(&dot_claude);
        make_gemini_fixture(&dot_gemini);
        make_cline_fixture(&dot_config); // Will be under .config/Code/... which matches Linux path relative to HOME
        make_amp_fixture(&xdg_data);

        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        // 1. INDEX
        cargo_bin_cmd!("cass")
            .arg("index")
            .arg("--full")
            .arg("--data-dir")
            .arg(&data_dir)
            .env("HOME", home.to_string_lossy().as_ref())
            .env("XDG_DATA_HOME", xdg_data.to_string_lossy().as_ref())
            .env("CODEX_HOME", dot_codex.to_string_lossy().as_ref())
            .env("GEMINI_HOME", dot_gemini.to_string_lossy().as_ref())
            .assert()
            .success();

        // 2. SEARCH (Robot mode)
        // Search for "user" - should find hits from all 5 agents
        let output = cargo_bin_cmd!("cass")
            .arg("search")
            .arg("user")
            .arg("--robot")
            .arg("--data-dir")
            .arg(&data_dir)
            .env("HOME", home.to_string_lossy().as_ref())
            .env("XDG_DATA_HOME", xdg_data.to_string_lossy().as_ref())
            .output()
            .expect("failed to execute search");

        assert!(output.status.success());
        let json_out: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("valid json");

        // Check results
        let hits = json_out
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        let found_agents: std::collections::HashSet<&str> = hits
            .iter()
            .filter_map(|h| h.get("agent").and_then(|s| s.as_str()))
            .collect();

        assert!(
            found_agents.contains("codex"),
            "Missing codex hit. Found: {found_agents:?}"
        );
        assert!(
            found_agents.contains("claude_code"),
            "Missing claude hit. Found: {found_agents:?}"
        );
        assert!(
            found_agents.contains("gemini"),
            "Missing gemini hit. Found: {found_agents:?}"
        );
        assert!(
            found_agents.contains("cline"),
            "Missing cline hit. Found: {found_agents:?}"
        );
        assert!(
            found_agents.contains("amp"),
            "Missing amp hit. Found: {found_agents:?}"
        );

        // 3. INCREMENTAL TEST
        // Ensure mtime is strictly greater than last scan
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Add a new file to Codex with CURRENT timestamp so message isn't filtered out
        let sessions = dot_codex.join("sessions/2025/11/22");
        fs::create_dir_all(&sessions).unwrap();

        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Use modern envelope format
        let content = format!(
            r#"{{"type": "event_msg", "timestamp": {now_ts}, "payload": {{"type": "user_message", "message": "codex_new"}}}}"#
        );
        fs::write(sessions.join("rollout-2.jsonl"), content).unwrap();

        // Index again (incremental) - must pass same env vars as full index
        cargo_bin_cmd!("cass")
            .arg("index")
            .arg("--data-dir")
            .arg(&data_dir)
            .env("HOME", home.to_string_lossy().as_ref())
            .env("XDG_DATA_HOME", xdg_data.to_string_lossy().as_ref())
            .env("CODEX_HOME", dot_codex.to_string_lossy().as_ref())
            .env("GEMINI_HOME", dot_gemini.to_string_lossy().as_ref())
            .assert()
            .success();

        // Search for "codex_new"
        let output_inc = cargo_bin_cmd!("cass")
            .arg("search")
            .arg("codex_new")
            .arg("--robot")
            .arg("--data-dir")
            .arg(&data_dir)
            .output()
            .expect("failed to execute search");

        let json_inc: serde_json::Value =
            serde_json::from_slice(&output_inc.stdout).expect("valid json");
        let hits_inc = json_inc
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");
        assert!(
            !hits_inc.is_empty(),
            "Incremental index failed to pick up new file"
        );
        assert_eq!(hits_inc[0]["content"], "codex_new");

        // 4. FILTER TEST
        // Filter by agent=claude_code
        let output_filter = cargo_bin_cmd!("cass")
            .arg("search")
            .arg("user")
            .arg("--agent")
            .arg("claude_code")
            .arg("--robot")
            .arg("--data-dir")
            .arg(&data_dir)
            .output()
            .expect("failed to execute search");

        let json_filter: serde_json::Value =
            serde_json::from_slice(&output_filter.stdout).expect("valid json");
        let hits_filter = json_filter
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        for hit in hits_filter {
            assert_eq!(hit["agent"], "claude_code");
        }
        assert!(!hits_filter.is_empty());
    });
}

// ============================================================================
// Cross-platform multi-connector tests (work on macOS and Linux)
// These tests use Codex and Claude Code which rely on HOME env var
// ============================================================================

/// Creates a Codex session with specific date and content.
fn make_codex_session(
    codex_home: &Path,
    date_path: &str,
    filename: &str,
    content: &str,
    ts_millis: u64,
) {
    let sessions = codex_home.join(format!("sessions/{date_path}"));
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join(filename);
    let sample = format!(
        r#"{{"type": "event_msg", "timestamp": {ts_millis}, "payload": {{"type": "user_message", "message": "{content}"}}}}
{{"type": "response_item", "timestamp": {}, "payload": {{"role": "assistant", "content": "{content}_response"}}}}"#,
        ts_millis + 1000
    );
    fs::write(file, sample).unwrap();
}

/// Creates a Claude Code session with specific content.
fn make_claude_session(
    claude_home: &Path,
    project_name: &str,
    filename: &str,
    content: &str,
    ts_iso: &str,
) {
    let project = claude_home.join(format!("projects/{project_name}"));
    fs::create_dir_all(&project).unwrap();
    let file = project.join(filename);
    let sample = format!(
        r#"{{"type": "user", "timestamp": "{ts_iso}", "message": {{"role": "user", "content": "{content}"}}}}
{{"type": "assistant", "timestamp": "{ts_iso}", "message": {{"role": "assistant", "content": "{content}_response"}}}}"#
    );
    fs::write(file, sample).unwrap();
}

/// Test: Multiple connectors can be indexed and searched together
#[test]
fn multi_connector_codex_and_claude() {
    logged_test!("multi_connector_codex_and_claude", "e2e_multi_connector", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let claude_home = home.join(".claude");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create sessions for both connectors with shared search term
        make_codex_session(
            &codex_home,
            "2024/11/20",
            "rollout-multi.jsonl",
            "multitest codex_unique_content",
            1732118400000,
        );
        make_claude_session(
            &claude_home,
            "multi-project",
            "session-multi.jsonl",
            "multitest claude_unique_content",
            "2024-11-20T10:00:00Z",
        );

        // Index all connectors
        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search for shared term - should find results from both connectors
        let output = cargo_bin_cmd!("cass")
            .args(["search", "multitest", "--robot", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        // Should have hits from both connectors
        let agents: std::collections::HashSet<_> =
            hits.iter().filter_map(|h| h["agent"].as_str()).collect();

        assert!(
            agents.contains("codex"),
            "Should find codex results. Agents found: {agents:?}"
        );
        assert!(
            agents.contains("claude_code"),
            "Should find claude_code results. Agents found: {agents:?}"
        );
        assert!(
            hits.len() >= 2,
            "Should have at least 2 hits from different connectors"
        );
    });
}

/// Test: Agent filter isolates results to specific connector
#[test]
fn multi_connector_agent_filter_isolation() {
    logged_test!(
        "multi_connector_agent_filter_isolation",
        "e2e_multi_connector",
        {
            let tmp = tempfile::TempDir::new().unwrap();
            let home = tmp.path();
            let codex_home = home.join(".codex");
            let claude_home = home.join(".claude");
            let data_dir = home.join("cass_data");
            fs::create_dir_all(&data_dir).unwrap();

            let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
            let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

            // Create sessions with shared search term
            make_codex_session(
                &codex_home,
                "2024/11/20",
                "rollout-iso.jsonl",
                "isolationtest codex_data",
                1732118400000,
            );
            make_claude_session(
                &claude_home,
                "iso-project",
                "session-iso.jsonl",
                "isolationtest claude_data",
                "2024-11-20T10:00:00Z",
            );

            cargo_bin_cmd!("cass")
                .args(["index", "--full", "--data-dir"])
                .arg(&data_dir)
                .env("CODEX_HOME", &codex_home)
                .env("HOME", home)
                .assert()
                .success();

            // Filter by codex only
            let codex_output = cargo_bin_cmd!("cass")
                .args([
                    "search",
                    "isolationtest",
                    "--agent",
                    "codex",
                    "--robot",
                    "--data-dir",
                ])
                .arg(&data_dir)
                .env("HOME", home)
                .output()
                .expect("search command");

            assert!(codex_output.status.success());
            let codex_json: serde_json::Value =
                serde_json::from_slice(&codex_output.stdout).expect("valid json");
            let codex_hits = codex_json
                .get("hits")
                .and_then(|h| h.as_array())
                .expect("hits array");

            assert!(!codex_hits.is_empty(), "Should find codex hits");
            for hit in codex_hits {
                assert_eq!(
                    hit["agent"], "codex",
                    "All hits should be from codex when filtering"
                );
            }

            // Filter by claude_code only
            let claude_output = cargo_bin_cmd!("cass")
                .args([
                    "search",
                    "isolationtest",
                    "--agent",
                    "claude_code",
                    "--robot",
                    "--data-dir",
                ])
                .arg(&data_dir)
                .env("HOME", home)
                .output()
                .expect("search command");

            assert!(claude_output.status.success());
            let claude_json: serde_json::Value =
                serde_json::from_slice(&claude_output.stdout).expect("valid json");
            let claude_hits = claude_json
                .get("hits")
                .and_then(|h| h.as_array())
                .expect("hits array");

            assert!(!claude_hits.is_empty(), "Should find claude_code hits");
            for hit in claude_hits {
                assert_eq!(
                    hit["agent"], "claude_code",
                    "All hits should be from claude_code when filtering"
                );
            }
        }
    );
}

/// Test: Each connector's unique content is properly indexed
#[test]
fn multi_connector_unique_content() {
    logged_test!("multi_connector_unique_content", "e2e_multi_connector", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let claude_home = home.join(".claude");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create sessions with unique content per connector
        make_codex_session(
            &codex_home,
            "2024/11/20",
            "rollout-unique.jsonl",
            "codexonly_xyzzy uniqueterm",
            1732118400000,
        );
        make_claude_session(
            &claude_home,
            "unique-project",
            "session-unique.jsonl",
            "claudeonly_plugh uniqueterm",
            "2024-11-20T10:00:00Z",
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search for codex-specific term
        let codex_output = cargo_bin_cmd!("cass")
            .args(["search", "codexonly_xyzzy", "--robot", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .output()
            .expect("search command");

        assert!(codex_output.status.success());
        let codex_json: serde_json::Value =
            serde_json::from_slice(&codex_output.stdout).expect("valid json");
        let codex_hits = codex_json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(!codex_hits.is_empty(), "Should find codex-specific content");
        assert!(
            codex_hits.iter().all(|h| h["agent"] == "codex"),
            "Codex-specific search should only return codex results"
        );

        // Search for claude-specific term
        let claude_output = cargo_bin_cmd!("cass")
            .args(["search", "claudeonly_plugh", "--robot", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .output()
            .expect("search command");

        assert!(claude_output.status.success());
        let claude_json: serde_json::Value =
            serde_json::from_slice(&claude_output.stdout).expect("valid json");
        let claude_hits = claude_json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            !claude_hits.is_empty(),
            "Should find claude-specific content"
        );
        assert!(
            claude_hits.iter().all(|h| h["agent"] == "claude_code"),
            "Claude-specific search should only return claude_code results"
        );
    });
}

/// Test: Aggregation by agent works with multiple connectors
#[test]
fn multi_connector_aggregation() {
    logged_test!("multi_connector_aggregation", "e2e_multi_connector", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let claude_home = home.join(".claude");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create multiple sessions per connector
        make_codex_session(
            &codex_home,
            "2024/11/20",
            "rollout-agg1.jsonl",
            "aggtest codex_first",
            1732118400000,
        );
        make_codex_session(
            &codex_home,
            "2024/11/21",
            "rollout-agg2.jsonl",
            "aggtest codex_second",
            1732204800000,
        );
        make_claude_session(
            &claude_home,
            "agg-project1",
            "session-agg1.jsonl",
            "aggtest claude_first",
            "2024-11-20T10:00:00Z",
        );
        make_claude_session(
            &claude_home,
            "agg-project2",
            "session-agg2.jsonl",
            "aggtest claude_second",
            "2024-11-21T10:00:00Z",
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with aggregation by agent
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "aggtest",
                "--aggregate",
                "agent",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");

        // Check aggregations exist
        let aggregations = json.get("aggregations").and_then(|a| a.as_object());
        assert!(
            aggregations.is_some(),
            "Should have aggregations in response"
        );

        let aggs = aggregations.unwrap();
        let agent_agg = aggs.get("agent").and_then(|a| a.as_object());
        assert!(agent_agg.is_some(), "Should have agent aggregation");

        // Aggregations use buckets format: { "buckets": [{"key": "codex", "count": N}, ...] }
        let buckets = agent_agg
            .unwrap()
            .get("buckets")
            .and_then(|b| b.as_array())
            .expect("Should have buckets array");

        let agent_keys: std::collections::HashSet<_> = buckets
            .iter()
            .filter_map(|b| b.get("key").and_then(|k| k.as_str()))
            .collect();

        assert!(
            agent_keys.contains("codex"),
            "Agent aggregation should include codex. Keys: {agent_keys:?}"
        );
        assert!(
            agent_keys.contains("claude_code"),
            "Agent aggregation should include claude_code. Keys: {agent_keys:?}"
        );
    });
}

/// Test: Incremental indexing works across multiple connectors
#[test]
fn multi_connector_incremental_index() {
    logged_test!(
        "multi_connector_incremental_index",
        "e2e_multi_connector",
        {
            let tmp = tempfile::TempDir::new().unwrap();
            let home = tmp.path();
            let codex_home = home.join(".codex");
            let claude_home = home.join(".claude");
            let data_dir = home.join("cass_data");
            fs::create_dir_all(&data_dir).unwrap();

            let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
            let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

            // Phase 1: Create initial sessions
            make_codex_session(
                &codex_home,
                "2024/11/20",
                "rollout-incr1.jsonl",
                "incrtest initial_codex",
                1732118400000,
            );
            make_claude_session(
                &claude_home,
                "incr-project1",
                "session-incr1.jsonl",
                "incrtest initial_claude",
                "2024-11-20T10:00:00Z",
            );

            // Full index
            cargo_bin_cmd!("cass")
                .args(["index", "--full", "--data-dir"])
                .arg(&data_dir)
                .env("CODEX_HOME", &codex_home)
                .env("HOME", home)
                .assert()
                .success();

            // Verify initial sessions indexed
            let output1 = cargo_bin_cmd!("cass")
                .args(["search", "incrtest", "--robot", "--data-dir"])
                .arg(&data_dir)
                .env("HOME", home)
                .output()
                .expect("search command");

            let json1: serde_json::Value =
                serde_json::from_slice(&output1.stdout).expect("valid json");
            let hits1 = json1
                .get("hits")
                .and_then(|h| h.as_array())
                .expect("hits array");
            assert!(hits1.len() >= 2, "Should have initial sessions indexed");

            // Phase 2: Add new sessions
            std::thread::sleep(std::time::Duration::from_secs(2)); // Ensure mtime difference

            // Use current timestamps so messages aren't filtered out
            let now_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let now_iso = chrono::Utc::now().to_rfc3339();

            make_codex_session(
                &codex_home,
                "2024/11/21",
                "rollout-incr2.jsonl",
                "incrtest new_codex",
                now_ts,
            );
            make_claude_session(
                &claude_home,
                "incr-project2",
                "session-incr2.jsonl",
                "incrtest new_claude",
                &now_iso,
            );

            // Incremental index (no --full flag)
            cargo_bin_cmd!("cass")
                .args(["index", "--data-dir"])
                .arg(&data_dir)
                .env("CODEX_HOME", &codex_home)
                .env("HOME", home)
                .assert()
                .success();

            // Verify all sessions now indexed
            let output2 = cargo_bin_cmd!("cass")
                .args(["search", "incrtest", "--robot", "--data-dir"])
                .arg(&data_dir)
                .env("HOME", home)
                .output()
                .expect("search command");

            let json2: serde_json::Value =
                serde_json::from_slice(&output2.stdout).expect("valid json");
            let hits2 = json2
                .get("hits")
                .and_then(|h| h.as_array())
                .expect("hits array");

            // Should have both old and new sessions
            assert!(
                hits2.len() > hits1.len(),
                "Incremental index should add new sessions. hits1={}, hits2={}",
                hits1.len(),
                hits2.len()
            );

            // Check specific content
            let has_initial = hits2
                .iter()
                .any(|h| h["content"].as_str().unwrap_or("").contains("initial"));
            let has_new = hits2
                .iter()
                .any(|h| h["content"].as_str().unwrap_or("").contains("new"));

            assert!(
                has_initial,
                "Should still have initial sessions after incremental index"
            );
            assert!(has_new, "Should have new sessions after incremental index");
        }
    );
}

/// Test: Multiple agent filter works correctly
#[test]
fn multi_connector_multiple_agent_filter() {
    logged_test!(
        "multi_connector_multiple_agent_filter",
        "e2e_multi_connector",
        {
            let tmp = tempfile::TempDir::new().unwrap();
            let home = tmp.path();
            let codex_home = home.join(".codex");
            let claude_home = home.join(".claude");
            let data_dir = home.join("cass_data");
            fs::create_dir_all(&data_dir).unwrap();

            let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
            let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

            make_codex_session(
                &codex_home,
                "2024/11/20",
                "rollout-maf.jsonl",
                "multiagent codex_content",
                1732118400000,
            );
            make_claude_session(
                &claude_home,
                "multi-agent-project",
                "session-maf.jsonl",
                "multiagent claude_content",
                "2024-11-20T10:00:00Z",
            );

            cargo_bin_cmd!("cass")
                .args(["index", "--full", "--data-dir"])
                .arg(&data_dir)
                .env("CODEX_HOME", &codex_home)
                .env("HOME", home)
                .assert()
                .success();

            // Filter by multiple agents (both codex and claude_code)
            let output = cargo_bin_cmd!("cass")
                .args([
                    "search",
                    "multiagent",
                    "--agent",
                    "codex",
                    "--agent",
                    "claude_code",
                    "--robot",
                    "--data-dir",
                ])
                .arg(&data_dir)
                .env("HOME", home)
                .output()
                .expect("search command");

            assert!(output.status.success());
            let json: serde_json::Value =
                serde_json::from_slice(&output.stdout).expect("valid json");
            let hits = json
                .get("hits")
                .and_then(|h| h.as_array())
                .expect("hits array");

            // Should have hits from both specified agents
            let agents: std::collections::HashSet<_> =
                hits.iter().filter_map(|h| h["agent"].as_str()).collect();

            assert!(
                agents.contains("codex") && agents.contains("claude_code"),
                "Should find results from both specified agents. Found: {agents:?}"
            );
        }
    );
}

/// Test: Empty connector doesn't break indexing of other connectors
#[test]
fn multi_connector_empty_connector() {
    logged_test!("multi_connector_empty_connector", "e2e_multi_connector", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");

        // Only create data_dir and codex_home, leave claude_home empty/nonexistent
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create only codex session
        make_codex_session(
            &codex_home,
            "2024/11/20",
            "rollout-only.jsonl",
            "singleconnector codex_only",
            1732118400000,
        );

        // Index should succeed even with non-existent claude_home
        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search should work and return codex results
        let output = cargo_bin_cmd!("cass")
            .args(["search", "singleconnector", "--robot", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(!hits.is_empty(), "Should find codex results");
        assert!(
            hits.iter().all(|h| h["agent"] == "codex"),
            "All results should be from codex"
        );
    });
}
