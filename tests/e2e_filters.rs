//! E2E tests for filter combinations.
//!
//! Tests all filter combinations work correctly end-to-end:
//! - Agent filter (--agent)
//! - Time filters (--since, --until, --days, --today, --week)
//! - Workspace filter (--workspace)
//! - Combined filters

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

/// Creates a Codex session with specific date and content.
/// Timestamp should be in milliseconds.
fn make_codex_session_at(
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

/// Creates a Claude Code session with specific date and content.
fn make_claude_session_at(claude_home: &Path, project_name: &str, content: &str, ts_iso: &str) {
    let project = claude_home.join(format!("projects/{project_name}"));
    fs::create_dir_all(&project).unwrap();
    let file = project.join("session.jsonl");
    let sample = format!(
        r#"{{"type": "user", "timestamp": "{ts_iso}", "message": {{"role": "user", "content": "{content}"}}}}
{{"type": "assistant", "timestamp": "{ts_iso}", "message": {{"role": "assistant", "content": "{content}_response"}}}}"#
    );
    fs::write(file, sample).unwrap();
}

/// Test: Agent filter correctly limits results to specified connector
#[test]
fn filter_by_agent_codex() {
    logged_test!("filter_by_agent_codex", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let claude_home = home.join(".claude");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create sessions for both connectors with identifiable content
        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "codex_specific agenttest",
            1732118400000,
        );
        make_claude_session_at(
            &claude_home,
            "test-project",
            "claude_specific agenttest",
            "2024-11-20T10:00:00Z",
        );

        // Index both
        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with agent filter for codex only
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "agenttest",
                "--agent",
                "codex",
                "--robot",
                "--data-dir",
            ])
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

        // All hits should be from codex
        for hit in hits {
            assert_eq!(
                hit["agent"], "codex",
                "Expected only codex hits when filtering by agent=codex"
            );
        }
        assert!(!hits.is_empty(), "Should find at least one codex hit");
    });
}

/// Test: Time filter --since correctly limits results
#[test]
fn filter_by_time_since() {
    logged_test!("filter_by_time_since", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Nov 15, 2024 10:00 UTC = 1731682800000
        // Nov 25, 2024 10:00 UTC = 1732546800000
        make_codex_session_at(
            &codex_home,
            "2024/11/15",
            "rollout-old.jsonl",
            "oldsession sincetest",
            1731682800000,
        );
        make_codex_session_at(
            &codex_home,
            "2024/11/25",
            "rollout-new.jsonl",
            "newsession sincetest",
            1732546800000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with --since Nov 20, 2024 - should only find Nov 25 session
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "sincetest",
                "--since",
                "2024-11-20",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            !hits.is_empty(),
            "Should find at least one hit with since filter"
        );
        for hit in hits {
            let content = hit["content"].as_str().unwrap_or("");
            assert!(
                content.contains("newsession"),
                "Should only find new session since 2024-11-20, got: {}",
                content
            );
        }
    });
}

/// Test: Time filter --until correctly limits results
#[test]
fn filter_by_time_until() {
    logged_test!("filter_by_time_until", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Nov 15, 2024 10:00 UTC = 1731682800000
        // Nov 25, 2024 10:00 UTC = 1732546800000
        make_codex_session_at(
            &codex_home,
            "2024/11/15",
            "rollout-old.jsonl",
            "oldsession untiltest",
            1731682800000,
        );
        make_codex_session_at(
            &codex_home,
            "2024/11/25",
            "rollout-new.jsonl",
            "newsession untiltest",
            1732546800000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with --until Nov 20, 2024 - should only find Nov 15 session
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "untiltest",
                "--until",
                "2024-11-20",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            !hits.is_empty(),
            "Should find at least one hit with until filter"
        );
        for hit in hits {
            let content = hit["content"].as_str().unwrap_or("");
            assert!(
                content.contains("oldsession"),
                "Should only find old session until 2024-11-20, got: {}",
                content
            );
        }
    });
}

/// Test: Combined time filters (--since AND --until) for date range
#[test]
fn filter_by_time_range() {
    logged_test!("filter_by_time_range", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Nov 10, 2024 = 1731250800000
        // Nov 20, 2024 = 1732114800000
        // Nov 30, 2024 = 1732978800000
        make_codex_session_at(
            &codex_home,
            "2024/11/10",
            "rollout-early.jsonl",
            "earlysession rangetest",
            1731250800000,
        );
        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-middle.jsonl",
            "middlesession rangetest",
            1732114800000,
        );
        make_codex_session_at(
            &codex_home,
            "2024/11/30",
            "rollout-late.jsonl",
            "latesession rangetest",
            1732978800000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with date range Nov 15 to Nov 25 - should only find Nov 20 session
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "rangetest",
                "--since",
                "2024-11-15",
                "--until",
                "2024-11-25",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            !hits.is_empty(),
            "Should find at least one hit in date range"
        );
        for hit in hits {
            let content = hit["content"].as_str().unwrap_or("");
            assert!(
                content.contains("middlesession"),
                "Should only find middle session in range, got: {}",
                content
            );
        }
    });
}

/// Test: Combined agent + time filter
#[test]
fn filter_combined_agent_and_time() {
    logged_test!("filter_combined_agent_and_time", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let claude_home = home.join(".claude");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create codex sessions (old and new)
        make_codex_session_at(
            &codex_home,
            "2024/11/15",
            "rollout-old.jsonl",
            "codex_combined_old combinedtest",
            1731682800000,
        );
        make_codex_session_at(
            &codex_home,
            "2024/11/25",
            "rollout-new.jsonl",
            "codex_combined_new combinedtest",
            1732546800000,
        );

        // Create claude sessions (old and new)
        make_claude_session_at(
            &claude_home,
            "project-old",
            "claude_combined_old combinedtest",
            "2024-11-15T10:00:00Z",
        );
        make_claude_session_at(
            &claude_home,
            "project-new",
            "claude_combined_new combinedtest",
            "2024-11-25T10:00:00Z",
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with agent=codex AND since=Nov 20
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "combinedtest",
                "--agent",
                "codex",
                "--since",
                "2024-11-20",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            !hits.is_empty(),
            "Should find at least one hit with combined filters"
        );
        for hit in hits {
            assert_eq!(hit["agent"], "codex", "Should only find codex hits");
            let content = hit["content"].as_str().unwrap_or("");
            assert!(
                content.contains("codex_combined_new"),
                "Should only find new codex session, got: {}",
                content
            );
        }
    });
}

/// Test: Empty result set when filters exclude everything
#[test]
fn filter_no_matches() {
    logged_test!("filter_no_matches", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create session in November 2024
        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "november nomatchtest",
            1732114800000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with impossible date filter (until October 2024, but content is November 2024)
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "nomatchtest",
                "--until",
                "2024-10-01",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            hits.is_empty(),
            "Should find no hits when filter excludes all results"
        );
    });
}

/// Test: Workspace filter using --workspace flag
#[test]
fn filter_by_workspace() {
    logged_test!("filter_by_workspace", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let claude_home = home.join(".claude");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());

        // Create Claude sessions with different workspaces (using cwd field)
        let workspace_alpha = "/projects/workspace-alpha";
        let workspace_beta = "/projects/workspace-beta";

        let project_a = claude_home.join("projects/project-a");
        fs::create_dir_all(&project_a).unwrap();
        let sample_a = format!(
            r#"{{"type": "user", "timestamp": "2024-11-20T10:00:00Z", "cwd": "{workspace_alpha}", "message": {{"role": "user", "content": "workspace_alpha workspacetest"}}}}
{{"type": "assistant", "timestamp": "2024-11-20T10:00:05Z", "cwd": "{workspace_alpha}", "message": {{"role": "assistant", "content": "workspace_alpha_response workspacetest"}}}}"#
        );
        // Use unique filename to avoid external_id collision in storage
        fs::write(project_a.join("session-alpha.jsonl"), sample_a).unwrap();

        // Add small delay to ensure different mtime
        std::thread::sleep(std::time::Duration::from_millis(100));

        let project_b = claude_home.join("projects/project-b");
        fs::create_dir_all(&project_b).unwrap();
        let sample_b = format!(
            r#"{{"type": "user", "timestamp": "2024-11-20T11:00:00Z", "cwd": "{workspace_beta}", "message": {{"role": "user", "content": "workspace_beta workspacetest"}}}}
{{"type": "assistant", "timestamp": "2024-11-20T11:00:05Z", "cwd": "{workspace_beta}", "message": {{"role": "assistant", "content": "workspace_beta_response workspacetest"}}}}"#
        );
        // Use unique filename to avoid external_id collision in storage
        fs::write(project_b.join("session-beta.jsonl"), sample_b).unwrap();

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .assert()
            .success();

        // Search with workspace filter for workspace-alpha (exact path match)
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "workspacetest",
                "--workspace",
                workspace_alpha,
                "--robot",
                "--data-dir",
            ])
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

        assert!(
            !hits.is_empty(),
            "Should find at least one hit with workspace filter"
        );
        for hit in hits {
            let ws = hit["workspace"].as_str().unwrap_or("");
            assert_eq!(
                ws, workspace_alpha,
                "Should only find content from workspace-alpha, got workspace: {}",
                ws
            );
        }
    });
}

/// Test: Days filter (--days N)
#[test]
fn filter_by_days() {
    logged_test!("filter_by_days", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create a session with a recent timestamp (today)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Create recent session (now) and old session (30 days ago)
        let thirty_days_ago = now - (30 * 24 * 60 * 60 * 1000);

        make_codex_session_at(
            &codex_home,
            "2024/12/01",
            "rollout-recent.jsonl",
            "recentsession daystest",
            now,
        );
        make_codex_session_at(
            &codex_home,
            "2024/11/01",
            "rollout-old.jsonl",
            "oldsession daystest",
            thirty_days_ago,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with --days 7 - should only find recent session
        let output = cargo_bin_cmd!("cass")
            .args(["search", "daystest", "--days", "7", "--robot", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            !hits.is_empty(),
            "Should find at least one hit with days filter"
        );
        for hit in hits {
            let content = hit["content"].as_str().unwrap_or("");
            assert!(
                content.contains("recentsession"),
                "Should only find recent session with --days 7, got: {}",
                content
            );
        }
    });
}

// =============================================================================
// Source filter tests (--source flag)
// =============================================================================

/// Test: search --source local filters to local sources only
#[test]
fn filter_by_source_local() {
    logged_test!("filter_by_source_local", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create local codex session
        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "localsession sourcetest",
            1732118400000,
        );

        // Index
        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with --source local
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "sourcetest",
                "--source",
                "local",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            !hits.is_empty(),
            "Should find local sessions with --source local"
        );

        // Verify source_id is local for all hits
        for hit in hits {
            let source = hit
                .get("source_id")
                .and_then(|s| s.as_str())
                .unwrap_or("local");
            assert_eq!(
                source, "local",
                "All hits should be from local source, got: {}",
                source
            );
        }
    });
}

/// Test: search --source with specific source name filters correctly
#[test]
fn filter_by_source_specific_name() {
    logged_test!("filter_by_source_specific_name", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create local codex session
        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "searchdata specifictest",
            1732118400000,
        );

        // Index first to create the database
        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with --source local (specific source name)
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "specifictest",
                "--source",
                "local",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            !hits.is_empty(),
            "Should find sessions when filtering by specific source name 'local'"
        );
    });
}

/// Test: search --source with nonexistent source returns empty results
#[test]
fn filter_by_source_nonexistent() {
    logged_test!("filter_by_source_nonexistent", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create local session
        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "somedata nonexistentsourcetest",
            1732118400000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with --source pointing to a nonexistent source
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "nonexistentsourcetest",
                "--source",
                "nonexistent-laptop",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            hits.is_empty(),
            "Should find no hits when filtering by nonexistent source"
        );
    });
}

/// Test: search --source remote returns empty when no remote sources exist
#[test]
fn filter_by_source_remote_empty() {
    logged_test!("filter_by_source_remote_empty", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create local session only
        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "localonly remotefiltertest",
            1732118400000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with --source remote should find nothing (only local exists)
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "remotefiltertest",
                "--source",
                "remote",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(
            hits.is_empty(),
            "Should find no remote hits when only local sessions exist"
        );
    });
}

/// Test: search --source all returns all sources (explicit)
#[test]
fn filter_by_source_all_explicit() {
    logged_test!("filter_by_source_all_explicit", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "allsources allsourcetest",
            1732118400000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Search with --source all (explicit)
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "allsourcetest",
                "--source",
                "all",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("search command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .expect("hits array");

        assert!(!hits.is_empty(), "Should find sessions with --source all");
    });
}

/// Test: search --source remote returns empty when no remote data indexed
/// Note: Remote source indexing via build_scan_roots is not fully integrated yet.
/// This test verifies that --source remote filter correctly returns empty results
/// when only local sessions exist (correct behavior - no false positives from local data).
#[test]
fn filter_by_source_remote_returns_empty_without_remote_indexing() {
    logged_test!(
        "filter_by_source_remote_returns_empty_without_remote_indexing",
        "e2e_filters",
        {
            let tmp = tempfile::TempDir::new().unwrap();
            let home = tmp.path();
            let codex_home = home.join(".codex");
            let data_dir = home.join("cass_data");
            fs::create_dir_all(&data_dir).unwrap();

            let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
            let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

            // Create local session with searchable content
            make_codex_session_at(
                &codex_home,
                "2024/11/20",
                "rollout-local.jsonl",
                "searchabledata remotefiltertest",
                1732118400000,
            );

            cargo_bin_cmd!("cass")
                .args(["index", "--full", "--data-dir"])
                .arg(&data_dir)
                .env("CODEX_HOME", &codex_home)
                .env("HOME", home)
                .assert()
                .success();

            // Search with --source remote should return empty (no remote data indexed)
            let output = cargo_bin_cmd!("cass")
                .args([
                    "search",
                    "remotefiltertest",
                    "--source",
                    "remote",
                    "--robot",
                    "--data-dir",
                ])
                .arg(&data_dir)
                .env("HOME", home)
                .env("CODEX_HOME", &codex_home)
                .output()
                .expect("search command");

            assert!(output.status.success());
            let json: serde_json::Value =
                serde_json::from_slice(&output.stdout).expect("valid json");
            let hits = json
                .get("hits")
                .and_then(|h| h.as_array())
                .expect("hits array");

            // --source remote should return empty because:
            // 1. No remote data is indexed (build_scan_roots not called in run_index)
            // 2. SQLite fallback is skipped when source filter is applied
            // This verifies the filter is working correctly (not returning local data)
            assert!(
                hits.is_empty(),
                "Remote filter should return empty when no remote data indexed"
            );
        }
    );
}

/// Test: search --source with specific source name returns empty for nonexistent sources
/// Note: This test verifies that filtering by a specific source name that has no indexed
/// data correctly returns empty results, demonstrating the filter is working.
#[test]
fn filter_by_source_specific_unindexed_source() {
    logged_test!(
        "filter_by_source_specific_unindexed_source",
        "e2e_filters",
        {
            let tmp = tempfile::TempDir::new().unwrap();
            let home = tmp.path();
            let codex_home = home.join(".codex");
            let data_dir = home.join("cass_data");
            fs::create_dir_all(&data_dir).unwrap();

            let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
            let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

            // Create local session with searchable content
            make_codex_session_at(
                &codex_home,
                "2024/11/20",
                "rollout-local.jsonl",
                "searchabledata specificsourcetest",
                1732118400000,
            );

            cargo_bin_cmd!("cass")
                .args(["index", "--full", "--data-dir"])
                .arg(&data_dir)
                .env("CODEX_HOME", &codex_home)
                .env("HOME", home)
                .assert()
                .success();

            // Search with --source work-laptop (source that doesn't exist in index)
            let output = cargo_bin_cmd!("cass")
                .args([
                    "search",
                    "specificsourcetest",
                    "--source",
                    "work-laptop",
                    "--robot",
                    "--data-dir",
                ])
                .arg(&data_dir)
                .env("HOME", home)
                .env("CODEX_HOME", &codex_home)
                .output()
                .expect("search command");

            assert!(output.status.success());
            let json: serde_json::Value =
                serde_json::from_slice(&output.stdout).expect("valid json");
            let hits = json
                .get("hits")
                .and_then(|h| h.as_array())
                .expect("hits array");

            // Should return empty because work-laptop source has no indexed data
            assert!(
                hits.is_empty(),
                "Filtering by unindexed source should return empty results"
            );
        }
    );
}

// =============================================================================
// Timeline source filter tests
// =============================================================================

/// Test: timeline --source local shows only local sessions
#[test]
fn timeline_source_local() {
    logged_test!("timeline_source_local", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "timelinelocal sessiondata",
            1732118400000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        let output = cargo_bin_cmd!("cass")
            .args(["timeline", "--source", "local", "--json", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("timeline command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");

        // Should have timeline data
        assert!(
            json.get("groups").is_some() || json.get("total_sessions").is_some(),
            "Timeline should return valid data structure"
        );
    });
}

/// Test: timeline --source remote with no remote data
#[test]
fn timeline_source_remote_empty() {
    logged_test!("timeline_source_remote_empty", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "timelineremote sessiondata",
            1732118400000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        let output = cargo_bin_cmd!("cass")
            .args(["timeline", "--source", "remote", "--json", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("timeline command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");

        // With only local data, remote filter should return 0 sessions
        let total = json
            .get("total_sessions")
            .and_then(|t| t.as_i64())
            .unwrap_or(0);
        assert_eq!(
            total, 0,
            "Timeline with --source remote should show 0 sessions when no remote data"
        );
    });
}

/// Test: timeline --source specific-name
#[test]
fn timeline_source_specific() {
    logged_test!("timeline_source_specific", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "timelinespecific data",
            1732118400000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Query with specific source name
        let output = cargo_bin_cmd!("cass")
            .args(["timeline", "--source", "local", "--json", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("timeline command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");

        // Should have valid timeline structure with source filter applied
        // Note: timeline may return 0 sessions if outside default date range, but
        // structure should still be valid and source filter accepted
        assert!(
            json.get("groups").is_some() || json.get("total_sessions").is_some(),
            "Timeline with --source local should return valid structure"
        );
    });
}

// =============================================================================
// Stats source filter tests
// =============================================================================

/// Test: stats --source local filters stats to local
#[test]
fn stats_source_local() {
    logged_test!("stats_source_local", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "statslocal data",
            1732118400000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        let output = cargo_bin_cmd!("cass")
            .args(["stats", "--source", "local", "--json", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("stats command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");

        // Should have conversation count
        let count = json
            .get("conversations")
            .and_then(|c| c.as_i64())
            .unwrap_or(0);
        assert!(
            count > 0,
            "Stats with --source local should show local conversations"
        );

        // Check source_filter is reported in output
        let filter = json
            .get("source_filter")
            .and_then(|f| f.as_str())
            .unwrap_or("");
        assert_eq!(filter, "local", "source_filter should be 'local' in output");
    });
}

/// Test: stats --source remote shows 0 when no remote data
#[test]
fn stats_source_remote_empty() {
    logged_test!("stats_source_remote_empty", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "statsremote data",
            1732118400000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        let output = cargo_bin_cmd!("cass")
            .args(["stats", "--source", "remote", "--json", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("stats command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");

        let count = json
            .get("conversations")
            .and_then(|c| c.as_i64())
            .unwrap_or(0);
        assert_eq!(
            count, 0,
            "Stats with --source remote should show 0 when no remote data"
        );
    });
}

/// Test: stats --by-source groups by source
#[test]
fn stats_by_source_grouping() {
    logged_test!("stats_by_source_grouping", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "bysource data",
            1732118400000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        let output = cargo_bin_cmd!("cass")
            .args(["stats", "--by-source", "--json", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("stats command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");

        // Should have by_source breakdown
        let by_source = json.get("by_source");
        assert!(
            by_source.is_some(),
            "Stats --by-source should include 'by_source' field in JSON"
        );

        // Should have at least local source
        if let Some(sources) = by_source.and_then(|s| s.as_array()) {
            assert!(
                !sources.is_empty(),
                "by_source should have at least one entry"
            );
            // First entry should be local
            let first_source = sources[0]
                .get("source_id")
                .and_then(|s| s.as_str())
                .unwrap_or("");
            assert_eq!(first_source, "local", "First source should be 'local'");
        }
    });
}

/// Test: stats --by-source with source filter combination
/// Note: Remote source indexing is not fully integrated yet (build_scan_roots not used in run_index),
/// so this test only verifies the --by-source flag works with local sources.
#[test]
fn stats_by_source_with_filter() {
    logged_test!("stats_by_source_with_filter", "e2e_filters", {
        let tmp = tempfile::TempDir::new().unwrap();
        let home = tmp.path();
        let codex_home = home.join(".codex");
        let data_dir = home.join("cass_data");
        fs::create_dir_all(&data_dir).unwrap();

        let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
        let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

        // Create local sessions
        make_codex_session_at(
            &codex_home,
            "2024/11/20",
            "rollout-1.jsonl",
            "statsbyfilter data1",
            1732118400000,
        );
        make_codex_session_at(
            &codex_home,
            "2024/11/21",
            "rollout-2.jsonl",
            "statsbyfilter data2",
            1732204800000,
        );

        cargo_bin_cmd!("cass")
            .args(["index", "--full", "--data-dir"])
            .arg(&data_dir)
            .env("CODEX_HOME", &codex_home)
            .env("HOME", home)
            .assert()
            .success();

        // Combine --by-source with --source local filter
        let output = cargo_bin_cmd!("cass")
            .args([
                "stats",
                "--by-source",
                "--source",
                "local",
                "--json",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODEX_HOME", &codex_home)
            .output()
            .expect("stats command");

        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid json");

        // Should have by_source breakdown
        let by_source = json.get("by_source").and_then(|s| s.as_array());
        assert!(by_source.is_some(), "Stats should include by_source array");

        // Should have local source with multiple conversations
        if let Some(sources) = by_source {
            let local_source = sources
                .iter()
                .find(|s| s.get("source_id").and_then(|id| id.as_str()) == Some("local"));
            assert!(local_source.is_some(), "Should have local source entry");

            if let Some(local) = local_source {
                let count = local
                    .get("conversations")
                    .and_then(|c| c.as_i64())
                    .unwrap_or(0);
                assert!(
                    count >= 2,
                    "Local source should have at least 2 conversations, got {}",
                    count
                );
            }
        }
    });
}
