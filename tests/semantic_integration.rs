//! Integration tests for semantic search flows.
//!
//! Tests cover:
//! - CLI models commands (status, verify, check-update)
//! - Search mode flags (lexical, semantic, hybrid)
//! - Determinism tests (same query yields consistent results)
//! - Robot output schema validation
//!
//! Part of bead: coding_agent_session_search-c8f8

use assert_cmd::cargo::cargo_bin_cmd;
use serde_json::Value;
use std::fs;

mod util;
use util::EnvGuard;

/// Helper to create Codex session with modern envelope format.
fn make_codex_session(
    root: &std::path::Path,
    date_path: &str,
    filename: &str,
    content: &str,
    ts: u64,
) {
    let sessions = root.join(format!("sessions/{date_path}"));
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join(filename);
    let sample = format!(
        r#"{{"type": "event_msg", "timestamp": {ts}, "payload": {{"type": "user_message", "message": "{content}"}}}}
{{"type": "response_item", "timestamp": {}, "payload": {{"role": "assistant", "content": "{content}_response"}}}}"#,
        ts + 1000
    );
    fs::write(file, sample).unwrap();
}

// =============================================================================
// CLI Models Command Tests
// =============================================================================

/// Test: cass models status returns valid output
#[test]
fn test_models_status_command() {
    let output = cargo_bin_cmd!("cass")
        .args(["models", "status"])
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("models status command");

    // Should succeed (exit 0) regardless of installation state
    assert!(
        output.status.success(),
        "models status should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should contain status-related output
    assert!(
        stdout.contains("Model") || stdout.contains("model") || stdout.contains("Status"),
        "Output should mention models or status. Got: {}",
        stdout
    );
}

/// Test: cass models status --json returns valid JSON
#[test]
fn test_models_status_json_output() {
    let output = cargo_bin_cmd!("cass")
        .args(["models", "status", "--json"])
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("models status --json command");

    assert!(
        output.status.success(),
        "models status --json should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value =
        serde_json::from_str(stdout.trim()).expect("models status --json should return valid JSON");

    // Verify expected fields exist (model_id not model_name)
    assert!(
        json.get("model_id").is_some(),
        "JSON should have model_id field. Got: {}",
        json
    );
    assert!(
        json.get("state").is_some(),
        "JSON should have state field. Got: {}",
        json
    );
}

/// Test: cass models verify returns valid output
#[test]
fn test_models_verify_command() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let output = cargo_bin_cmd!("cass")
        .args(["models", "verify", "--data-dir"])
        .arg(&data_dir)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("models verify command");

    // Should succeed (model not installed is still a valid result)
    assert!(
        output.status.success(),
        "models verify should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Test: cass models verify --json returns valid JSON
#[test]
fn test_models_verify_json_output() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let output = cargo_bin_cmd!("cass")
        .args(["models", "verify", "--json", "--data-dir"])
        .arg(&data_dir)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("models verify --json command");

    assert!(
        output.status.success(),
        "models verify --json should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value =
        serde_json::from_str(stdout.trim()).expect("models verify --json should return valid JSON");

    // Verify expected fields exist
    assert!(
        json.get("model_dir").is_some(),
        "JSON should have model_dir field. Got: {}",
        json
    );
    assert!(
        json.get("status").is_some(),
        "JSON should have status field. Got: {}",
        json
    );
}

/// Test: cass models check-update returns valid output
#[test]
fn test_models_check_update_command() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let output = cargo_bin_cmd!("cass")
        .args(["models", "check-update", "--data-dir"])
        .arg(&data_dir)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("models check-update command");

    // Should succeed regardless of installation state
    assert!(
        output.status.success(),
        "models check-update should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Test: cass models check-update --json returns valid JSON
#[test]
fn test_models_check_update_json_output() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let output = cargo_bin_cmd!("cass")
        .args(["models", "check-update", "--json", "--data-dir"])
        .arg(&data_dir)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("models check-update --json command");

    assert!(
        output.status.success(),
        "models check-update --json should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(stdout.trim())
        .expect("models check-update --json should return valid JSON");

    // Verify expected fields exist
    assert!(
        json.get("update_available").is_some(),
        "JSON should have update_available field. Got: {}",
        json
    );
    assert!(
        json.get("latest_revision").is_some(),
        "JSON should have latest_revision field. Got: {}",
        json
    );
}

/// Test: cass models help shows all subcommands
#[test]
fn test_models_help_shows_subcommands() {
    let output = cargo_bin_cmd!("cass")
        .args(["models", "--help"])
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("models --help command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should list all subcommands
    assert!(
        stdout.contains("status"),
        "Help should mention status subcommand"
    );
    assert!(
        stdout.contains("install"),
        "Help should mention install subcommand"
    );
    assert!(
        stdout.contains("verify"),
        "Help should mention verify subcommand"
    );
    assert!(
        stdout.contains("remove"),
        "Help should mention remove subcommand"
    );
    assert!(
        stdout.contains("check-update"),
        "Help should mention check-update subcommand"
    );
}

// =============================================================================
// Search Mode Flag Tests
// =============================================================================

/// Test: --mode lexical uses lexical search
#[test]
fn test_mode_flag_lexical() {
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
    let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

    // Create fixture
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-mode.jsonl",
        "lexical_mode_test_content",
        1732118400000,
    );

    // Index first
    cargo_bin_cmd!("cass")
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .assert()
        .success();

    // Search with --mode lexical
    let output = cargo_bin_cmd!("cass")
        .args([
            "search",
            "lexical_mode_test_content",
            "--mode",
            "lexical",
            "--robot",
            "--data-dir",
        ])
        .arg(&data_dir)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("search --mode lexical");

    assert!(
        output.status.success(),
        "Search with --mode lexical should succeed"
    );

    let json: Value = serde_json::from_slice(&output.stdout).expect("valid JSON");
    let hits = json.get("hits").and_then(|h| h.as_array());
    assert!(hits.is_some(), "Should have hits array");
}

/// Test: --mode semantic is accepted (may fail if model not installed)
#[test]
fn test_mode_flag_semantic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
    let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

    // Create fixture
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-semantic.jsonl",
        "semantic_mode_test_content",
        1732118400000,
    );

    // Index first
    cargo_bin_cmd!("cass")
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .assert()
        .success();

    // Search with --mode semantic
    let output = cargo_bin_cmd!("cass")
        .args([
            "search",
            "semantic_mode_test_content",
            "--mode",
            "semantic",
            "--robot",
            "--data-dir",
        ])
        .arg(&data_dir)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("search --mode semantic");

    // Either succeeds or fails with "semantic-unavailable" error (when model not installed)
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("semantic-unavailable")
                || stderr.contains("Semantic search not available"),
            "If semantic fails, should be due to unavailability. Got: {}",
            stderr
        );
    }
}

/// Test: --mode hybrid combines lexical and semantic (may fail if model not installed)
#[test]
fn test_mode_flag_hybrid() {
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
    let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

    // Create fixture
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-hybrid.jsonl",
        "hybrid_mode_test_content",
        1732118400000,
    );

    // Index first
    cargo_bin_cmd!("cass")
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .assert()
        .success();

    // Search with --mode hybrid
    let output = cargo_bin_cmd!("cass")
        .args([
            "search",
            "hybrid_mode_test_content",
            "--mode",
            "hybrid",
            "--robot",
            "--data-dir",
        ])
        .arg(&data_dir)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("search --mode hybrid");

    // Either succeeds or fails with "semantic-unavailable" error (when model not installed)
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("semantic-unavailable")
                || stderr.contains("Hybrid search not available"),
            "If hybrid fails, should be due to unavailability. Got: {}",
            stderr
        );
    }
}

// =============================================================================
// Determinism Tests
// =============================================================================

/// Test: Same query returns same results across multiple invocations
#[test]
fn test_same_query_same_results() {
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
    let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

    // Create multiple fixtures with deterministic content
    for i in 1..=3 {
        make_codex_session(
            &codex_home,
            "2024/11/20",
            &format!("rollout-det{i}.jsonl"),
            &format!("deterministic_test_content_{i}"),
            1732118400000 + (i as u64 * 1000),
        );
    }

    // Index
    cargo_bin_cmd!("cass")
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .assert()
        .success();

    // Run the same search query multiple times
    let mut results: Vec<String> = Vec::new();
    for _ in 0..3 {
        let output = cargo_bin_cmd!("cass")
            .args([
                "search",
                "deterministic_test_content",
                "--robot",
                "--data-dir",
            ])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
            .output()
            .expect("deterministic search");

        assert!(output.status.success());

        let json: Value = serde_json::from_slice(&output.stdout).expect("valid JSON");

        // Extract hit IDs or paths for comparison
        let empty_vec = vec![];
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .unwrap_or(&empty_vec);
        let hit_ids: Vec<String> = hits
            .iter()
            .filter_map(|h| h.get("source_path").and_then(|p| p.as_str()))
            .map(String::from)
            .collect();
        results.push(hit_ids.join(","));
    }

    // All results should be identical
    assert!(
        results.iter().all(|r| r == &results[0]),
        "Same query should return same results. Got: {:?}",
        results
    );
}

/// Test: Results are ordered deterministically (same order each time)
#[test]
fn test_result_ordering_deterministic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
    let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

    // Create fixtures with shared term
    for i in 1..=5 {
        make_codex_session(
            &codex_home,
            "2024/11/20",
            &format!("rollout-order{i}.jsonl"),
            &format!("ordering_test_shared_{i}"),
            1732118400000 + (i as u64 * 100000),
        );
    }

    // Index
    cargo_bin_cmd!("cass")
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .assert()
        .success();

    // Run search multiple times and compare ordering
    let mut orderings: Vec<Vec<String>> = Vec::new();
    for _ in 0..3 {
        let output = cargo_bin_cmd!("cass")
            .args(["search", "ordering_test_shared", "--robot", "--data-dir"])
            .arg(&data_dir)
            .env("HOME", home)
            .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
            .output()
            .expect("ordering search");

        assert!(output.status.success());

        let json: Value = serde_json::from_slice(&output.stdout).expect("valid JSON");
        let empty_vec = vec![];
        let hits = json
            .get("hits")
            .and_then(|h| h.as_array())
            .unwrap_or(&empty_vec);
        let order: Vec<String> = hits
            .iter()
            .filter_map(|h| h.get("source_path").and_then(|p| p.as_str()))
            .map(String::from)
            .collect();
        orderings.push(order);
    }

    // All orderings should be identical
    assert!(
        orderings.iter().all(|o| o == &orderings[0]),
        "Result ordering should be deterministic. Got: {:?}",
        orderings
    );
}

// =============================================================================
// Robot Output Schema Tests
// =============================================================================

/// Test: Robot JSON output includes all expected fields
#[test]
fn test_robot_output_schema() {
    let tmp = tempfile::TempDir::new().unwrap();
    let home = tmp.path();
    let codex_home = home.join(".codex");
    let data_dir = home.join("cass_data");
    fs::create_dir_all(&data_dir).unwrap();

    let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());
    let _guard_codex = EnvGuard::set("CODEX_HOME", codex_home.to_string_lossy());

    // Create fixture
    make_codex_session(
        &codex_home,
        "2024/11/20",
        "rollout-schema.jsonl",
        "schema_test_content",
        1732118400000,
    );

    // Index
    cargo_bin_cmd!("cass")
        .args(["index", "--full", "--data-dir"])
        .arg(&data_dir)
        .env("CODEX_HOME", &codex_home)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .assert()
        .success();

    // Search with --robot
    let output = cargo_bin_cmd!("cass")
        .args(["search", "schema_test_content", "--robot", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", home)
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("robot schema search");

    assert!(output.status.success());

    let json: Value = serde_json::from_slice(&output.stdout).expect("valid JSON");

    // Verify top-level schema fields
    assert!(json.get("hits").is_some(), "Should have hits field");
    assert!(
        json.get("total_matches").is_some(),
        "Should have total_matches field"
    );
    assert!(json.get("limit").is_some(), "Should have limit field");

    // Verify hit schema
    let hits = json
        .get("hits")
        .and_then(|h| h.as_array())
        .expect("hits array");
    if !hits.is_empty() {
        let hit = &hits[0];
        // Required fields in each hit
        assert!(
            hit.get("content").is_some(),
            "Hit should have content field"
        );
        assert!(hit.get("agent").is_some(), "Hit should have agent field");
        assert!(
            hit.get("source_path").is_some(),
            "Hit should have source_path field"
        );
        assert!(
            hit.get("match_type").is_some(),
            "Hit should have match_type field"
        );
    }
}

/// Test: introspect includes models command in schema
#[test]
fn test_introspect_includes_models_command() {
    let output = cargo_bin_cmd!("cass")
        .args(["introspect", "--json"])
        .env("CODING_AGENT_SEARCH_NO_UPDATE_PROMPT", "1")
        .output()
        .expect("introspect command");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(stdout.trim()).expect("valid introspect JSON");

    let commands = json
        .get("commands")
        .and_then(|c| c.as_array())
        .expect("commands array");

    // Find models command
    let models_cmd = commands
        .iter()
        .find(|c| c.get("name") == Some(&Value::String("models".into())));
    assert!(
        models_cmd.is_some(),
        "introspect should include models command"
    );

    // Verify models has description
    if let Some(models) = models_cmd {
        let description = models
            .get("description")
            .and_then(|d| d.as_str())
            .expect("models command should have description");
        assert!(
            description.contains("model") || description.contains("semantic"),
            "models description should mention models or semantic"
        );
    }
}
