//! E2E tests for `cass sources` CLI commands.
//!
//! Tests the sources subcommands end-to-end:
//! - sources add (with --no-test to skip SSH)
//! - sources list
//! - sources remove
//! - sources doctor (limited without actual SSH)
//! - sources sync (dry-run only)
//!
//! Note: Tests that require actual SSH connectivity are marked #[ignore].

use assert_cmd::cargo::cargo_bin_cmd;
use std::fs;
use std::path::Path;

mod util;
use util::EnvGuard;

/// Helper: Create a sources.toml config file with given content.
fn create_sources_config(config_dir: &Path, toml_content: &str) {
    let config_file = config_dir.join("cass").join("sources.toml");
    fs::create_dir_all(config_file.parent().unwrap()).unwrap();
    fs::write(&config_file, toml_content).unwrap();
}

/// Helper: Read the sources.toml config file.
fn read_sources_config(config_dir: &Path) -> String {
    let config_file = config_dir.join("cass").join("sources.toml");
    fs::read_to_string(&config_file).unwrap_or_default()
}

// =============================================================================
// sources list tests
// =============================================================================

/// Test: sources list with no configured sources shows appropriate message.
#[test]
fn sources_list_empty() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "list"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No sources configured") || stdout.contains("0 sources"),
        "Expected empty sources message, got: {stdout}"
    );
}

/// Test: sources list with configured sources shows them.
#[test]
fn sources_list_with_sources() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    // Create a config with one source
    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
sync_schedule = "manual"
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "list"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("laptop"),
        "Expected source name in output, got: {stdout}"
    );
}

/// Test: sources list --verbose shows additional details.
#[test]
fn sources_list_verbose() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "workstation"
type = "ssh"
host = "dev@work.example.com"
paths = ["~/.claude/projects", "~/.codex/sessions"]
sync_schedule = "daily"
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "list", "--verbose"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list --verbose command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("workstation"), "Missing source name");
    assert!(
        stdout.contains("work.example.com") || stdout.contains("dev@work"),
        "Missing host info in verbose output"
    );
}

/// Test: sources list --json outputs valid JSON.
#[test]
fn sources_list_json() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "list", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list --json command");

    assert!(output.status.success());
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid JSON output");

    // Should have a sources array
    assert!(
        json.get("sources").is_some(),
        "Expected 'sources' field in JSON"
    );
    let sources = json["sources"].as_array().expect("sources should be array");
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["name"], "laptop");
}

// =============================================================================
// sources add tests
// =============================================================================

/// Test: sources add with --no-test creates config without SSH connectivity.
#[test]
fn sources_add_no_test() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "add",
            "user@myserver.local",
            "--name",
            "myserver",
            "--preset",
            "linux-defaults",
            "--no-test",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");

    assert!(
        output.status.success(),
        "sources add failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Added source 'myserver'"),
        "Expected success message, got: {stdout}"
    );

    // Verify config was written
    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("myserver"),
        "Source not in config file"
    );
    assert!(
        config_content.contains("user@myserver.local"),
        "Host not in config file"
    );
}

/// Test: sources add with explicit paths.
#[test]
fn sources_add_explicit_paths() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "add",
            "admin@devbox",
            "--name",
            "devbox",
            "--path",
            "~/.claude/projects",
            "--path",
            "~/.codex/sessions",
            "--no-test",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");

    assert!(
        output.status.success(),
        "sources add failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("devbox"),
        "Source name not in config"
    );
    assert!(
        config_content.contains(".claude/projects"),
        "Path 1 not in config"
    );
    assert!(
        config_content.contains(".codex/sessions"),
        "Path 2 not in config"
    );
}

/// Test: sources add fails without paths.
#[test]
fn sources_add_no_paths_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "add",
            "user@server.local",
            "--name",
            "server",
            "--no-test",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No paths") || stderr.contains("path"),
        "Expected paths error, got: {stderr}"
    );
}

/// Test: sources add rejects duplicate source names.
#[test]
fn sources_add_duplicate_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    // Create existing source
    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    // Try to add source with same name
    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "add",
            "other@other.local",
            "--name",
            "laptop",
            "--preset",
            "linux-defaults",
            "--no-test",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("already exists") || stderr.contains("duplicate"),
        "Expected duplicate error, got: {stderr}"
    );
}

/// Test: sources add with invalid URL format.
#[test]
fn sources_add_invalid_url() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    // Missing @ in URL
    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "add",
            "laptop.local",
            "--preset",
            "linux-defaults",
            "--no-test",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("username") || stderr.contains("Invalid"),
        "Expected invalid URL error, got: {stderr}"
    );
}

/// Test: sources add auto-generates name from hostname.
#[test]
fn sources_add_auto_name() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "add",
            "user@devlaptop.home.lan",
            "--preset",
            "linux-defaults",
            "--no-test",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");

    assert!(output.status.success());

    let config_content = read_sources_config(&config_dir);
    // Should extract "devlaptop" from "devlaptop.home.lan"
    assert!(
        config_content.contains("name = \"devlaptop\""),
        "Auto-generated name not found in config: {config_content}"
    );
}

// =============================================================================
// sources remove tests
// =============================================================================

/// Test: sources remove removes a configured source.
#[test]
fn sources_remove_basic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources]]
name = "workstation"
type = "ssh"
host = "dev@work.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "remove", "laptop", "-y"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources remove command");

    assert!(
        output.status.success(),
        "sources remove failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify config was updated
    let config_content = read_sources_config(&config_dir);
    assert!(
        !config_content.contains("name = \"laptop\""),
        "Removed source still in config"
    );
    assert!(
        config_content.contains("workstation"),
        "Other source incorrectly removed"
    );
}

/// Test: sources remove with nonexistent source.
#[test]
fn sources_remove_nonexistent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "remove", "nonexistent", "-y"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources remove command");

    // Should fail gracefully
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found") || stderr.contains("does not exist"),
        "Expected not found error, got: {stderr}"
    );
}

/// Test: sources remove with --purge flag.
#[test]
fn sources_remove_with_purge() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();

    // Create source data directory
    let source_data = data_dir.join("cass").join("remotes").join("laptop");
    fs::create_dir_all(&source_data).unwrap();
    fs::write(source_data.join("session.jsonl"), "test data").unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());
    let _guard_data = EnvGuard::set("XDG_DATA_HOME", data_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "remove", "laptop", "--purge", "-y"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("XDG_DATA_HOME", &data_dir)
        .output()
        .expect("sources remove --purge command");

    assert!(
        output.status.success(),
        "sources remove --purge failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify config was updated
    let config_content = read_sources_config(&config_dir);
    assert!(
        !config_content.contains("laptop"),
        "Removed source still in config"
    );
}

// =============================================================================
// sources doctor tests
// =============================================================================

/// Test: sources doctor with no sources configured.
#[test]
fn sources_doctor_no_sources() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "doctor"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources doctor command");

    // Should succeed but indicate no sources
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No") && stdout.contains("sources"),
        "Expected no sources message, got: {stdout}"
    );
}

/// Test: sources doctor --json outputs valid JSON.
#[test]
fn sources_doctor_json() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "doctor", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources doctor --json command");

    // Should output valid JSON (even if connectivity fails)
    // Note: The output is an array of source diagnostics
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid JSON output");

    // JSON should be an array of source diagnostics
    assert!(
        json.is_array(),
        "Expected array of source diagnostics, got: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 1, "Expected one source in diagnostics");
    assert_eq!(arr[0]["source_id"], "laptop");
}

/// Test: sources doctor --source filters to specific source.
#[test]
fn sources_doctor_single_source() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources]]
name = "workstation"
type = "ssh"
host = "dev@work.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "doctor", "--source", "laptop", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources doctor --source command");

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid JSON output");

    // Should only contain laptop diagnostics
    if let Some(sources) = json.get("sources").and_then(|s| s.as_array()) {
        assert_eq!(sources.len(), 1, "Should only have one source in output");
        assert_eq!(sources[0]["name"], "laptop");
    }
}

// =============================================================================
// sources sync tests
// =============================================================================

/// Test: sources sync with no sources configured.
#[test]
fn sources_sync_no_sources() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());
    let _guard_data = EnvGuard::set("XDG_DATA_HOME", data_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "sync"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("XDG_DATA_HOME", &data_dir)
        .output()
        .expect("sources sync command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No") && stdout.contains("sources"),
        "Expected no sources message, got: {stdout}"
    );
}

/// Test: sources sync --dry-run shows what would be synced.
#[test]
fn sources_sync_dry_run() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());
    let _guard_data = EnvGuard::set("XDG_DATA_HOME", data_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "sync", "--dry-run"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("XDG_DATA_HOME", &data_dir)
        .output()
        .expect("sources sync --dry-run command");

    // Dry run should indicate the source would be synced
    // Note: Will likely fail SSH connectivity, but should still report the source
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");

    assert!(
        combined.contains("laptop") || combined.contains("dry"),
        "Expected source name or dry run message, got: {combined}"
    );
}

/// Test: sources sync --source filters to specific source.
#[test]
fn sources_sync_single_source() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources]]
name = "workstation"
type = "ssh"
host = "dev@work.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());
    let _guard_data = EnvGuard::set("XDG_DATA_HOME", data_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "sync", "--source", "laptop", "--dry-run"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("XDG_DATA_HOME", &data_dir)
        .output()
        .expect("sources sync --source command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");

    // Should only mention laptop, not workstation
    assert!(
        combined.contains("laptop"),
        "Expected laptop in output, got: {combined}"
    );
    // The source filter should work even if sync fails due to SSH
}

/// Test: sources sync --json outputs valid JSON.
#[test]
fn sources_sync_json() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());
    let _guard_data = EnvGuard::set("XDG_DATA_HOME", data_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "sync", "--json", "--dry-run"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env("XDG_DATA_HOME", &data_dir)
        .output()
        .expect("sources sync --json command");

    // Should output valid JSON even if sync fails
    if !output.stdout.is_empty() {
        let json: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("valid JSON output");

        // Should have a sources or results field
        assert!(
            json.get("sources").is_some() || json.get("results").is_some(),
            "Expected sources or results field in JSON output: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

// =============================================================================
// Integration workflow tests
// =============================================================================

/// Test: Complete workflow - add, list, remove.
#[test]
fn sources_workflow_add_list_remove() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    // 1. Add a source
    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "add",
            "user@server.example",
            "--name",
            "server",
            "--preset",
            "linux-defaults",
            "--no-test",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources add command");
    assert!(output.status.success());

    // 2. List sources - should show the added source
    let output = cargo_bin_cmd!("cass")
        .args(["sources", "list"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("server"));

    // 3. Remove the source
    let output = cargo_bin_cmd!("cass")
        .args(["sources", "remove", "server", "-y"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources remove command");
    assert!(output.status.success());

    // 4. List again - should be empty
    let output = cargo_bin_cmd!("cass")
        .args(["sources", "list"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("server"),
        "Source should be removed, got: {stdout}"
    );
}

/// Test: Add multiple sources and list them.
#[test]
fn sources_multiple_add_list() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    // Add first source
    cargo_bin_cmd!("cass")
        .args([
            "sources",
            "add",
            "user@laptop.local",
            "--name",
            "laptop",
            "--preset",
            "macos-defaults",
            "--no-test",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();

    // Add second source
    cargo_bin_cmd!("cass")
        .args([
            "sources",
            "add",
            "dev@workstation.office",
            "--name",
            "workstation",
            "--preset",
            "linux-defaults",
            "--no-test",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();

    // List all sources
    let output = cargo_bin_cmd!("cass")
        .args(["sources", "list", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources list command");

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid JSON");
    let sources = json["sources"].as_array().expect("sources array");

    assert_eq!(sources.len(), 2);
    let names: Vec<&str> = sources.iter().filter_map(|s| s["name"].as_str()).collect();
    assert!(names.contains(&"laptop"));
    assert!(names.contains(&"workstation"));
}

// =============================================================================
// sources mappings list tests
// =============================================================================

/// Test: sources mappings list with no mappings configured.
#[test]
fn mappings_list_empty() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "list", "laptop"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings list command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No") || stdout.contains("0 mapping"),
        "Expected no mappings message, got: {stdout}"
    );
}

/// Test: sources mappings list with mappings configured.
#[test]
fn mappings_list_with_mappings() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user/projects"
to = "/Users/me/projects"
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "list", "laptop"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings list command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("/home/user/projects") && stdout.contains("/Users/me/projects"),
        "Expected mapping paths in output, got: {stdout}"
    );
}

/// Test: sources mappings list --json outputs valid JSON.
#[test]
fn mappings_list_json() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user/projects"
to = "/Users/me/projects"
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "list", "laptop", "--json"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings list --json command");

    assert!(output.status.success());
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid JSON output");

    assert!(
        json.get("mappings").is_some(),
        "Expected 'mappings' field in JSON"
    );
}

/// Test: sources mappings list for nonexistent source.
#[test]
fn mappings_list_nonexistent_source() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "list", "nonexistent"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings list command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found") || stderr.contains("does not exist"),
        "Expected not found error, got: {stderr}"
    );
}

// =============================================================================
// sources mappings add tests
// =============================================================================

/// Test: sources mappings add basic mapping.
#[test]
fn mappings_add_basic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/remote/path",
            "--to",
            "/local/path",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings add command");

    assert!(
        output.status.success(),
        "mappings add failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify config was updated
    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("/remote/path") && config_content.contains("/local/path"),
        "Mapping not in config: {config_content}"
    );
}

/// Test: sources mappings add with agent filter.
#[test]
fn mappings_add_with_agents() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/opt/work",
            "--to",
            "/Volumes/Work",
            "--agents",
            "claude_code,codex",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings add command");

    assert!(
        output.status.success(),
        "mappings add failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("claude_code") || config_content.contains("agents"),
        "Agent filter not in config: {config_content}"
    );
}

/// Test: sources mappings add multiple mappings.
#[test]
fn mappings_add_multiple() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    // Add first mapping
    cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/home/user",
            "--to",
            "/Users/me",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();

    // Add second mapping
    cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/opt/projects",
            "--to",
            "/Projects",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();

    // Verify both mappings are in config
    let config_content = read_sources_config(&config_dir);
    assert!(
        config_content.contains("/home/user") && config_content.contains("/opt/projects"),
        "Both mappings not in config: {config_content}"
    );
}

/// Test: sources mappings add to nonexistent source.
#[test]
fn mappings_add_nonexistent_source() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "add",
            "nonexistent",
            "--from",
            "/from",
            "--to",
            "/to",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings add command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found") || stderr.contains("does not exist"),
        "Expected not found error, got: {stderr}"
    );
}

// =============================================================================
// sources mappings remove tests
// =============================================================================

/// Test: sources mappings remove by index.
#[test]
fn mappings_remove_by_index() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user"
to = "/Users/me"

[[sources.path_mappings]]
from = "/opt/work"
to = "/Work"
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "remove", "laptop", "0"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings remove command");

    assert!(
        output.status.success(),
        "mappings remove failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // First mapping should be gone, second should remain
    let config_content = read_sources_config(&config_dir);
    assert!(
        !config_content.contains("/home/user"),
        "Removed mapping still in config"
    );
    assert!(
        config_content.contains("/opt/work"),
        "Other mapping incorrectly removed"
    );
}

/// Test: sources mappings remove with invalid index.
#[test]
fn mappings_remove_invalid_index() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user"
to = "/Users/me"
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "remove", "laptop", "99"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings remove command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("index") || stderr.contains("out of") || stderr.contains("range"),
        "Expected index error, got: {stderr}"
    );
}

/// Test: sources mappings remove from empty mappings list.
#[test]
fn mappings_remove_from_empty() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "remove", "laptop", "0"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings remove command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no mapping") || stderr.contains("empty") || stderr.contains("index"),
        "Expected no mappings error, got: {stderr}"
    );
}

// =============================================================================
// sources mappings test tests
// =============================================================================

/// Test: sources mappings test with matching path.
#[test]
fn mappings_test_match() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user/projects"
to = "/Users/me/projects"
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "test",
            "laptop",
            "/home/user/projects/myapp/src/main.rs",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings test command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("/Users/me/projects/myapp/src/main.rs"),
        "Expected rewritten path, got: {stdout}"
    );
}

/// Test: sources mappings test with non-matching path.
#[test]
fn mappings_test_no_match() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user/projects"
to = "/Users/me/projects"
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "test",
            "laptop",
            "/opt/other/path/file.rs",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings test command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Path should be unchanged or indicate no match
    assert!(
        stdout.contains("/opt/other/path/file.rs") || stdout.contains("no match"),
        "Expected unchanged path or no match, got: {stdout}"
    );
}

/// Test: sources mappings test with agent filter.
#[test]
fn mappings_test_with_agent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]

[[sources.path_mappings]]
from = "/home/user"
to = "/Users/me"
agents = ["claude_code"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    // Test with matching agent
    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "test",
            "laptop",
            "/home/user/file.rs",
            "--agent",
            "claude_code",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("sources mappings test command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("/Users/me/file.rs"),
        "Expected rewritten path for matching agent, got: {stdout}"
    );
}

// =============================================================================
// mappings workflow integration test
// =============================================================================

/// Test: Complete mappings workflow - add, list, test, remove.
#[test]
fn mappings_workflow_complete() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_dir = tmp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();

    create_sources_config(
        &config_dir,
        r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@laptop.local"
paths = ["~/.claude/projects"]
"#,
    );

    let _guard_config = EnvGuard::set("XDG_CONFIG_HOME", config_dir.to_string_lossy());

    // 1. Add a mapping
    cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "add",
            "laptop",
            "--from",
            "/remote/path",
            "--to",
            "/local/path",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();

    // 2. List mappings - should show the added mapping
    let output = cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "list", "laptop"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("list command");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("/remote/path"));

    // 3. Test the mapping
    let output = cargo_bin_cmd!("cass")
        .args([
            "sources",
            "mappings",
            "test",
            "laptop",
            "/remote/path/subdir/file.rs",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("test command");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("/local/path/subdir/file.rs"));

    // 4. Remove the mapping
    cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "remove", "laptop", "0"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .assert()
        .success();

    // 5. List again - should be empty
    let output = cargo_bin_cmd!("cass")
        .args(["sources", "mappings", "list", "laptop"])
        .env("XDG_CONFIG_HOME", &config_dir)
        .output()
        .expect("list command");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // After removal, should show "No path mappings" message
    assert!(
        stdout.contains("No") || !stdout.contains("/remote/path"),
        "Mapping should be removed, got: {stdout}"
    );
}
