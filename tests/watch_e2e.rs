use std::path::Path;
use std::time::Duration;

use tempfile::TempDir;

fn cass_bin() -> String {
    std::env::var("CARGO_BIN_EXE_cass")
        .ok()
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_cass").to_string())
}

fn run_watch_once(
    paths: &[&Path],
    data_dir: &Path,
    home_dir: &Path,
    xdg_data: &Path,
    xdg_config: &Path,
) -> (std::process::Output, String, String) {
    let mut cmd = std::process::Command::new(cass_bin());
    cmd.arg("index")
        .arg("--watch")
        .arg("--watch-once")
        .arg(
            paths
                .iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
        .arg("--data-dir")
        .arg(data_dir)
        .env("HOME", home_dir)
        .env("XDG_DATA_HOME", xdg_data)
        .env("XDG_CONFIG_HOME", xdg_config)
        .env("CODEX_HOME", data_dir.join(".codex"));
    let output = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("run watch");
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    (output, stdout, stderr)
}

fn read_watch_state(path: &Path) -> std::collections::HashMap<String, i64> {
    let contents = std::fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("missing watch_state at {}", path.display()));
    // Handle new versioned format: {"v":1,"m":{"cx":123}} where "m" is the map
    if let Ok(versioned) = serde_json::from_str::<serde_json::Value>(&contents)
        && let Some(map) = versioned.get("m")
        && let Ok(parsed) = serde_json::from_value(map.clone())
    {
        return parsed;
    }
    // Fallback to legacy format: {"Codex":123}
    serde_json::from_str(&contents).expect("parse watch_state")
}

/// E2E: watch-mode smoke. Touch a fixture file and ensure incremental re-index logs fire.
#[test]
fn watch_mode_reindexes_on_file_change() {
    // Temp sandbox to isolate all filesystem access
    let sandbox = TempDir::new().expect("temp dir");
    let data_dir = sandbox.path().join("data");
    let home_dir = sandbox.path().join("home");
    let xdg_data = sandbox.path().join("xdg-data");
    let xdg_config = sandbox.path().join("xdg-config");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    std::fs::create_dir_all(&home_dir).expect("home dir");
    std::fs::create_dir_all(&xdg_data).expect("xdg data");
    std::fs::create_dir_all(&xdg_config).expect("xdg config");

    // Seed a tiny connector fixture under Codex path so watch can detect
    let codex_root = data_dir.join(".codex/sessions");
    std::fs::create_dir_all(&codex_root).expect("codex root");
    let rollout = codex_root.join("rollout-1.jsonl");
    std::fs::write(
        &rollout,
        r#"{"role":"user","content":"hello","createdAt":1700000000000}"#,
    )
    .expect("write rollout");

    // Start watch in background: cass index --watch --data-dir <tmp>
    let (output, stdout, stderr) = run_watch_once(
        &[rollout.as_path()],
        &data_dir,
        &home_dir,
        &xdg_data,
        &xdg_config,
    );
    assert!(
        output.status.success(),
        "watch run failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // Verify watch_state.json was updated for Codex connector
    let watch_state_path = data_dir.join("watch_state.json");
    let map = read_watch_state(&watch_state_path);
    // New format uses compact keys: "cx" for Codex
    let ts = map.get("cx").copied().unwrap_or(0);
    assert!(
        ts > 0,
        "expected Codex (cx) entry in watch_state, got {map:?}"
    );
}

/// Ensure multiple paths (cross connectors) are handled and `watch_state` records both.
#[test]
fn watch_mode_updates_multiple_connectors() {
    let sandbox = TempDir::new().expect("temp dir");
    let data_dir = sandbox.path().join("data");
    let home_dir = sandbox.path().join("home");
    let xdg_data = sandbox.path().join("xdg-data");
    let xdg_config = sandbox.path().join("xdg-config");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&home_dir).unwrap();
    std::fs::create_dir_all(&xdg_data).unwrap();
    std::fs::create_dir_all(&xdg_config).unwrap();

    // Codex fixture
    let codex_root = data_dir.join(".codex/sessions/2025/12/02");
    std::fs::create_dir_all(&codex_root).unwrap();
    let codex_file = codex_root.join("rollout-2.jsonl");
    std::fs::write(
        &codex_file,
        r#"{"type":"user","createdAt":1700000000000,"payload":{"text":"ping codex"}}"#,
    )
    .unwrap();

    // Claude fixture lives under HOME/.claude/projects for detection
    let claude_root = home_dir.join(".claude/projects/demo");
    std::fs::create_dir_all(&claude_root).unwrap();
    let claude_file = claude_root.join("session.claude");
    std::fs::write(
        &claude_file,
        r#"{"type":"user","timestamp":1700000001000,"text":"ping claude"}"#,
    )
    .unwrap();

    let (output, stdout, stderr) = run_watch_once(
        &[codex_file.as_path(), claude_file.as_path()],
        &data_dir,
        &home_dir,
        &xdg_data,
        &xdg_config,
    );
    assert!(
        output.status.success(),
        "watch run failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let map = read_watch_state(&data_dir.join("watch_state.json"));
    // New format uses compact keys: "cx" for Codex, "cd" for Claude
    assert!(
        map.contains_key("cx"),
        "expected Codex (cx) entry, got {map:?}"
    );
    assert!(
        map.contains_key("cd"),
        "expected Claude (cd) entry, got {map:?}"
    );
}

/// If files change quickly in succession, `watch_once` should still refresh timestamps.
#[test]
fn watch_mode_handles_rapid_changes() {
    let sandbox = TempDir::new().expect("temp dir");
    let data_dir = sandbox.path().join("data");
    let home_dir = sandbox.path().join("home");
    let xdg_data = sandbox.path().join("xdg-data");
    let xdg_config = sandbox.path().join("xdg-config");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&home_dir).unwrap();
    std::fs::create_dir_all(&xdg_data).unwrap();
    std::fs::create_dir_all(&xdg_config).unwrap();

    let codex_root = data_dir.join(".codex/sessions");
    std::fs::create_dir_all(&codex_root).unwrap();
    let rollout = codex_root.join("rollout-rapid.jsonl");
    std::fs::write(
        &rollout,
        r#"{"role":"user","content":"first","createdAt":1700000000000}"#,
    )
    .unwrap();

    let (first, stdout1, stderr1) = run_watch_once(
        &[rollout.as_path()],
        &data_dir,
        &home_dir,
        &xdg_data,
        &xdg_config,
    );
    assert!(
        first.status.success(),
        "first watch failed\nstdout:\n{stdout1}\nstderr:\n{stderr1}"
    );
    let ts1 = read_watch_state(&data_dir.join("watch_state.json"))
        .get("cx")
        .copied()
        .unwrap_or(0);
    // Touch file quickly and rerun
    std::fs::write(
        &rollout,
        r#"{"role":"user","content":"second","createdAt":1700000001000}"#,
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let (second, stdout2, stderr2) = run_watch_once(
        &[rollout.as_path()],
        &data_dir,
        &home_dir,
        &xdg_data,
        &xdg_config,
    );
    assert!(
        second.status.success(),
        "second watch failed\nstdout:\n{stdout2}\nstderr:\n{stderr2}"
    );
    let ts2 = read_watch_state(&data_dir.join("watch_state.json"))
        .get("cx")
        .copied()
        .unwrap_or(0);
    assert!(
        ts2 >= ts1,
        "expected timestamp to advance or stay equal; before={ts1}, after={ts2}"
    );
}

/// Corrupt inputs should not crash `watch_once`; it should exit successfully and keep state file.
#[test]
fn watch_mode_survives_corrupt_file() {
    let sandbox = TempDir::new().expect("temp dir");
    let data_dir = sandbox.path().join("data");
    let home_dir = sandbox.path().join("home");
    let xdg_data = sandbox.path().join("xdg-data");
    let xdg_config = sandbox.path().join("xdg-config");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&home_dir).unwrap();
    std::fs::create_dir_all(&xdg_data).unwrap();
    std::fs::create_dir_all(&xdg_config).unwrap();

    let codex_root = data_dir.join(".codex/sessions");
    std::fs::create_dir_all(&codex_root).unwrap();
    let rollout = codex_root.join("rollout-corrupt.jsonl");
    std::fs::write(&rollout, r#"{"role": "user", "content": bad json"#).unwrap();

    let (output, stdout, stderr) = run_watch_once(
        &[rollout.as_path()],
        &data_dir,
        &home_dir,
        &xdg_data,
        &xdg_config,
    );
    assert!(
        output.status.success(),
        "watch with corrupt file should not crash\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        data_dir.join("watch_state.json").exists(),
        "watch_state should still be written"
    );
}
