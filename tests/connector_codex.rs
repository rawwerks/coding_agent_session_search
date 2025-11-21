use std::fs;
use tempfile::TempDir;

use coding_agent_search::connectors::{Connector, ScanContext, codex::CodexConnector};

#[test]
fn codex_connector_reads_rollout_jsonl() {
    let dir = TempDir::new().unwrap();
    let sessions = dir.path().join("sessions/2025/11/21");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-1.jsonl");
    let sample = r#"
{"role":"user","timestamp":1700000000000,"content":"write a hello program"}
{"role":"assistant","timestamp":1700000001000,"content":"here is code"}
"#;
    fs::write(&file, sample.trim_start()).unwrap();

    // Safe in test scope: we control process env.
    unsafe {
        std::env::set_var("CODEX_HOME", dir.path());
    }

    let connector = CodexConnector::new();
    let ctx = ScanContext {
        data_root: dir.path().to_path_buf(),
        since_ts: None,
    };
    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    let c = &convs[0];
    assert_eq!(c.agent_slug, "codex");
    assert_eq!(c.messages.len(), 2);
    assert!(c.title.as_ref().unwrap().contains("write a hello program"));
}
