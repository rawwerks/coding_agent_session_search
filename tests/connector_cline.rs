use coding_agent_search::connectors::cline::ClineConnector;
use coding_agent_search::connectors::{Connector, ScanContext};
use std::path::PathBuf;

#[test]
fn cline_parses_fixture_task() {
    let fixture_root = PathBuf::from("tests/fixtures/cline");
    let conn = ClineConnector::new();
    let ctx = ScanContext {
        data_root: fixture_root.clone(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).expect("scan");
    assert_eq!(convs.len(), 1);
    let c = &convs[0];
    assert_eq!(c.title.as_deref(), Some("Cline fixture task"));
    // We now prefer ui_messages.json (2 msgs) over api_conversation_history.json (1 msg)
    // to avoid duplicates and prefer user-facing content.
    assert_eq!(c.messages.len(), 2);
    assert!(c.messages.iter().any(|m| m.content.contains("Hello Cline")));
}

#[test]
fn cline_respects_since_ts_and_resequences_indices() {
    let dir = tempfile::TempDir::new().unwrap();

    // Point HOME to temp so storage_root resolves inside the sandbox.
    unsafe {
        std::env::set_var("HOME", dir.path());
    }

    let root = dir
        .path()
        .join(".config/Code/User/globalStorage/saoudrizwan.claude-dev/task-123");
    std::fs::create_dir_all(&root).unwrap();

    let ui_messages_path = root.join("ui_messages.json");

    // Two messages: older (timestamp=1_000) and newer (timestamp=2_000).
    let msgs = serde_json::json!([
        {
            "timestamp": 1_000,
            "role": "user",
            "content": "old msg"
        },
        {
            "timestamp": 2_000,
            "role": "assistant",
            "content": "new msg"
        }
    ]);
    std::fs::write(&ui_messages_path, serde_json::to_string(&msgs).unwrap()).unwrap();

    let connector = ClineConnector::new();
    let ctx = ScanContext {
        data_root: dir.path().to_path_buf(),
        since_ts: Some(1_500),
    };

    let convs = connector.scan(&ctx).unwrap();
    assert_eq!(convs.len(), 1);
    let c = &convs[0];

    // Should keep only the newer message
    assert_eq!(
        c.messages.len(),
        1,
        "expected since_ts to drop older messages"
    );
    let msg = &c.messages[0];
    assert_eq!(msg.idx, 0, "idx should be re-sequenced after filtering");
    assert_eq!(msg.role, "assistant");
    assert!(msg.content.contains("new msg"));
}
