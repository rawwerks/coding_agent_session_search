use coding_agent_search::connectors::gemini::GeminiConnector;
use coding_agent_search::connectors::{Connector, ScanContext};
use std::path::PathBuf;

#[test]
fn gemini_parses_jsonl_fixture() {
    let fixture_root = PathBuf::from("tests/fixtures/gemini");
    let conn = GeminiConnector::new();
    let ctx = ScanContext {
        data_root: fixture_root.clone(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).expect("scan");
    assert!(
        !convs.is_empty(),
        "expected at least one conversation from fixture root"
    );
    let c = &convs[0];
    assert_eq!(c.messages.len(), 2);
    assert_eq!(c.messages[0].content, "Gemini hello");
}
