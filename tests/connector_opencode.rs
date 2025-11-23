use coding_agent_search::connectors::opencode::OpenCodeConnector;
use coding_agent_search::connectors::{Connector, ScanContext};
use std::path::PathBuf;

#[test]
fn opencode_parses_sqlite_fixture() {
    let fixture_root = PathBuf::from("tests/fixtures/opencode");
    let conn = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_root: fixture_root.clone(),
        since_ts: None,
    };
    let convs = conn.scan(&ctx).expect("scan");
    assert_eq!(convs.len(), 1);
    let c = &convs[0];
    assert_eq!(c.title.as_deref(), Some("OpenCode Session"));
    assert_eq!(c.messages.len(), 2);
}

#[test]
fn opencode_filters_messages_with_since_ts() {
    let fixture_root = PathBuf::from("tests/fixtures/opencode");
    let conn = OpenCodeConnector::new();
    let ctx = ScanContext {
        data_root: fixture_root.clone(),
        since_ts: Some(1_700_000_000_000),
    };
    let convs = conn.scan(&ctx).expect("scan");
    assert_eq!(convs.len(), 1);
    let c = &convs[0];
    assert_eq!(c.messages.len(), 1);
    assert_eq!(c.messages[0].idx, 0);
    assert_eq!(c.messages[0].created_at, Some(1_700_000_005_000));
    assert_eq!(c.started_at, Some(1_700_000_005_000));
    assert_eq!(c.ended_at, Some(1_700_000_005_000));
}
