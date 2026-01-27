//! End-to-end integration tests for the Pages export pipeline (P6.5).
//!
//! This module validates the complete workflow:
//! - Export → Encrypt → Bundle → Verify → Decrypt
//!
//! # Running
//!
//! ```bash
//! # Run all pages E2E tests
//! cargo test --test e2e_pages
//!
//! # Run with detailed logging
//! RUST_LOG=debug cargo test --test e2e_pages -- --nocapture
//!
//! # Run specific test
//! cargo test --test e2e_pages test_full_export_pipeline_password_only
//! ```

use coding_agent_search::model::types::{Agent, AgentKind};
use coding_agent_search::pages::bundle::{BundleBuilder, BundleResult};
use coding_agent_search::pages::encrypt::{DecryptionEngine, EncryptionEngine, load_config};
use coding_agent_search::pages::export::{ExportEngine, ExportFilter, PathMode};
use coding_agent_search::pages::verify::verify_bundle;
use coding_agent_search::storage::sqlite::SqliteStorage;
use rusqlite::Connection;
use std::fs;
use std::path::Path;
use std::time::Instant;
use tempfile::TempDir;

#[path = "util/mod.rs"]
mod util;

use util::ConversationFixtureBuilder;
use util::e2e_log::{E2eLogger, E2ePhase};

// =============================================================================
// Test Constants
// =============================================================================

const TEST_PASSWORD: &str = "test-password-123!";
const TEST_RECOVERY_SECRET: &[u8] = b"recovery-secret-32bytes-padding!";
const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB chunks

// =============================================================================
// E2E Logger Support
// =============================================================================

/// Check if E2E logging is enabled via environment variable.
fn e2e_logging_enabled() -> bool {
    std::env::var("E2E_LOG").is_ok()
}

/// Phase tracker that uses E2eLogger when enabled.
///
/// Emits structured phase_start/phase_end events and also prints to stderr
/// for CI parsing compatibility.
struct PhaseTracker {
    logger: Option<E2eLogger>,
}

impl PhaseTracker {
    /// Create a new PhaseTracker, optionally with E2eLogger if E2E_LOG is set.
    fn new() -> Self {
        let logger = if e2e_logging_enabled() {
            E2eLogger::new("rust").ok()
        } else {
            None
        };
        Self { logger }
    }

    /// Start a phase and return the start time for duration calculation.
    fn start(&self, name: &str, description: Option<&str>) -> Instant {
        let phase = E2ePhase {
            name: name.to_string(),
            description: description.map(String::from),
        };
        if let Some(ref lg) = self.logger {
            let _ = lg.phase_start(&phase);
        }
        Instant::now()
    }

    /// End a phase, logging duration to E2eLogger and stderr.
    fn end(&self, name: &str, description: Option<&str>, start: Instant) {
        let duration_ms = start.elapsed().as_millis() as u64;
        let phase = E2ePhase {
            name: name.to_string(),
            description: description.map(String::from),
        };
        if let Some(ref lg) = self.logger {
            let _ = lg.phase_end(&phase, duration_ms);
        }
        // Also emit to stderr for CI compatibility
        eprintln!(
            "{{\"phase\":\"{}\",\"duration_ms\":{},\"status\":\"PASS\"}}",
            name, duration_ms
        );
    }

    /// Flush the logger if present.
    fn flush(&self) {
        if let Some(ref lg) = self.logger {
            let _ = lg.flush();
        }
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Setup a test database with conversations.
fn setup_test_db(data_dir: &Path, conversation_count: usize) -> std::path::PathBuf {
    let db_path = data_dir.join("agent_search.db");

    let mut storage = SqliteStorage::open(&db_path).expect("Failed to open storage");

    // Create agent
    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    // Create workspace
    let workspace_path = Path::new("/home/user/projects/test");
    let workspace_id = Some(
        storage
            .ensure_workspace(workspace_path, None)
            .expect("ensure workspace"),
    );

    // Create conversations
    for i in 0..conversation_count {
        let conversation = ConversationFixtureBuilder::new("claude_code")
            .title(format!("Test Conversation {}", i))
            .workspace(workspace_path)
            .source_path(format!(
                "/home/user/.claude/projects/test/session-{}.jsonl",
                i
            ))
            .messages(10)
            .with_content(0, format!("User message {} - requesting help with code", i))
            .with_content(1, format!("Assistant response {} - here's the solution", i))
            .build_conversation();

        storage
            .insert_conversation_tree(agent_id, workspace_id, &conversation)
            .expect("Failed to insert conversation");
    }

    db_path
}

/// Build the complete pipeline and return artifacts.
struct PipelineArtifacts {
    export_db_path: std::path::PathBuf,
    bundle: BundleResult,
    _temp_dir: TempDir, // Keep alive for duration of test
}

fn build_full_pipeline(
    conversation_count: usize,
    include_password: bool,
    include_recovery: bool,
) -> PipelineArtifacts {
    let tracker = PhaseTracker::new();
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir).expect("Failed to create data directory");

    // Step 1: Setup database
    let start = tracker.start(
        "setup_database",
        Some("Create test database with conversations"),
    );
    let source_db_path = setup_test_db(&data_dir, conversation_count);
    tracker.end(
        "setup_database",
        Some("Create test database with conversations"),
        start,
    );

    // Step 2: Export
    let start = tracker.start("export", Some("Export conversations to staging database"));
    let export_staging = temp_dir.path().join("export_staging");
    fs::create_dir_all(&export_staging).expect("Failed to create export staging");
    let export_db_path = export_staging.join("export.db");

    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Relative,
    };

    let export_engine = ExportEngine::new(&source_db_path, &export_db_path, filter);
    let stats = export_engine
        .execute(|_, _| {}, None)
        .expect("Export failed");
    assert!(
        stats.conversations_processed > 0,
        "Should export at least one conversation"
    );
    tracker.end(
        "export",
        Some("Export conversations to staging database"),
        start,
    );

    // Step 3: Encrypt
    let start = tracker.start("encrypt", Some("Encrypt exported database with AES-GCM"));
    let encrypt_dir = temp_dir.path().join("encrypt_staging");
    let mut enc_engine = EncryptionEngine::new(CHUNK_SIZE);

    if include_password {
        enc_engine
            .add_password_slot(TEST_PASSWORD)
            .expect("Failed to add password slot");
    }

    if include_recovery {
        enc_engine
            .add_recovery_slot(TEST_RECOVERY_SECRET)
            .expect("Failed to add recovery slot");
    }

    let _enc_config = enc_engine
        .encrypt_file(&export_db_path, &encrypt_dir, |_, _| {})
        .expect("Encryption failed");
    tracker.end(
        "encrypt",
        Some("Encrypt exported database with AES-GCM"),
        start,
    );

    // Step 4: Bundle
    let start = tracker.start("bundle", Some("Create deployable web bundle"));
    let bundle_dir = temp_dir.path().join("bundle");
    let mut builder = BundleBuilder::new()
        .title("E2E Test Archive")
        .description("Test archive for integration tests")
        .generate_qr(false);

    if include_recovery {
        builder = builder.recovery_secret(Some(TEST_RECOVERY_SECRET.to_vec()));
    }

    let bundle = builder
        .build(&encrypt_dir, &bundle_dir, |_, _| {})
        .expect("Bundle failed");
    tracker.end("bundle", Some("Create deployable web bundle"), start);

    tracker.flush();

    PipelineArtifacts {
        export_db_path,
        bundle,
        _temp_dir: temp_dir,
    }
}

// =============================================================================
// Test: Full Export Pipeline (Password Only)
// =============================================================================

/// Test the complete export pipeline with password-only authentication.
#[test]
fn test_full_export_pipeline_password_only() {
    let tracker = PhaseTracker::new();
    let test_start = Instant::now();
    eprintln!("{{\"test\":\"test_full_export_pipeline_password_only\",\"status\":\"START\"}}");

    let artifacts = build_full_pipeline(5, true, false);

    // Verify bundle structure
    let verify_start = tracker.start("verify_structure", Some("Validate bundle artifacts exist"));
    let site = &artifacts.bundle.site_dir;
    assert!(site.join("index.html").exists(), "index.html should exist");
    assert!(site.join("sw.js").exists(), "sw.js should exist");
    assert!(
        site.join("config.json").exists(),
        "config.json should exist"
    );
    assert!(
        site.join("payload").exists(),
        "payload directory should exist"
    );

    // Verify config.json has single key slot
    let config_str = fs::read_to_string(site.join("config.json")).expect("read config");
    let config: serde_json::Value = serde_json::from_str(&config_str).expect("parse config");
    let slots = config.get("key_slots").expect("key_slots field");
    assert_eq!(slots.as_array().unwrap().len(), 1, "Should have 1 key slot");
    assert_eq!(
        slots[0].get("kdf").unwrap().as_str().unwrap(),
        "argon2id",
        "Should use argon2id KDF"
    );

    tracker.end(
        "verify_structure",
        Some("Validate bundle artifacts exist"),
        verify_start,
    );
    tracker.flush();
    eprintln!(
        "{{\"test\":\"test_full_export_pipeline_password_only\",\"duration_ms\":{},\"status\":\"PASS\"}}",
        test_start.elapsed().as_millis()
    );
}

// =============================================================================
// Test: Full Export Pipeline (Password + Recovery)
// =============================================================================

/// Test the complete export pipeline with dual authentication (password + recovery).
#[test]
fn test_full_export_pipeline_dual_auth() {
    let start = Instant::now();
    eprintln!("{{\"test\":\"test_full_export_pipeline_dual_auth\",\"status\":\"START\"}}");

    let artifacts = build_full_pipeline(3, true, true);

    // Verify config.json has two key slots
    let site = &artifacts.bundle.site_dir;
    let config_str = fs::read_to_string(site.join("config.json")).expect("read config");
    let config: serde_json::Value = serde_json::from_str(&config_str).expect("parse config");
    let slots = config.get("key_slots").expect("key_slots field");
    let slots_arr = slots.as_array().unwrap();
    assert_eq!(slots_arr.len(), 2, "Should have 2 key slots");

    // Verify first slot is password (argon2id)
    assert_eq!(
        slots_arr[0].get("kdf").unwrap().as_str().unwrap(),
        "argon2id"
    );

    // Verify second slot is recovery (hkdf-sha256)
    assert_eq!(
        slots_arr[1].get("kdf").unwrap().as_str().unwrap(),
        "hkdf-sha256"
    );

    // Verify private directory has recovery secret
    assert!(
        artifacts
            .bundle
            .private_dir
            .join("recovery-secret.txt")
            .exists(),
        "recovery-secret.txt should exist"
    );

    eprintln!(
        "{{\"test\":\"test_full_export_pipeline_dual_auth\",\"duration_ms\":{},\"status\":\"PASS\"}}",
        start.elapsed().as_millis()
    );
}

// =============================================================================
// Test: Integrity and Decrypt Roundtrip
// =============================================================================

/// Test that decrypted payload matches original export database.
#[test]
fn test_integrity_decrypt_roundtrip_password() {
    let tracker = PhaseTracker::new();
    let test_start = Instant::now();
    eprintln!("{{\"test\":\"test_integrity_decrypt_roundtrip_password\",\"status\":\"START\"}}");

    let temp_dir = TempDir::new().unwrap();
    let artifacts = build_full_pipeline(2, true, true);

    // Decrypt with password
    let decrypt_start = tracker.start(
        "decrypt_password",
        Some("Decrypt payload using password-derived key"),
    );
    let config = load_config(&artifacts.bundle.site_dir).expect("load config");
    let decryptor =
        DecryptionEngine::unlock_with_password(config, TEST_PASSWORD).expect("unlock password");
    let decrypted_path = temp_dir.path().join("decrypted_password.db");
    decryptor
        .decrypt_to_file(&artifacts.bundle.site_dir, &decrypted_path, |_, _| {})
        .expect("decrypt with password");

    // Verify bytes match
    let original = fs::read(&artifacts.export_db_path).expect("read original");
    let decrypted = fs::read(&decrypted_path).expect("read decrypted");
    assert_eq!(
        original, decrypted,
        "Decrypted content should match original"
    );

    tracker.end(
        "decrypt_password",
        Some("Decrypt payload using password-derived key"),
        decrypt_start,
    );
    tracker.flush();
    eprintln!(
        "{{\"test\":\"test_integrity_decrypt_roundtrip_password\",\"duration_ms\":{},\"status\":\"PASS\"}}",
        test_start.elapsed().as_millis()
    );
}

/// Test that decrypted payload matches original using recovery key.
#[test]
fn test_integrity_decrypt_roundtrip_recovery() {
    let tracker = PhaseTracker::new();
    let test_start = Instant::now();
    eprintln!("{{\"test\":\"test_integrity_decrypt_roundtrip_recovery\",\"status\":\"START\"}}");

    let temp_dir = TempDir::new().unwrap();
    let artifacts = build_full_pipeline(2, true, true);

    // Decrypt with recovery key
    let decrypt_start = tracker.start(
        "decrypt_recovery",
        Some("Decrypt payload using recovery secret"),
    );
    let config = load_config(&artifacts.bundle.site_dir).expect("load config");
    let decryptor = DecryptionEngine::unlock_with_recovery(config, TEST_RECOVERY_SECRET)
        .expect("unlock recovery");
    let decrypted_path = temp_dir.path().join("decrypted_recovery.db");
    decryptor
        .decrypt_to_file(&artifacts.bundle.site_dir, &decrypted_path, |_, _| {})
        .expect("decrypt with recovery");
    tracker.end(
        "decrypt_recovery",
        Some("Decrypt payload using recovery secret"),
        decrypt_start,
    );

    // Verify bytes match
    let verify_start = tracker.start(
        "verify_content",
        Some("Compare decrypted content with original"),
    );
    let original = fs::read(&artifacts.export_db_path).expect("read original");
    let decrypted = fs::read(&decrypted_path).expect("read decrypted");
    assert_eq!(
        original, decrypted,
        "Decrypted content should match original"
    );
    tracker.end(
        "verify_content",
        Some("Compare decrypted content with original"),
        verify_start,
    );

    tracker.flush();
    eprintln!(
        "{{\"test\":\"test_integrity_decrypt_roundtrip_recovery\",\"duration_ms\":{},\"status\":\"PASS\"}}",
        test_start.elapsed().as_millis()
    );
}

// =============================================================================
// Test: Tampering Detection
// =============================================================================

/// Test that tampering with a chunk fails authentication.
#[test]
fn test_tampering_fails_authentication() {
    let tracker = PhaseTracker::new();
    let test_start = Instant::now();
    eprintln!("{{\"test\":\"test_tampering_fails_authentication\",\"status\":\"START\"}}");

    let artifacts = build_full_pipeline(2, true, false);
    let site_dir = &artifacts.bundle.site_dir;

    // Baseline: verify passes
    let phase_start = tracker.start(
        "verify_baseline",
        Some("Verify bundle is valid before tampering"),
    );
    let baseline = verify_bundle(site_dir, false).expect("verify baseline");
    assert_eq!(baseline.status, "valid", "Baseline should be valid");
    tracker.end(
        "verify_baseline",
        Some("Verify bundle is valid before tampering"),
        phase_start,
    );

    // Find and corrupt a payload chunk
    let phase_start = tracker.start(
        "corrupt_chunk",
        Some("Modify payload chunk to simulate tampering"),
    );
    let payload_dir = site_dir.join("payload");
    let chunk = fs::read_dir(&payload_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.extension().map(|e| e == "bin").unwrap_or(false))
        .expect("payload chunk");
    fs::write(&chunk, b"corrupted payload data").expect("corrupt chunk");
    tracker.end(
        "corrupt_chunk",
        Some("Modify payload chunk to simulate tampering"),
        phase_start,
    );

    // Verify should now detect corruption
    let phase_start = tracker.start(
        "verify_corruption_detected",
        Some("Confirm verification detects tampering"),
    );
    let result = verify_bundle(site_dir, false).expect("verify after corruption");
    assert_eq!(
        result.status, "invalid",
        "Corrupted bundle should be invalid"
    );
    tracker.end(
        "verify_corruption_detected",
        Some("Confirm verification detects tampering"),
        phase_start,
    );

    tracker.flush();
    eprintln!(
        "{{\"test\":\"test_tampering_fails_authentication\",\"duration_ms\":{},\"status\":\"PASS\"}}",
        test_start.elapsed().as_millis()
    );
}

// =============================================================================
// Test: Bundle Verification
// =============================================================================

/// Test CLI verify command works correctly.
/// NOTE: Requires the cass binary to be built first (`cargo build`)
#[test]
#[ignore = "Requires cass binary - run with --ignored or after cargo build"]
fn test_cli_verify_command() {
    use assert_cmd::cargo::cargo_bin_cmd;

    let tracker = PhaseTracker::new();
    let test_start = Instant::now();
    eprintln!("{{\"test\":\"test_cli_verify_command\",\"status\":\"START\"}}");

    let artifacts = build_full_pipeline(1, true, false);

    // Run cass pages --verify
    let phase_start = tracker.start("cli_verify", Some("Execute cass pages --verify command"));
    let mut cmd = cargo_bin_cmd!("cass");
    let assert = cmd
        .arg("pages")
        .arg("--verify")
        .arg(&artifacts.bundle.site_dir)
        .arg("--json")
        .assert();

    assert.success();
    tracker.end(
        "cli_verify",
        Some("Execute cass pages --verify command"),
        phase_start,
    );

    tracker.flush();
    eprintln!(
        "{{\"test\":\"test_cli_verify_command\",\"duration_ms\":{},\"status\":\"PASS\"}}",
        test_start.elapsed().as_millis()
    );
}

// =============================================================================
// Test: Search in Decrypted Archive
// =============================================================================

/// Test that we can query the decrypted export database.
#[test]
fn test_search_in_decrypted_archive() {
    let tracker = PhaseTracker::new();
    let test_start = Instant::now();
    eprintln!("{{\"test\":\"test_search_in_decrypted_archive\",\"status\":\"START\"}}");

    let temp_dir = TempDir::new().unwrap();
    let artifacts = build_full_pipeline(5, true, false);

    // Decrypt
    let phase_start = tracker.start("decrypt", Some("Decrypt payload to SQLite database"));
    let config = load_config(&artifacts.bundle.site_dir).expect("load config");
    let decryptor = DecryptionEngine::unlock_with_password(config, TEST_PASSWORD).expect("unlock");
    let decrypted_path = temp_dir.path().join("decrypted.db");
    decryptor
        .decrypt_to_file(&artifacts.bundle.site_dir, &decrypted_path, |_, _| {})
        .expect("decrypt");
    tracker.end(
        "decrypt",
        Some("Decrypt payload to SQLite database"),
        phase_start,
    );

    // Open the export database directly (it has a different schema than the main DB)
    let phase_start = tracker.start(
        "query_database",
        Some("Query decrypted database to verify schema"),
    );
    let conn = Connection::open(&decrypted_path).expect("open decrypted db");

    // Verify conversations table exists and has data
    let conv_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM conversations", [], |row| row.get(0))
        .expect("count conversations");
    assert_eq!(conv_count, 5, "Should have 5 conversations");

    // Verify messages table exists and has data
    let msg_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
        .expect("count messages");
    assert!(msg_count > 0, "Should have messages");

    // Verify export_meta table has schema version
    let schema_version: String = conn
        .query_row(
            "SELECT value FROM export_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .expect("get schema version");
    assert_eq!(schema_version, "1", "Export schema version should be 1");
    tracker.end(
        "query_database",
        Some("Query decrypted database to verify schema"),
        phase_start,
    );

    tracker.flush();
    eprintln!(
        "{{\"test\":\"test_search_in_decrypted_archive\",\"duration_ms\":{},\"status\":\"PASS\"}}",
        test_start.elapsed().as_millis()
    );
}
