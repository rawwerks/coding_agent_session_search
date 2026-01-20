use coding_agent_search::storage::sqlite::{CURRENT_SCHEMA_VERSION, MigrationError, SqliteStorage};
use rusqlite::Connection;
use std::path::Path;
use tempfile::TempDir;

// Helper to create a V1 database with some data
fn create_v1_db(path: &Path) {
    let conn = Connection::open(path).expect("create v1 db");
    conn.execute_batch(
        r"
        PRAGMA foreign_keys = ON;
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO meta(key, value) VALUES('schema_version', '1');

        CREATE TABLE agents (
            id INTEGER PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            version TEXT,
            kind TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE workspaces (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE,
            display_name TEXT
        );

        CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL REFERENCES agents(id),
            workspace_id INTEGER REFERENCES workspaces(id),
            external_id TEXT,
            title TEXT,
            source_path TEXT NOT NULL,
            started_at INTEGER,
            ended_at INTEGER,
            approx_tokens INTEGER,
            metadata_json TEXT,
            UNIQUE(agent_id, external_id)
        );

        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
            idx INTEGER NOT NULL,
            role TEXT NOT NULL,
            author TEXT,
            created_at INTEGER,
            content TEXT NOT NULL,
            extra_json TEXT,
            UNIQUE(conversation_id, idx)
        );

        CREATE TABLE snippets (
            id INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            file_path TEXT,
            start_line INTEGER,
            end_line INTEGER,
            language TEXT,
            snippet_text TEXT
        );

        CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);

        CREATE TABLE conversation_tags (
            conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
            tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
            PRIMARY KEY (conversation_id, tag_id)
        );

        -- Insert sample data
        INSERT INTO agents(slug, name, kind, created_at, updated_at)
        VALUES ('claude', 'Claude', 'cli', 1000, 1000);

        INSERT INTO conversations(agent_id, source_path, title, started_at)
        VALUES (1, '/logs/v1.jsonl', 'V1 Conversation', 2000);

        INSERT INTO messages(conversation_id, idx, role, content, created_at)
        VALUES (1, 0, 'user', 'Hello from V1', 2000);
        ",
    )
    .expect("setup v1 schema/data");
}

#[test]
fn test_migration_v1_to_current_preserves_data() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("v1_to_curr.db");

    create_v1_db(&db_path);

    // Perform migration
    let storage = SqliteStorage::open(&db_path).expect("open and migrate");

    // Check version
    assert_eq!(storage.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);

    // Verify data preservation
    let conn = storage.raw();

    // Check Agent
    let agent_name: String = conn
        .query_row("SELECT name FROM agents WHERE slug = 'claude'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(agent_name, "Claude");

    // Check Conversation
    let title: String = conn
        .query_row(
            "SELECT title FROM conversations WHERE source_path = '/logs/v1.jsonl'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(title, "V1 Conversation");

    // Check Message
    let content: String = conn
        .query_row(
            "SELECT content FROM messages WHERE content = 'Hello from V1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(content, "Hello from V1");

    // Verify V2+ features (FTS)
    let fts_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM fts_messages", [], |r| r.get(0))
        .unwrap();
    // V1 migration should populate FTS?
    // The migration V2 script does: INSERT INTO fts_messages SELECT ... FROM messages ...
    // So yes, it should be 1.
    assert_eq!(fts_count, 1, "FTS should be backfilled");

    // Verify V4 features (Sources)
    let source_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM sources WHERE id = 'local'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(source_count, 1, "Local source should be created");

    // Verify V5 features (source_id)
    let source_id: String = conn
        .query_row(
            "SELECT source_id FROM conversations WHERE source_path = '/logs/v1.jsonl'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        source_id, "local",
        "Legacy conversations should be attributed to local source"
    );

    // Verify V7 features (binary columns) - should be NULL for legacy rows
    let metadata_bin: Option<Vec<u8>> = conn
        .query_row(
            "SELECT metadata_bin FROM conversations WHERE source_path = '/logs/v1.jsonl'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        metadata_bin.is_none(),
        "Legacy rows should have NULL binary metadata"
    );

    let extra_bin: Option<Vec<u8>> = conn
        .query_row(
            "SELECT extra_bin FROM messages WHERE content = 'Hello from V1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        extra_bin.is_none(),
        "Legacy rows should have NULL binary extra"
    );
}

#[test]
fn test_rebuild_safety_on_corruption() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("corrupt.db");

    // Create a corrupted file
    std::fs::write(&db_path, "Not a SQLite file").unwrap();

    // open_or_rebuild should fail with RebuildRequired
    let result = SqliteStorage::open_or_rebuild(&db_path);

    match result {
        Err(MigrationError::RebuildRequired {
            reason,
            backup_path,
        }) => {
            println!("Rebuild required as expected: {}", reason);
            assert!(backup_path.is_some());
            let backup = backup_path.unwrap();
            assert!(backup.exists());

            // Verify backup contains original corrupted data
            let content = std::fs::read_to_string(&backup).unwrap();
            assert_eq!(content, "Not a SQLite file");

            // The original file should be gone (or replaced? logic says remove_database_files called)
            assert!(!db_path.exists());

            // Now we can "rebuild" by opening fresh
            let _new_storage = SqliteStorage::open(&db_path).expect("open fresh");
            assert!(db_path.exists());
        }
        _ => panic!("Should have required rebuild"),
    }
}

#[test]
fn test_missing_meta_triggers_rebuild() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("no_meta.db");

    // Create a valid SQLite DB but without meta table (simulating very old or broken state)
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE some_table (id INTEGER)", [])
            .unwrap();
    }

    let result = SqliteStorage::open_or_rebuild(&db_path);
    match result {
        Err(MigrationError::RebuildRequired { reason, .. }) => {
            assert!(reason.contains("metadata"));
        }
        _ => panic!("Should have required rebuild due to missing meta"),
    }
}

#[test]
fn test_future_schema_triggers_rebuild() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("future.db");

    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE meta (key TEXT, value TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO meta VALUES ('schema_version', '9999')", [])
            .unwrap();
    }

    let result = SqliteStorage::open_or_rebuild(&db_path);
    match result {
        Err(MigrationError::RebuildRequired { reason, .. }) => {
            assert!(reason.contains("newer than supported"));
        }
        _ => panic!("Should have required rebuild due to future version"),
    }
}
