#[cfg(test)]
mod tests {
    use anyhow::Result;
    use coding_agent_search::pages::export::{ExportEngine, ExportFilter, PathMode};
    use rusqlite::Connection;
    use std::path::Path;
    use tempfile::TempDir;

    fn setup_source_db(path: &Path) -> Result<()> {
        let conn = Connection::open(path)?;

        conn.execute_batch(
            r#"
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY,
                agent TEXT NOT NULL,
                workspace TEXT,
                title TEXT,
                source_path TEXT NOT NULL,
                started_at INTEGER,
                ended_at INTEGER,
                message_count INTEGER,
                metadata_json TEXT
            );

            CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                idx INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at INTEGER,
                updated_at INTEGER,
                model TEXT
            );
            "#,
        )?;

        // Insert test data
        conn.execute(
            "INSERT INTO conversations (id, agent, workspace, title, source_path, started_at, message_count) 
             VALUES (1, 'claude', '/home/user/proj1', 'Test Conv 1', '/home/user/proj1/.claude/1.json', 1600000000000, 2)",
            [],
        )?;
        conn.execute(
            "INSERT INTO messages (conversation_id, idx, role, content, created_at)
             VALUES (1, 0, 'user', 'hello', 1600000000000)",
            [],
        )?;
        conn.execute(
            "INSERT INTO messages (conversation_id, idx, role, content, created_at)
             VALUES (1, 1, 'assistant', 'world', 1600000005000)",
            [],
        )?;

        conn.execute(
            "INSERT INTO conversations (id, agent, workspace, title, source_path, started_at, message_count) 
             VALUES (2, 'codex', '/home/user/proj2', 'Test Conv 2', '/home/user/proj2/.codex/session.json', 1700000000000, 1)",
            [],
        )?;
        conn.execute(
            "INSERT INTO messages (conversation_id, idx, role, content, created_at)
             VALUES (2, 0, 'user', 'rust code', 1700000000000)",
            [],
        )?;

        Ok(())
    }

    #[test]
    fn test_export_engine_basic() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_path = temp_dir.path().join("source.db");
        let output_path = temp_dir.path().join("export.db");

        setup_source_db(&source_path)?;

        let filter = ExportFilter {
            agents: None,
            workspaces: None,
            since: None,
            until: None,
            path_mode: PathMode::Relative,
        };

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        let stats = engine.execute(|_, _| {}, None)?;

        assert_eq!(stats.conversations_processed, 2);
        assert_eq!(stats.messages_processed, 3);

        // Verify output DB
        let conn = Connection::open(&output_path)?;

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM conversations", [], |r| r.get(0))?;
        assert_eq!(count, 2);

        let fts_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM messages_fts", [], |r| r.get(0))?;
        assert_eq!(fts_count, 3);

        // Verify Path Transformation (Relative)
        let path: String = conn.query_row(
            "SELECT source_path FROM conversations WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(path, ".claude/1.json"); // Stripped workspace prefix

        Ok(())
    }

    #[test]
    fn test_export_filter_agent() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_path = temp_dir.path().join("source.db");
        let output_path = temp_dir.path().join("export.db");

        setup_source_db(&source_path)?;

        let filter = ExportFilter {
            agents: Some(vec!["claude".to_string()]),
            workspaces: None,
            since: None,
            until: None,
            path_mode: PathMode::Relative,
        };

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        let stats = engine.execute(|_, _| {}, None)?;

        assert_eq!(stats.conversations_processed, 1);

        let conn = Connection::open(&output_path)?;
        let agent: String = conn.query_row("SELECT agent FROM conversations", [], |r| r.get(0))?;
        assert_eq!(agent, "claude");

        Ok(())
    }

    #[test]
    fn test_export_path_mode_hash() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_path = temp_dir.path().join("source.db");
        let output_path = temp_dir.path().join("export.db");

        setup_source_db(&source_path)?;

        let filter = ExportFilter {
            agents: None,
            workspaces: None,
            since: None,
            until: None,
            path_mode: PathMode::Hash,
        };

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        engine.execute(|_, _| {}, None)?;

        let conn = Connection::open(&output_path)?;
        let path: String = conn.query_row(
            "SELECT source_path FROM conversations WHERE id=1",
            [],
            |r| r.get(0),
        )?;

        assert_eq!(path.len(), 16); // 16 chars hex
        assert_ne!(path, "/home/user/proj1/.claude/1.json");

        Ok(())
    }
}
