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

    #[test]
    fn test_export_filter_multiple_agents() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_path = temp_dir.path().join("source.db");
        let output_path = temp_dir.path().join("export.db");

        setup_source_db(&source_path)?;

        // Filter for both agents
        let filter = ExportFilter {
            agents: Some(vec!["claude".to_string(), "codex".to_string()]),
            workspaces: None,
            since: None,
            until: None,
            path_mode: PathMode::Relative,
        };

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        let stats = engine.execute(|_, _| {}, None)?;

        // Should get both conversations
        assert_eq!(stats.conversations_processed, 2);
        assert_eq!(stats.messages_processed, 3);

        Ok(())
    }

    #[test]
    fn test_export_filter_time_range() -> Result<()> {
        use chrono::{TimeZone, Utc};

        let temp_dir = TempDir::new()?;
        let source_path = temp_dir.path().join("source.db");
        let output_path = temp_dir.path().join("export.db");

        setup_source_db(&source_path)?;

        // Filter for conversations after first one's start time
        // Conv 1: started_at = 1600000000000 (Sep 2020)
        // Conv 2: started_at = 1700000000000 (Nov 2023)
        let filter = ExportFilter {
            agents: None,
            workspaces: None,
            since: Some(Utc.timestamp_millis_opt(1650000000000).unwrap()), // ~Apr 2022
            until: None,
            path_mode: PathMode::Relative,
        };

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        let stats = engine.execute(|_, _| {}, None)?;

        // Should only get conv 2 (codex)
        assert_eq!(stats.conversations_processed, 1);

        let conn = Connection::open(&output_path)?;
        let agent: String = conn.query_row("SELECT agent FROM conversations", [], |r| r.get(0))?;
        assert_eq!(agent, "codex");

        Ok(())
    }

    #[test]
    fn test_export_filter_workspace() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_path = temp_dir.path().join("source.db");
        let output_path = temp_dir.path().join("export.db");

        setup_source_db(&source_path)?;

        let filter = ExportFilter {
            agents: None,
            workspaces: Some(vec![std::path::PathBuf::from("/home/user/proj1")]),
            since: None,
            until: None,
            path_mode: PathMode::Relative,
        };

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        let stats = engine.execute(|_, _| {}, None)?;

        // Should only get conv 1 (claude in proj1)
        assert_eq!(stats.conversations_processed, 1);

        let conn = Connection::open(&output_path)?;
        let agent: String = conn.query_row("SELECT agent FROM conversations", [], |r| r.get(0))?;
        assert_eq!(agent, "claude");

        Ok(())
    }

    #[test]
    fn test_export_empty_result() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_path = temp_dir.path().join("source.db");
        let output_path = temp_dir.path().join("export.db");

        setup_source_db(&source_path)?;

        // Filter for non-existent agent
        let filter = ExportFilter {
            agents: Some(vec!["nonexistent".to_string()]),
            workspaces: None,
            since: None,
            until: None,
            path_mode: PathMode::Relative,
        };

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        let stats = engine.execute(|_, _| {}, None)?;

        // Should get empty result
        assert_eq!(stats.conversations_processed, 0);
        assert_eq!(stats.messages_processed, 0);

        // Output DB should still exist with schema
        let conn = Connection::open(&output_path)?;
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM conversations", [], |r| r.get(0))?;
        assert_eq!(count, 0);

        // Schema should exist (FTS table)
        let _: i64 = conn.query_row("SELECT COUNT(*) FROM messages_fts", [], |r| r.get(0))?;

        Ok(())
    }

    #[test]
    fn test_export_path_mode_basename() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_path = temp_dir.path().join("source.db");
        let output_path = temp_dir.path().join("export.db");

        setup_source_db(&source_path)?;

        let filter = ExportFilter {
            agents: None,
            workspaces: None,
            since: None,
            until: None,
            path_mode: PathMode::Basename,
        };

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        engine.execute(|_, _| {}, None)?;

        let conn = Connection::open(&output_path)?;
        let path: String = conn.query_row(
            "SELECT source_path FROM conversations WHERE id=1",
            [],
            |r| r.get(0),
        )?;

        // Should be just the filename
        assert_eq!(path, "1.json");

        Ok(())
    }

    #[test]
    fn test_export_path_mode_full() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_path = temp_dir.path().join("source.db");
        let output_path = temp_dir.path().join("export.db");

        setup_source_db(&source_path)?;

        let filter = ExportFilter {
            agents: None,
            workspaces: None,
            since: None,
            until: None,
            path_mode: PathMode::Full,
        };

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        engine.execute(|_, _| {}, None)?;

        let conn = Connection::open(&output_path)?;
        let path: String = conn.query_row(
            "SELECT source_path FROM conversations WHERE id=1",
            [],
            |r| r.get(0),
        )?;

        // Should be full path unchanged
        assert_eq!(path, "/home/user/proj1/.claude/1.json");

        Ok(())
    }

    #[test]
    fn test_export_progress_callback() -> Result<()> {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

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

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();

        let engine = ExportEngine::new(&source_path, &output_path, filter);
        engine.execute(
            move |current, total| {
                callback_count_clone.fetch_add(1, Ordering::SeqCst);
                assert!(current <= total);
            },
            None,
        )?;

        // Should have been called for each conversation (2)
        assert_eq!(callback_count.load(Ordering::SeqCst), 2);

        Ok(())
    }
}
