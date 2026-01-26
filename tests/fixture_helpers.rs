//! Fixture helpers for connector tests.
//!
//! This module provides utilities for loading real session fixtures and setting up
//! test environments without using "mock-*" directory naming.
//!
//! # Migration Guide
//!
//! **Old pattern (deprecated):**
//! ```ignore
//! let projects = dir.path().join("mock-claude/projects/test-proj");
//! ```
//!
//! **New pattern:**
//! ```ignore
//! let projects = dir.path().join("fixture-claude/projects/test-proj");
//! // Or use the helper:
//! let (dir, data_dir) = setup_connector_test("claude");
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Directory naming prefix for connector test fixtures.
/// Using "fixture-" instead of "mock-" to clearly indicate real test data.
pub const FIXTURE_PREFIX: &str = "fixture-";

/// Legacy prefix that should be migrated away from.
pub const LEGACY_PREFIX: &str = "mock-";

/// Set up a temp directory structure for connector testing.
///
/// Returns `(TempDir, data_dir_path)` where `data_dir_path` is the path
/// that should be passed to `ScanContext::data_dir`.
///
/// # Example
/// ```ignore
/// let (dir, data_dir) = setup_connector_test("claude");
/// let projects = data_dir.join("projects/my-project");
/// fs::create_dir_all(&projects).unwrap();
/// ```
pub fn setup_connector_test(agent_name: &str) -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("create temp dir");
    let data_dir = dir.path().join(format!("{}{}", FIXTURE_PREFIX, agent_name));
    fs::create_dir_all(&data_dir).expect("create data dir");
    (dir, data_dir)
}

/// Set up a connector test with projects subdirectory.
///
/// Returns `(TempDir, data_dir, projects_dir)`.
pub fn setup_connector_test_with_projects(agent_name: &str) -> (TempDir, PathBuf, PathBuf) {
    let (dir, data_dir) = setup_connector_test(agent_name);
    let projects_dir = data_dir.join("projects");
    fs::create_dir_all(&projects_dir).expect("create projects dir");
    (dir, data_dir, projects_dir)
}

/// Copy a fixture file from the fixtures directory to a temp location.
///
/// # Arguments
/// * `fixture_path` - Relative path within `tests/fixtures/` (e.g., "claude_code_real/projects/...")
/// * `dest_path` - Absolute destination path
///
/// # Returns
/// The destination path if copy succeeded.
pub fn copy_fixture(fixture_path: &str, dest_path: &Path) -> std::io::Result<PathBuf> {
    let src = PathBuf::from("tests/fixtures").join(fixture_path);
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(&src, dest_path)?;
    Ok(dest_path.to_path_buf())
}

/// Load a fixture file and return its contents.
pub fn load_fixture(fixture_path: &str) -> std::io::Result<String> {
    let path = PathBuf::from("tests/fixtures").join(fixture_path);
    fs::read_to_string(path)
}

/// Create a project directory within a connector test setup.
///
/// Returns the full path to the project directory.
pub fn create_project_dir(data_dir: &Path, project_name: &str) -> PathBuf {
    let project_dir = data_dir.join("projects").join(project_name);
    fs::create_dir_all(&project_dir).expect("create project dir");
    project_dir
}

/// Write a session file with the given content.
///
/// Creates parent directories if needed.
pub fn write_session_file(project_dir: &Path, filename: &str, content: &str) -> PathBuf {
    let file_path = project_dir.join(filename);
    fs::write(&file_path, content).expect("write session file");
    file_path
}

/// Check if a test is using legacy "mock-" naming and suggest migration.
///
/// Call this in tests that haven't been migrated yet to track progress.
#[allow(dead_code)]
pub fn check_legacy_naming(path: &Path) -> bool {
    let path_str = path.to_string_lossy();
    if path_str.contains(LEGACY_PREFIX) {
        eprintln!(
            "WARNING: Test uses legacy '{}' naming. Consider migrating to '{}' pattern.",
            LEGACY_PREFIX, FIXTURE_PREFIX
        );
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setup_connector_test() {
        let (dir, data_dir) = setup_connector_test("claude");
        assert!(data_dir.exists());
        assert!(data_dir.to_string_lossy().contains("fixture-claude"));
        drop(dir); // Cleanup
    }

    #[test]
    fn test_setup_with_projects() {
        let (_dir, data_dir, projects_dir) = setup_connector_test_with_projects("codex");
        assert!(projects_dir.exists());
        assert_eq!(projects_dir, data_dir.join("projects"));
    }

    #[test]
    fn test_create_project_dir() {
        let (dir, data_dir) = setup_connector_test("cursor");
        let project = create_project_dir(&data_dir, "my-app");
        assert!(project.exists());
        assert!(project.ends_with("projects/my-app"));
        drop(dir);
    }
}
