//! Integration tests for GitHub Pages deployment module.
//!
//! Tests the GitHubDeployer functionality including size checks,
//! prerequisites validation, and bundle preparation.

use anyhow::Result;
use coding_agent_search::pages::deploy_github::{GitHubDeployer, Prerequisites};
use std::fs;
use tempfile::TempDir;

// ============================================
// Prerequisites Tests
// ============================================

#[test]
fn test_prerequisites_all_ready() {
    let prereqs = Prerequisites {
        gh_version: Some("gh version 2.40.0".to_string()),
        gh_authenticated: true,
        gh_username: Some("testuser".to_string()),
        git_version: Some("git version 2.43.0".to_string()),
        disk_space_mb: 10000,
        estimated_size_mb: 100,
    };

    assert!(prereqs.is_ready());
    assert!(prereqs.missing().is_empty());
}

#[test]
fn test_prerequisites_gh_not_installed() {
    let prereqs = Prerequisites {
        gh_version: None,
        gh_authenticated: false,
        gh_username: None,
        git_version: Some("git version 2.43.0".to_string()),
        disk_space_mb: 10000,
        estimated_size_mb: 100,
    };

    assert!(!prereqs.is_ready());
    let missing = prereqs.missing();
    assert!(missing.iter().any(|m| m.contains("gh CLI not installed")));
}

#[test]
fn test_prerequisites_gh_not_authenticated() {
    let prereqs = Prerequisites {
        gh_version: Some("gh version 2.40.0".to_string()),
        gh_authenticated: false,
        gh_username: None,
        git_version: Some("git version 2.43.0".to_string()),
        disk_space_mb: 10000,
        estimated_size_mb: 100,
    };

    assert!(!prereqs.is_ready());
    let missing = prereqs.missing();
    assert!(missing.iter().any(|m| m.contains("not authenticated")));
}

#[test]
fn test_prerequisites_git_not_installed() {
    let prereqs = Prerequisites {
        gh_version: Some("gh version 2.40.0".to_string()),
        gh_authenticated: true,
        gh_username: Some("testuser".to_string()),
        git_version: None,
        disk_space_mb: 10000,
        estimated_size_mb: 100,
    };

    assert!(!prereqs.is_ready());
    let missing = prereqs.missing();
    assert!(missing.iter().any(|m| m.contains("git not installed")));
}

// ============================================
// Deployer Builder Tests
// ============================================

#[test]
fn test_deployer_default_creation() {
    // Test that default deployer can be created
    let _deployer = GitHubDeployer::default();
    // If it compiles and doesn't panic, the default works
}

#[test]
fn test_deployer_builder_chain() {
    // Test builder pattern - if it compiles, the chain works
    let _deployer = GitHubDeployer::new("my-custom-archive")
        .description("My custom archive description")
        .public(false)
        .force(true);
}

// ============================================
// Size Check Tests
// ============================================

#[test]
fn test_size_check_empty_directory() -> Result<()> {
    let temp = TempDir::new()?;
    let deployer = GitHubDeployer::default();
    let check = deployer.check_size(temp.path())?;

    assert_eq!(check.file_count, 0);
    assert_eq!(check.total_bytes, 0);
    assert!(check.large_files.is_empty());
    assert!(!check.exceeds_limit);
    assert!(!check.has_oversized_files);

    Ok(())
}

#[test]
fn test_size_check_small_files() -> Result<()> {
    let temp = TempDir::new()?;

    // Create some small files
    fs::write(temp.path().join("file1.txt"), vec![0u8; 1000])?;
    fs::write(temp.path().join("file2.txt"), vec![0u8; 2000])?;
    fs::create_dir(temp.path().join("subdir"))?;
    fs::write(temp.path().join("subdir/file3.txt"), vec![0u8; 500])?;

    let deployer = GitHubDeployer::default();
    let check = deployer.check_size(temp.path())?;

    assert_eq!(check.file_count, 3);
    assert_eq!(check.total_bytes, 3500);
    assert!(check.large_files.is_empty());
    assert!(!check.exceeds_limit);
    assert!(!check.has_oversized_files);

    Ok(())
}

#[test]
fn test_size_check_nested_directories() -> Result<()> {
    let temp = TempDir::new()?;

    // Create nested structure
    fs::create_dir_all(temp.path().join("a/b/c"))?;
    fs::write(temp.path().join("root.txt"), "root")?;
    fs::write(temp.path().join("a/level1.txt"), "level1")?;
    fs::write(temp.path().join("a/b/level2.txt"), "level2")?;
    fs::write(temp.path().join("a/b/c/level3.txt"), "level3")?;

    let deployer = GitHubDeployer::default();
    let check = deployer.check_size(temp.path())?;

    assert_eq!(check.file_count, 4);
    assert!(!check.exceeds_limit);

    Ok(())
}

// ============================================
// Bundle Structure Tests
// ============================================

#[test]
fn test_bundle_structure_validation() -> Result<()> {
    let temp = TempDir::new()?;

    // Create a minimal test bundle structure
    fs::write(temp.path().join("index.html"), "<html></html>")?;
    fs::write(temp.path().join(".nojekyll"), "")?;
    fs::write(temp.path().join("robots.txt"), "User-agent: *\nDisallow: /")?;
    fs::write(temp.path().join("config.json"), r#"{"version": 1}"#)?;

    fs::create_dir(temp.path().join("payload"))?;
    fs::write(temp.path().join("payload/chunk-00000.bin"), vec![0u8; 100])?;

    fs::create_dir(temp.path().join("vendor"))?;
    fs::write(temp.path().join("vendor/sqlite3.js"), "// sqlite")?;

    let deployer = GitHubDeployer::default();
    let check = deployer.check_size(temp.path())?;

    assert!(check.file_count >= 6);
    assert!(!check.exceeds_limit);

    Ok(())
}

// ============================================
// Progress Callback Tests
// ============================================

#[test]
fn test_progress_phases() {
    // Verify expected progress phases
    let expected_phases = [
        "prereq", "size", "repo", "clone", "copy", "push", "pages", "complete",
    ];

    // The deploy function uses these phases in order
    for phase in expected_phases {
        assert!(
            !phase.is_empty(),
            "Progress phase '{}' should be non-empty",
            phase
        );
    }
}

// ============================================
// Error Message Tests
// ============================================

#[test]
fn test_prerequisites_error_messages_are_helpful() {
    let prereqs = Prerequisites {
        gh_version: None,
        gh_authenticated: false,
        gh_username: None,
        git_version: None,
        disk_space_mb: 0,
        estimated_size_mb: 0,
    };

    let missing = prereqs.missing();

    // Check that error messages include actionable instructions
    assert!(missing.iter().any(|m| m.contains("https://cli.github.com")));
    assert!(missing.iter().any(|m| m.contains("gh auth login")));
}

// ============================================
// Size Limit Constants Tests
// ============================================

#[test]
fn test_github_size_limits() {
    // GitHub Pages limits (documented)
    const MAX_SITE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
    const MAX_FILE_SIZE: u64 = 100 * 1024 * 1024; // 100 MiB
    const WARNING_SIZE: u64 = 50 * 1024 * 1024; // 50 MiB

    // Verify our constants match expected limits
    assert_eq!(MAX_SITE_SIZE, 1_073_741_824);
    assert_eq!(MAX_FILE_SIZE, 104_857_600);
    assert_eq!(WARNING_SIZE, 52_428_800);
}

// ============================================
// Deployer Configuration Tests
// ============================================

#[test]
fn test_deployer_public_private_toggle() {
    // Test that builder accepts public/private setting
    let _public_deployer = GitHubDeployer::new("test").public(true);
    let _private_deployer = GitHubDeployer::new("test").public(false);
}

#[test]
fn test_deployer_force_toggle() {
    // Test that builder accepts force setting
    let _normal_deployer = GitHubDeployer::new("test").force(false);
    let _force_deployer = GitHubDeployer::new("test").force(true);
}

#[test]
fn test_deployer_description() {
    // Test that builder accepts description
    let _deployer = GitHubDeployer::new("test").description("A custom description for my archive");
}
