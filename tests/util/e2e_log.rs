//! E2E Logging utilities for structured JSONL output.
//!
//! This module provides helpers for Rust E2E tests to emit structured logs
//! following the unified schema defined in `test-results/e2e/SCHEMA.md`.
//!
//! # Usage
//!
//! ```ignore
//! use crate::util::e2e_log::{E2eLogger, E2eTestInfo};
//!
//! let logger = E2eLogger::new("rust")?;
//! logger.run_start()?;
//!
//! let test_info = E2eTestInfo::new("test_pages_export", "e2e_pages", file!(), line!());
//! logger.test_start(&test_info)?;
//!
//! // ... run test ...
//!
//! logger.test_end(&test_info, "pass", duration_ms, None)?;
//! logger.run_end(total, passed, failed, skipped, duration_ms)?;
//! ```

use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Environment metadata captured at the start of a test run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eEnvironment {
    pub git_sha: Option<String>,
    pub git_branch: Option<String>,
    pub os: String,
    pub arch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rust_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cass_version: Option<String>,
    pub ci: bool,
}

impl E2eEnvironment {
    /// Capture current environment metadata.
    pub fn capture() -> Self {
        Self {
            git_sha: Self::git_sha(),
            git_branch: Self::git_branch(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            rust_version: Self::rust_version(),
            node_version: Self::node_version(),
            cass_version: Self::cass_version(),
            ci: std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok(),
        }
    }

    fn git_sha() -> Option<String> {
        Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    }

    fn git_branch() -> Option<String> {
        Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    }

    fn rust_version() -> Option<String> {
        Command::new("rustc")
            .args(["--version"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| {
                let full = String::from_utf8_lossy(&o.stdout);
                // "rustc 1.84.0 (abc123 2025-01-01)" -> "1.84.0"
                full.split_whitespace().nth(1).unwrap_or(&full).to_string()
            })
    }

    fn node_version() -> Option<String> {
        Command::new("node")
            .args(["--version"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    }

    fn cass_version() -> Option<String> {
        // Try to get from Cargo.toml or built binary
        std::env::var("CARGO_PKG_VERSION").ok()
    }
}

/// Test information for logging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eTestInfo {
    pub name: String,
    pub suite: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
}

impl E2eTestInfo {
    pub fn new(name: &str, suite: &str, file: &str, line: u32) -> Self {
        Self {
            name: name.to_string(),
            suite: suite.to_string(),
            file: Some(file.to_string()),
            line: Some(line),
        }
    }

    pub fn simple(name: &str, suite: &str) -> Self {
        Self {
            name: name.to_string(),
            suite: suite.to_string(),
            file: None,
            line: None,
        }
    }
}

/// Test result for test_end events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eTestResult {
    pub status: String,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
}

/// Error information for failed tests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eError {
    pub message: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}

/// Run summary for run_end events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eRunSummary {
    pub total: u32,
    pub passed: u32,
    pub failed: u32,
    pub skipped: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flaky: Option<u32>,
    pub duration_ms: u64,
}

/// Phase information for phase_start/phase_end events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2ePhase {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Log context for log events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eLogContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_name: Option<String>,
}

/// Configuration for the E2E logger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fail_fast: Option<bool>,
}

/// Base event structure that all events share.
#[derive(Debug, Clone, Serialize)]
struct BaseEvent {
    ts: String,
    event: String,
    run_id: String,
    runner: String,
}

/// E2E Logger that writes structured JSONL events.
pub struct E2eLogger {
    run_id: String,
    runner: String,
    output_path: PathBuf,
    writer: Arc<Mutex<BufWriter<File>>>,
    env: E2eEnvironment,
}

impl E2eLogger {
    /// Create a new E2E logger.
    ///
    /// # Arguments
    /// * `runner` - The runner type ("rust", "shell", or "playwright")
    ///
    /// # Returns
    /// A new logger that writes to `test-results/e2e/{runner}_{timestamp}.jsonl`
    pub fn new(runner: &str) -> std::io::Result<Self> {
        let timestamp = Self::timestamp_id();
        let run_id = format!("{}_{}", timestamp, Self::random_suffix());
        let output_dir = Self::output_dir()?;
        let output_path = output_dir.join(format!("{}_{}.jsonl", runner, timestamp));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_path)?;
        let writer = Arc::new(Mutex::new(BufWriter::new(file)));

        Ok(Self {
            run_id,
            runner: runner.to_string(),
            output_path,
            writer,
            env: E2eEnvironment::capture(),
        })
    }

    /// Create a logger with a specific output path (for testing).
    pub fn with_path(runner: &str, output_path: PathBuf) -> std::io::Result<Self> {
        let timestamp = Self::timestamp_id();
        let run_id = format!("{}_{}", timestamp, Self::random_suffix());

        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_path)?;
        let writer = Arc::new(Mutex::new(BufWriter::new(file)));

        Ok(Self {
            run_id,
            runner: runner.to_string(),
            output_path,
            writer,
            env: E2eEnvironment::capture(),
        })
    }

    /// Get the run ID for this logger.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Get the output path for this logger.
    pub fn output_path(&self) -> &PathBuf {
        &self.output_path
    }

    /// Emit a run_start event.
    pub fn run_start(&self, config: Option<E2eConfig>) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct RunStartEvent {
            ts: String,
            event: String,
            run_id: String,
            runner: String,
            env: E2eEnvironment,
            #[serde(skip_serializing_if = "Option::is_none")]
            config: Option<E2eConfig>,
        }

        let event = RunStartEvent {
            ts: Self::iso_timestamp(),
            event: "run_start".to_string(),
            run_id: self.run_id.clone(),
            runner: self.runner.clone(),
            env: self.env.clone(),
            config,
        };

        self.write_event(&event)
    }

    /// Emit a test_start event.
    pub fn test_start(&self, test: &E2eTestInfo) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct TestStartEvent {
            ts: String,
            event: String,
            run_id: String,
            runner: String,
            test: E2eTestInfo,
        }

        let event = TestStartEvent {
            ts: Self::iso_timestamp(),
            event: "test_start".to_string(),
            run_id: self.run_id.clone(),
            runner: self.runner.clone(),
            test: test.clone(),
        };

        self.write_event(&event)
    }

    /// Emit a test_end event for a passing test.
    pub fn test_pass(
        &self,
        test: &E2eTestInfo,
        duration_ms: u64,
        retries: Option<u32>,
    ) -> std::io::Result<()> {
        self.test_end(test, "pass", duration_ms, retries, None)
    }

    /// Emit a test_end event for a failing test.
    pub fn test_fail(
        &self,
        test: &E2eTestInfo,
        duration_ms: u64,
        retries: Option<u32>,
        error: E2eError,
    ) -> std::io::Result<()> {
        self.test_end(test, "fail", duration_ms, retries, Some(error))
    }

    /// Emit a test_end event for a skipped test.
    pub fn test_skip(&self, test: &E2eTestInfo) -> std::io::Result<()> {
        self.test_end(test, "skip", 0, None, None)
    }

    /// Emit a test_end event with full control.
    pub fn test_end(
        &self,
        test: &E2eTestInfo,
        status: &str,
        duration_ms: u64,
        retries: Option<u32>,
        error: Option<E2eError>,
    ) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct TestEndEvent {
            ts: String,
            event: String,
            run_id: String,
            runner: String,
            test: E2eTestInfo,
            result: E2eTestResult,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<E2eError>,
        }

        let event = TestEndEvent {
            ts: Self::iso_timestamp(),
            event: "test_end".to_string(),
            run_id: self.run_id.clone(),
            runner: self.runner.clone(),
            test: test.clone(),
            result: E2eTestResult {
                status: status.to_string(),
                duration_ms,
                retries,
            },
            error,
        };

        self.write_event(&event)
    }

    /// Emit a run_end event.
    pub fn run_end(&self, summary: E2eRunSummary, exit_code: i32) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct RunEndEvent {
            ts: String,
            event: String,
            run_id: String,
            runner: String,
            summary: E2eRunSummary,
            exit_code: i32,
        }

        let event = RunEndEvent {
            ts: Self::iso_timestamp(),
            event: "run_end".to_string(),
            run_id: self.run_id.clone(),
            runner: self.runner.clone(),
            summary,
            exit_code,
        };

        self.write_event(&event)?;
        self.flush()
    }

    /// Emit a log event.
    pub fn log(
        &self,
        level: &str,
        msg: &str,
        context: Option<E2eLogContext>,
    ) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct LogEvent {
            ts: String,
            event: String,
            run_id: String,
            runner: String,
            level: String,
            msg: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            context: Option<E2eLogContext>,
        }

        let event = LogEvent {
            ts: Self::iso_timestamp(),
            event: "log".to_string(),
            run_id: self.run_id.clone(),
            runner: self.runner.clone(),
            level: level.to_string(),
            msg: msg.to_string(),
            context,
        };

        self.write_event(&event)
    }

    /// Convenience: log at INFO level.
    pub fn info(&self, msg: &str) -> std::io::Result<()> {
        self.log("INFO", msg, None)
    }

    /// Convenience: log at WARN level.
    pub fn warn(&self, msg: &str) -> std::io::Result<()> {
        self.log("WARN", msg, None)
    }

    /// Convenience: log at ERROR level.
    pub fn error(&self, msg: &str) -> std::io::Result<()> {
        self.log("ERROR", msg, None)
    }

    /// Convenience: log at DEBUG level.
    pub fn debug(&self, msg: &str) -> std::io::Result<()> {
        self.log("DEBUG", msg, None)
    }

    /// Emit a phase_start event.
    pub fn phase_start(&self, phase: &E2ePhase) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct PhaseStartEvent {
            ts: String,
            event: String,
            run_id: String,
            runner: String,
            phase: E2ePhase,
        }

        let event = PhaseStartEvent {
            ts: Self::iso_timestamp(),
            event: "phase_start".to_string(),
            run_id: self.run_id.clone(),
            runner: self.runner.clone(),
            phase: phase.clone(),
        };

        self.write_event(&event)
    }

    /// Emit a phase_end event.
    pub fn phase_end(&self, phase: &E2ePhase, duration_ms: u64) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct PhaseEndEvent {
            ts: String,
            event: String,
            run_id: String,
            runner: String,
            phase: E2ePhase,
            duration_ms: u64,
        }

        let event = PhaseEndEvent {
            ts: Self::iso_timestamp(),
            event: "phase_end".to_string(),
            run_id: self.run_id.clone(),
            runner: self.runner.clone(),
            phase: phase.clone(),
            duration_ms,
        };

        self.write_event(&event)
    }

    /// Flush the writer to ensure all events are persisted.
    pub fn flush(&self) -> std::io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.flush()
    }

    // Internal helpers

    fn write_event<T: Serialize>(&self, event: &T) -> std::io::Result<()> {
        let json = serde_json::to_string(event)?;
        let mut writer = self.writer.lock().unwrap();
        writeln!(writer, "{}", json)?;
        Ok(())
    }

    fn output_dir() -> std::io::Result<PathBuf> {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        let output_dir = manifest_dir.join("test-results").join("e2e");
        fs::create_dir_all(&output_dir)?;
        Ok(output_dir)
    }

    fn iso_timestamp() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let millis = now.subsec_millis();

        // Convert to ISO-8601 format
        let datetime = chrono::DateTime::from_timestamp(secs as i64, millis * 1_000_000)
            .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH);
        datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    }

    fn timestamp_id() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let datetime = chrono::DateTime::from_timestamp(secs as i64, 0)
            .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH);
        datetime.format("%Y%m%d_%H%M%S").to_string()
    }

    fn random_suffix() -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        std::process::id().hash(&mut hasher);
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .hash(&mut hasher);
        format!("{:x}", hasher.finish() & 0xFFFFFF)
    }
}

/// Convenience macro for creating E2eTestInfo with file and line.
#[macro_export]
macro_rules! e2e_test_info {
    ($name:expr, $suite:expr) => {
        $crate::util::e2e_log::E2eTestInfo::new($name, $suite, file!(), line!())
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_environment_capture() {
        let env = E2eEnvironment::capture();
        assert!(!env.os.is_empty());
        assert!(!env.arch.is_empty());
    }

    #[test]
    fn test_logger_creates_file() {
        let tmp = TempDir::new().unwrap();
        let output_path = tmp.path().join("test.jsonl");

        let logger = E2eLogger::with_path("rust", output_path.clone()).unwrap();
        logger.run_start(None).unwrap();
        logger.flush().unwrap();

        assert!(output_path.exists());
        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("run_start"));
        assert!(content.contains(&logger.run_id));
    }

    #[test]
    fn test_logger_test_lifecycle() {
        let tmp = TempDir::new().unwrap();
        let output_path = tmp.path().join("lifecycle.jsonl");

        let logger = E2eLogger::with_path("rust", output_path.clone()).unwrap();

        let test_info = E2eTestInfo::new("test_example", "unit", "test.rs", 42);

        logger.run_start(None).unwrap();
        logger.test_start(&test_info).unwrap();
        logger.test_pass(&test_info, 100, None).unwrap();
        logger
            .run_end(
                E2eRunSummary {
                    total: 1,
                    passed: 1,
                    failed: 0,
                    skipped: 0,
                    flaky: None,
                    duration_ms: 100,
                },
                0,
            )
            .unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 4);
        assert!(lines[0].contains("run_start"));
        assert!(lines[1].contains("test_start"));
        assert!(lines[2].contains("test_end"));
        assert!(lines[3].contains("run_end"));
    }

    #[test]
    fn test_logger_error_event() {
        let tmp = TempDir::new().unwrap();
        let output_path = tmp.path().join("error.jsonl");

        let logger = E2eLogger::with_path("rust", output_path.clone()).unwrap();
        let test_info = E2eTestInfo::simple("failing_test", "e2e");

        logger
            .test_fail(
                &test_info,
                500,
                Some(1),
                E2eError {
                    message: "assertion failed".to_string(),
                    error_type: Some("AssertionError".to_string()),
                    stack: Some("at line 42".to_string()),
                },
            )
            .unwrap();
        logger.flush().unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("assertion failed"));
        assert!(content.contains("AssertionError"));
        assert!(content.contains("\"status\":\"fail\""));
    }

    #[test]
    fn test_log_levels() {
        let tmp = TempDir::new().unwrap();
        let output_path = tmp.path().join("logs.jsonl");

        let logger = E2eLogger::with_path("rust", output_path.clone()).unwrap();

        logger.info("info message").unwrap();
        logger.warn("warning message").unwrap();
        logger.error("error message").unwrap();
        logger.debug("debug message").unwrap();
        logger.flush().unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("\"level\":\"INFO\""));
        assert!(content.contains("\"level\":\"WARN\""));
        assert!(content.contains("\"level\":\"ERROR\""));
        assert!(content.contains("\"level\":\"DEBUG\""));
    }
}
