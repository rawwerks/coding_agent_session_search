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
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    /// Performance metrics captured during test execution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<E2ePerformanceMetrics>,
}

/// Error information for failed tests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eError {
    pub message: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<E2eErrorContext>,
}

/// Additional context captured at the point of failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eErrorContext {
    /// Relevant state values at failure point (key-value pairs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<std::collections::HashMap<String, serde_json::Value>>,
    /// Path to screenshot file (for browser tests)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub screenshot_path: Option<String>,
    /// Sanitized environment variables (sensitive values redacted)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_vars: Option<std::collections::HashMap<String, String>>,
    /// Current working directory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Command that was being executed (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Stdout from failed command
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    /// Stderr from failed command
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
}

impl E2eError {
    /// Create a basic error with just a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            error_type: None,
            stack: None,
            context: None,
        }
    }

    /// Create an error with type information.
    pub fn with_type(message: impl Into<String>, error_type: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            error_type: Some(error_type.into()),
            stack: None,
            context: None,
        }
    }

    /// Add a stack trace to the error.
    pub fn with_stack(mut self, stack: impl Into<String>) -> Self {
        self.stack = Some(stack.into());
        self
    }

    /// Add context to the error.
    pub fn with_context(mut self, context: E2eErrorContext) -> Self {
        self.context = Some(context);
        self
    }
}

impl E2eErrorContext {
    /// Create an empty error context.
    pub fn new() -> Self {
        Self {
            state: None,
            screenshot_path: None,
            env_vars: None,
            cwd: None,
            command: None,
            stdout: None,
            stderr: None,
        }
    }

    /// Add state values to the context.
    pub fn with_state(
        mut self,
        state: std::collections::HashMap<String, serde_json::Value>,
    ) -> Self {
        self.state = Some(state);
        self
    }

    /// Add a single state value.
    pub fn add_state(
        mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        let state = self
            .state
            .get_or_insert_with(std::collections::HashMap::new);
        state.insert(key.into(), value.into());
        self
    }

    /// Add screenshot path.
    pub fn with_screenshot(mut self, path: impl Into<String>) -> Self {
        self.screenshot_path = Some(path.into());
        self
    }

    /// Capture and sanitize current environment variables.
    pub fn capture_env(mut self) -> Self {
        self.env_vars = Some(capture_sanitized_env());
        self
    }

    /// Add specific environment variables (sanitized).
    pub fn with_env(mut self, env: std::collections::HashMap<String, String>) -> Self {
        self.env_vars = Some(env);
        self
    }

    /// Capture current working directory.
    pub fn capture_cwd(mut self) -> Self {
        if let Ok(cwd) = std::env::current_dir() {
            self.cwd = Some(cwd.display().to_string());
        }
        self
    }

    /// Add command information.
    pub fn with_command(mut self, cmd: impl Into<String>) -> Self {
        self.command = Some(cmd.into());
        self
    }

    /// Add command output.
    pub fn with_output(mut self, stdout: Option<String>, stderr: Option<String>) -> Self {
        self.stdout = stdout;
        self.stderr = stderr;
        self
    }
}

impl Default for E2eErrorContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Capture environment variables with sensitive values redacted.
///
/// Sensitive keys are detected by patterns like:
/// - *_KEY, *_SECRET, *_TOKEN, *_PASSWORD, *_CREDENTIAL
/// - API_*, AUTH_*, AWS_*, GITHUB_TOKEN, etc.
pub fn capture_sanitized_env() -> std::collections::HashMap<String, String> {
    let sensitive_patterns = [
        "_KEY",
        "_SECRET",
        "_TOKEN",
        "_PASSWORD",
        "_CREDENTIAL",
        "_PASS",
        "API_",
        "AUTH_",
        "AWS_",
        "PRIVATE",
        "ENCRYPTION",
    ];
    let sensitive_exact = [
        "GITHUB_TOKEN",
        "NPM_TOKEN",
        "CARGO_REGISTRY_TOKEN",
        "DATABASE_URL",
        "REDIS_URL",
        "MONGODB_URI",
    ];

    std::env::vars()
        .filter(|(k, _)| {
            // Only include relevant env vars
            k.starts_with("RUST_")
                || k.starts_with("CARGO_")
                || k.starts_with("CI")
                || k.starts_with("GITHUB_")
                || k.starts_with("E2E_")
                || k.starts_with("TEST_")
                || k == "HOME"
                || k == "PATH"
                || k == "USER"
                || k == "SHELL"
                || k == "TERM"
        })
        .map(|(k, v)| {
            let is_sensitive = sensitive_exact.contains(&k.as_str())
                || sensitive_patterns.iter().any(|p| k.contains(p));

            if is_sensitive {
                (k, "[REDACTED]".to_string())
            } else {
                (k, v)
            }
        })
        .collect()
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
#[allow(dead_code)]
pub struct E2ePhase {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Performance metrics for tests.
///
/// Captures various performance indicators that can be analyzed post-run.
/// All fields are optional to allow incremental metric capture.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct E2ePerformanceMetrics {
    /// Test/phase duration in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Memory usage in bytes (resident set size).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    /// Peak memory usage in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_memory_bytes: Option<u64>,
    /// Number of file read operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_reads: Option<u64>,
    /// Number of file write operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_writes: Option<u64>,
    /// Bytes read from disk.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_read: Option<u64>,
    /// Bytes written to disk.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
    /// Throughput in items per second (e.g., messages indexed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_per_sec: Option<f64>,
    /// Number of items processed (for throughput calculation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items_processed: Option<u64>,
    /// Network requests made (for browser tests).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_requests: Option<u64>,
    /// Custom metrics as key-value pairs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<std::collections::HashMap<String, serde_json::Value>>,
}

#[allow(dead_code)]
impl E2ePerformanceMetrics {
    /// Create empty metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set duration.
    pub fn with_duration(mut self, ms: u64) -> Self {
        self.duration_ms = Some(ms);
        self
    }

    /// Set memory usage.
    pub fn with_memory(mut self, bytes: u64) -> Self {
        self.memory_bytes = Some(bytes);
        self
    }

    /// Set peak memory usage.
    pub fn with_peak_memory(mut self, bytes: u64) -> Self {
        self.peak_memory_bytes = Some(bytes);
        self
    }

    /// Set throughput metrics.
    pub fn with_throughput(mut self, items: u64, duration_ms: u64) -> Self {
        self.items_processed = Some(items);
        self.duration_ms = Some(duration_ms);
        if duration_ms > 0 {
            self.throughput_per_sec = Some((items as f64) / (duration_ms as f64 / 1000.0));
        }
        self
    }

    /// Set I/O metrics.
    pub fn with_io(mut self, reads: u64, writes: u64, bytes_read: u64, bytes_written: u64) -> Self {
        self.file_reads = Some(reads);
        self.file_writes = Some(writes);
        self.bytes_read = Some(bytes_read);
        self.bytes_written = Some(bytes_written);
        self
    }

    /// Set network requests count.
    pub fn with_network(mut self, requests: u64) -> Self {
        self.network_requests = Some(requests);
        self
    }

    /// Add a custom metric.
    pub fn with_custom(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        if self.custom.is_none() {
            self.custom = Some(std::collections::HashMap::new());
        }
        if let Some(ref mut map) = self.custom {
            map.insert(key.to_string(), value.into());
        }
        self
    }

    /// Capture current process memory usage (best effort).
    ///
    /// Returns the resident set size in bytes on Linux.
    /// Returns None on other platforms or if reading fails.
    pub fn capture_memory() -> Option<u64> {
        // Try to read from /proc/self/statm on Linux
        #[cfg(target_os = "linux")]
        {
            if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
                // statm format: size resident shared text lib data dt (in pages)
                if let Some(resident) = statm.split_whitespace().nth(1)
                    && let Ok(pages) = resident.parse::<u64>()
                {
                    // Page size is typically 4096 bytes
                    return Some(pages * 4096);
                }
            }
        }
        None
    }

    /// Capture current process I/O statistics (best effort).
    ///
    /// Returns (bytes_read, bytes_written) on Linux.
    /// Returns None on other platforms or if reading fails.
    pub fn capture_io() -> Option<(u64, u64)> {
        #[cfg(target_os = "linux")]
        {
            if let Ok(io_content) = std::fs::read_to_string("/proc/self/io") {
                let mut read_bytes = 0u64;
                let mut write_bytes = 0u64;
                for line in io_content.lines() {
                    if let Some(val) = line.strip_prefix("read_bytes: ") {
                        read_bytes = val.trim().parse().unwrap_or(0);
                    } else if let Some(val) = line.strip_prefix("write_bytes: ") {
                        write_bytes = val.trim().parse().unwrap_or(0);
                    }
                }
                return Some((read_bytes, write_bytes));
            }
        }
        None
    }
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
#[allow(dead_code)]
struct BaseEvent {
    ts: String,
    event: String,
    run_id: String,
    runner: String,
}

/// E2E Logger that writes structured JSONL events.
#[allow(dead_code)]
pub struct E2eLogger {
    run_id: String,
    runner: String,
    output_path: PathBuf,
    writer: Arc<Mutex<BufWriter<File>>>,
    env: E2eEnvironment,
}

#[allow(dead_code)]
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
        self.test_end_with_metrics(test, status, duration_ms, retries, error, None)
    }

    /// Emit a test_end event with performance metrics.
    pub fn test_end_with_metrics(
        &self,
        test: &E2eTestInfo,
        status: &str,
        duration_ms: u64,
        retries: Option<u32>,
        error: Option<E2eError>,
        metrics: Option<E2ePerformanceMetrics>,
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
                metrics,
            },
            error,
        };

        self.write_event(&event)
    }

    /// Emit a test_end event for a passing test with performance metrics.
    pub fn test_pass_with_metrics(
        &self,
        test: &E2eTestInfo,
        duration_ms: u64,
        metrics: E2ePerformanceMetrics,
    ) -> std::io::Result<()> {
        self.test_end_with_metrics(test, "pass", duration_ms, None, None, Some(metrics))
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

    /// Emit a metrics event with performance data.
    ///
    /// Use this to log performance metrics for a test or phase.
    /// The `name` parameter identifies what the metrics are for (e.g., "index", "search", "export").
    pub fn metrics(&self, name: &str, metrics: &E2ePerformanceMetrics) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct MetricsEvent {
            ts: String,
            event: String,
            run_id: String,
            runner: String,
            name: String,
            metrics: E2ePerformanceMetrics,
        }

        let event = MetricsEvent {
            ts: Self::iso_timestamp(),
            event: "metrics".to_string(),
            run_id: self.run_id.clone(),
            runner: self.runner.clone(),
            name: name.to_string(),
            metrics: metrics.clone(),
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
            .unwrap_or(chrono::DateTime::UNIX_EPOCH);
        datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    }

    fn timestamp_id() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let datetime = chrono::DateTime::from_timestamp(secs as i64, 0)
            .unwrap_or(chrono::DateTime::UNIX_EPOCH);
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

/// Phase tracker for structured logging in Rust E2E tests.
///
/// Emits test_start/test_end events and provides helpers for phase timing.
#[allow(dead_code)]
pub struct PhaseTracker {
    logger: Option<E2eLogger>,
    test_info: E2eTestInfo,
    start_time: Instant,
    completed: bool,
}

#[allow(dead_code)]
impl PhaseTracker {
    /// Create a new PhaseTracker for the given test.
    pub fn new(suite: &str, test_name: &str) -> Self {
        let logger = if std::env::var("E2E_LOG").is_ok() {
            E2eLogger::new("rust").ok()
        } else {
            None
        };

        let test_info = E2eTestInfo::simple(test_name, suite);

        if let Some(ref lg) = logger {
            let _ = lg.test_start(&test_info);
        }

        Self {
            logger,
            test_info,
            start_time: Instant::now(),
            completed: false,
        }
    }

    /// Execute a phase and log start/end events.
    pub fn phase<F, R>(&self, name: &str, description: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let phase = E2ePhase {
            name: name.to_string(),
            description: Some(description.to_string()),
        };

        if let Some(ref lg) = self.logger {
            let _ = lg.phase_start(&phase);
        }

        let start = Instant::now();
        let result = f();
        let duration_ms = start.elapsed().as_millis() as u64;

        if let Some(ref lg) = self.logger {
            let _ = lg.phase_end(&phase, duration_ms);
        }

        result
    }

    /// Start a phase and return the start time for manual timing.
    pub fn start(&self, name: &str, description: Option<&str>) -> Instant {
        let phase = E2ePhase {
            name: name.to_string(),
            description: description.map(String::from),
        };
        if let Some(ref lg) = self.logger {
            let _ = lg.phase_start(&phase);
        }
        Instant::now()
    }

    /// End a phase, logging duration.
    pub fn end(&self, name: &str, description: Option<&str>, start: Instant) {
        let duration_ms = start.elapsed().as_millis() as u64;
        let phase = E2ePhase {
            name: name.to_string(),
            description: description.map(String::from),
        };
        if let Some(ref lg) = self.logger {
            let _ = lg.phase_end(&phase, duration_ms);
        }
    }

    /// Emit a metrics event.
    pub fn metrics(&self, name: &str, metrics: &E2ePerformanceMetrics) {
        if let Some(ref lg) = self.logger {
            let _ = lg.metrics(name, metrics);
        }
    }

    /// Mark test as completed successfully.
    pub fn complete(mut self) {
        self.completed = true;
        let duration_ms = self.start_time.elapsed().as_millis() as u64;
        if let Some(ref lg) = self.logger {
            let _ = lg.test_end(&self.test_info, "pass", duration_ms, None, None);
            let _ = lg.flush();
        }
    }

    /// Mark test as failed with error.
    pub fn fail(mut self, error: E2eError) {
        self.completed = true;
        let duration_ms = self.start_time.elapsed().as_millis() as u64;
        if let Some(ref lg) = self.logger {
            let _ = lg.test_end(&self.test_info, "fail", duration_ms, None, Some(error));
            let _ = lg.flush();
        }
    }

    /// Flush the logger if present.
    pub fn flush(&self) {
        if let Some(ref lg) = self.logger {
            let _ = lg.flush();
        }
    }
}

impl Drop for PhaseTracker {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Some(ref lg) = self.logger {
            let duration_ms = self.start_time.elapsed().as_millis() as u64;
            let panicking = std::thread::panicking();
            let error = if panicking {
                Some(E2eError::new("panic"))
            } else {
                None
            };
            let status = if panicking { "fail" } else { "pass" };
            let _ = lg.test_end(&self.test_info, status, duration_ms, None, error);
            let _ = lg.flush();
        }
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
    use std::collections::HashMap;
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
                    context: None,
                },
            )
            .unwrap();
        logger.flush().unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("assertion failed"));
        assert!(content.contains("AssertionError"));
        assert!(content.contains("\"status\":\"fail\""));
    }

    // ==================== E2eError builder tests ====================

    #[test]
    fn test_e2e_error_new() {
        let error = E2eError::new("test error");
        assert_eq!(error.message, "test error");
        assert!(error.error_type.is_none());
        assert!(error.stack.is_none());
        assert!(error.context.is_none());
    }

    #[test]
    fn test_e2e_error_with_type() {
        let error = E2eError::with_type("assertion failed", "AssertionError");
        assert_eq!(error.message, "assertion failed");
        assert_eq!(error.error_type, Some("AssertionError".to_string()));
    }

    #[test]
    fn test_e2e_error_with_stack() {
        let error = E2eError::new("error").with_stack("stack trace here");
        assert_eq!(error.stack, Some("stack trace here".to_string()));
    }

    #[test]
    fn test_e2e_error_builder_chain() {
        let context = E2eErrorContext::new()
            .add_state("variable", serde_json::json!("value"))
            .capture_cwd();

        let error = E2eError::with_type("test failure", "TestError")
            .with_stack("at test.rs:42")
            .with_context(context);

        assert_eq!(error.message, "test failure");
        assert_eq!(error.error_type, Some("TestError".to_string()));
        assert_eq!(error.stack, Some("at test.rs:42".to_string()));
        assert!(error.context.is_some());
    }

    // ==================== E2eErrorContext tests ====================

    #[test]
    fn test_error_context_new() {
        let ctx = E2eErrorContext::new();
        assert!(ctx.state.is_none());
        assert!(ctx.screenshot_path.is_none());
        assert!(ctx.env_vars.is_none());
        assert!(ctx.cwd.is_none());
        assert!(ctx.command.is_none());
        assert!(ctx.stdout.is_none());
        assert!(ctx.stderr.is_none());
    }

    #[test]
    fn test_error_context_default() {
        let ctx = E2eErrorContext::default();
        assert!(ctx.state.is_none());
    }

    #[test]
    fn test_error_context_add_state() {
        let ctx = E2eErrorContext::new()
            .add_state("count", serde_json::json!(42))
            .add_state("name", serde_json::json!("test"));

        let state = ctx.state.unwrap();
        assert_eq!(state.get("count"), Some(&serde_json::json!(42)));
        assert_eq!(state.get("name"), Some(&serde_json::json!("test")));
    }

    #[test]
    fn test_error_context_with_state() {
        let mut state = HashMap::new();
        state.insert("phase".to_string(), serde_json::json!("init"));
        state.insert("count".to_string(), serde_json::json!(3));

        let ctx = E2eErrorContext::new().with_state(state.clone());
        assert_eq!(ctx.state, Some(state));
    }

    #[test]
    fn test_error_context_with_screenshot() {
        let ctx = E2eErrorContext::new().with_screenshot("/tmp/failure.png");
        assert_eq!(ctx.screenshot_path, Some("/tmp/failure.png".to_string()));
    }

    #[test]
    fn test_error_context_capture_cwd() {
        let ctx = E2eErrorContext::new().capture_cwd();
        assert!(ctx.cwd.is_some());
        // CWD should be a valid path
        assert!(!ctx.cwd.unwrap().is_empty());
    }

    #[test]
    fn test_error_context_with_command() {
        let ctx = E2eErrorContext::new()
            .with_command("cargo test")
            .with_output(
                Some("test output".to_string()),
                Some("error output".to_string()),
            );

        assert_eq!(ctx.command, Some("cargo test".to_string()));
        assert_eq!(ctx.stdout, Some("test output".to_string()));
        assert_eq!(ctx.stderr, Some("error output".to_string()));
    }

    #[test]
    fn test_error_context_capture_env() {
        let ctx = E2eErrorContext::new().capture_env();
        assert!(ctx.env_vars.is_some());
        // Should have some env vars
        let env = ctx.env_vars.unwrap();
        // PATH is usually present
        assert!(env.contains_key("PATH") || env.contains_key("HOME") || !env.is_empty());
    }

    #[test]
    fn test_error_context_with_env() {
        let mut env = HashMap::new();
        env.insert("E2E_TEST_VAR".to_string(), "value".to_string());
        let ctx = E2eErrorContext::new().with_env(env.clone());
        assert_eq!(ctx.env_vars, Some(env));
    }

    // ==================== capture_sanitized_env tests ====================

    #[test]
    fn test_sanitized_env_redacts_sensitive() {
        // Set a test sensitive env var
        // SAFETY: This test runs in isolation and the env var is cleaned up afterwards
        unsafe {
            std::env::set_var("TEST_SECRET_KEY", "super_secret_value");
        }

        let env = capture_sanitized_env();

        // Check that sensitive keys are redacted
        if let Some(value) = env.get("TEST_SECRET_KEY") {
            assert_eq!(value, "[REDACTED]");
        }

        // Clean up
        // SAFETY: Cleaning up the env var we set above
        unsafe {
            std::env::remove_var("TEST_SECRET_KEY");
        }
    }

    #[test]
    fn test_sanitized_env_preserves_safe() {
        // Set a safe test env var
        // SAFETY: This test runs in isolation and the env var is cleaned up afterwards
        unsafe {
            std::env::set_var("TEST_SAFE_VAR", "safe_value");
        }

        let env = capture_sanitized_env();

        // Safe vars should be preserved
        if let Some(value) = env.get("TEST_SAFE_VAR") {
            assert_eq!(value, "safe_value");
        }

        // Clean up
        // SAFETY: Cleaning up the env var we set above
        unsafe {
            std::env::remove_var("TEST_SAFE_VAR");
        }
    }

    // ==================== Error with context in logger tests ====================

    #[test]
    fn test_logger_error_with_context() {
        let tmp = TempDir::new().unwrap();
        let output_path = tmp.path().join("error_context.jsonl");

        let logger = E2eLogger::with_path("rust", output_path.clone()).unwrap();
        let test_info = E2eTestInfo::simple("context_test", "e2e");

        let context = E2eErrorContext::new()
            .add_state("iteration", serde_json::json!(5))
            .with_command("cargo test")
            .capture_cwd();

        let error = E2eError::with_type("assertion failed", "AssertionError")
            .with_stack("at test.rs:100")
            .with_context(context);

        logger.test_fail(&test_info, 100, None, error).unwrap();
        logger.flush().unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("context"));
        assert!(content.contains("iteration"));
        assert!(content.contains("cargo test"));
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

    // ==================== E2ePerformanceMetrics tests ====================

    #[test]
    fn test_performance_metrics_new() {
        let metrics = E2ePerformanceMetrics::new();
        assert!(metrics.duration_ms.is_none());
        assert!(metrics.memory_bytes.is_none());
        assert!(metrics.custom.is_none());
    }

    #[test]
    fn test_performance_metrics_with_duration() {
        let metrics = E2ePerformanceMetrics::new().with_duration(1000);
        assert_eq!(metrics.duration_ms, Some(1000));
    }

    #[test]
    fn test_performance_metrics_with_memory() {
        let metrics = E2ePerformanceMetrics::new()
            .with_memory(1024 * 1024)
            .with_peak_memory(2 * 1024 * 1024);
        assert_eq!(metrics.memory_bytes, Some(1024 * 1024));
        assert_eq!(metrics.peak_memory_bytes, Some(2 * 1024 * 1024));
    }

    #[test]
    fn test_performance_metrics_with_throughput() {
        let metrics = E2ePerformanceMetrics::new().with_throughput(1000, 2000);
        assert_eq!(metrics.items_processed, Some(1000));
        assert_eq!(metrics.duration_ms, Some(2000));
        assert_eq!(metrics.throughput_per_sec, Some(500.0));
    }

    #[test]
    fn test_performance_metrics_with_io() {
        let metrics = E2ePerformanceMetrics::new().with_io(100, 50, 10240, 5120);
        assert_eq!(metrics.file_reads, Some(100));
        assert_eq!(metrics.file_writes, Some(50));
        assert_eq!(metrics.bytes_read, Some(10240));
        assert_eq!(metrics.bytes_written, Some(5120));
    }

    #[test]
    fn test_performance_metrics_with_network() {
        let metrics = E2ePerformanceMetrics::new().with_network(42);
        assert_eq!(metrics.network_requests, Some(42));
    }

    #[test]
    fn test_performance_metrics_with_custom() {
        let metrics = E2ePerformanceMetrics::new()
            .with_custom("cache_hits", serde_json::json!(99))
            .with_custom("retries", serde_json::json!(3));
        let custom = metrics.custom.unwrap();
        assert_eq!(custom.get("cache_hits"), Some(&serde_json::json!(99)));
        assert_eq!(custom.get("retries"), Some(&serde_json::json!(3)));
    }

    #[test]
    fn test_performance_metrics_builder_chain() {
        let metrics = E2ePerformanceMetrics::new()
            .with_duration(1500)
            .with_memory(2048)
            .with_io(10, 5, 1024, 512)
            .with_custom("index_count", serde_json::json!(100));

        assert_eq!(metrics.duration_ms, Some(1500));
        assert_eq!(metrics.memory_bytes, Some(2048));
        assert_eq!(metrics.file_reads, Some(10));
        assert!(metrics.custom.is_some());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_performance_metrics_capture_memory() {
        let memory = E2ePerformanceMetrics::capture_memory();
        // On Linux, this should succeed
        assert!(memory.is_some());
        assert!(memory.unwrap() > 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_performance_metrics_capture_io() {
        let io = E2ePerformanceMetrics::capture_io();
        // On Linux, this should succeed
        assert!(io.is_some());
    }

    // ==================== Logger with metrics tests ====================

    #[test]
    fn test_logger_test_pass_with_metrics() {
        let tmp = TempDir::new().unwrap();
        let output_path = tmp.path().join("metrics.jsonl");

        let logger = E2eLogger::with_path("rust", output_path.clone()).unwrap();
        let test_info = E2eTestInfo::simple("metrics_test", "e2e");

        let metrics = E2ePerformanceMetrics::new()
            .with_duration(500)
            .with_memory(1024)
            .with_throughput(100, 500);

        logger
            .test_pass_with_metrics(&test_info, 500, metrics)
            .unwrap();
        logger.flush().unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("\"metrics\""));
        assert!(content.contains("\"memory_bytes\":1024"));
        assert!(content.contains("\"throughput_per_sec\":200"));
    }

    #[test]
    fn test_logger_test_end_with_metrics() {
        let tmp = TempDir::new().unwrap();
        let output_path = tmp.path().join("metrics_full.jsonl");

        let logger = E2eLogger::with_path("rust", output_path.clone()).unwrap();
        let test_info = E2eTestInfo::simple("full_metrics_test", "e2e");

        let metrics = E2ePerformanceMetrics::new()
            .with_io(50, 25, 5000, 2500)
            .with_network(10)
            .with_custom("search_latency_p99", serde_json::json!(45.5));

        logger
            .test_end_with_metrics(&test_info, "pass", 1000, None, None, Some(metrics))
            .unwrap();
        logger.flush().unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("\"file_reads\":50"));
        assert!(content.contains("\"network_requests\":10"));
        assert!(content.contains("search_latency_p99"));
    }

    #[test]
    fn test_logger_test_end_without_metrics_still_works() {
        let tmp = TempDir::new().unwrap();
        let output_path = tmp.path().join("no_metrics.jsonl");

        let logger = E2eLogger::with_path("rust", output_path.clone()).unwrap();
        let test_info = E2eTestInfo::simple("no_metrics_test", "e2e");

        // Old API should still work
        logger.test_pass(&test_info, 100, None).unwrap();
        logger.flush().unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("\"status\":\"pass\""));
        // metrics should not be present when not provided
        assert!(!content.contains("\"metrics\""));
    }
}
