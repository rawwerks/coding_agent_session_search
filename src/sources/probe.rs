//! SSH host probing for remote source setup.
//!
//! This module provides functionality to probe SSH hosts and gather comprehensive
//! information needed for remote source configuration decisions:
//! - Whether cass is installed (and what version)
//! - Index status (session count)
//! - Detected agent session data directories
//! - System information (OS, architecture)
//! - Resource availability (disk space, memory)
//!
//! # Design
//!
//! Probing uses a single SSH session per host to minimize latency. A bash probe
//! script is piped to `bash -s` on the remote, gathering all information in one
//! round-trip.
//!
//! # Example
//!
//! ```rust,ignore
//! use coding_agent_search::sources::probe::{probe_host, probe_hosts_parallel};
//! use coding_agent_search::sources::config::DiscoveredHost;
//!
//! // Single host probe
//! let host = DiscoveredHost { name: "laptop".into(), .. };
//! let result = probe_host(&host, 10)?;
//!
//! // Parallel probing with progress
//! let results = probe_hosts_parallel(&hosts, 10, |done, total, name| {
//!     println!("Probing {}/{}: {}", done, total, name);
//! }).await;
//! ```

use std::collections::HashMap;
use std::process::{Command, Stdio};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::config::DiscoveredHost;

/// Default connection timeout in seconds.
pub const DEFAULT_PROBE_TIMEOUT: u64 = 10;

/// Result of probing an SSH host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostProbeResult {
    /// SSH config host alias.
    pub host_name: String,
    /// Whether the host was reachable via SSH.
    pub reachable: bool,
    /// Connection time in milliseconds.
    pub connection_time_ms: u64,
    /// Status of cass installation on the remote.
    pub cass_status: CassStatus,
    /// Detected agent session directories.
    pub detected_agents: Vec<DetectedAgent>,
    /// System information.
    pub system_info: Option<SystemInfo>,
    /// Resource information (disk/memory).
    pub resources: Option<ResourceInfo>,
    /// Error message if probe failed.
    pub error: Option<String>,
}

impl HostProbeResult {
    /// Create a result for an unreachable host.
    pub fn unreachable(host_name: &str, error: impl Into<String>) -> Self {
        Self {
            host_name: host_name.to_string(),
            reachable: false,
            connection_time_ms: 0,
            cass_status: CassStatus::Unknown,
            detected_agents: Vec::new(),
            system_info: None,
            resources: None,
            error: Some(error.into()),
        }
    }

    /// Check if cass is installed on this host.
    pub fn has_cass(&self) -> bool {
        self.cass_status.is_installed()
    }

    /// Check if this host has any agent session data.
    pub fn has_agent_data(&self) -> bool {
        !self.detected_agents.is_empty()
    }

    /// Get total estimated sessions across all detected agents.
    pub fn total_sessions(&self) -> u64 {
        self.detected_agents
            .iter()
            .filter_map(|a| a.estimated_sessions)
            .sum()
    }
}

/// Status of cass installation on a remote host.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CassStatus {
    /// cass is installed and has an indexed database.
    Indexed {
        version: String,
        session_count: u64,
        last_indexed: Option<String>,
    },
    /// cass is installed but no index exists or is empty.
    InstalledNotIndexed { version: String },
    /// cass is not found on PATH.
    NotFound,
    /// Couldn't determine cass status.
    Unknown,
}

impl CassStatus {
    /// Check if cass is installed (any version).
    pub fn is_installed(&self) -> bool {
        matches!(
            self,
            CassStatus::Indexed { .. } | CassStatus::InstalledNotIndexed { .. }
        )
    }

    /// Get the installed version if available.
    pub fn version(&self) -> Option<&str> {
        match self {
            CassStatus::Indexed { version, .. } | CassStatus::InstalledNotIndexed { version } => {
                Some(version)
            }
            _ => None,
        }
    }
}

/// Detected agent session data on a remote host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedAgent {
    /// Type of agent (claude_code, codex, cursor, etc.).
    pub agent_type: String,
    /// Path to the agent's session directory.
    pub path: String,
    /// Estimated number of sessions (from file count).
    pub estimated_sessions: Option<u64>,
    /// Estimated size in megabytes.
    pub estimated_size_mb: Option<u64>,
}

/// System information gathered from remote host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Operating system (linux, darwin).
    pub os: String,
    /// CPU architecture (x86_64, aarch64).
    pub arch: String,
    /// Linux distro name if available.
    pub distro: Option<String>,
    /// Whether cargo is available.
    pub has_cargo: bool,
    /// Whether cargo-binstall is available.
    pub has_cargo_binstall: bool,
    /// Whether curl is available.
    pub has_curl: bool,
    /// Whether wget is available.
    pub has_wget: bool,
    /// Remote home directory.
    pub remote_home: String,
}

/// Resource information for installation feasibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceInfo {
    /// Available disk space in MB (in home directory).
    pub disk_available_mb: u64,
    /// Total memory in MB.
    pub memory_total_mb: u64,
    /// Available memory in MB.
    pub memory_available_mb: u64,
    /// Heuristic: enough resources to compile Rust.
    pub can_compile: bool,
}

impl ResourceInfo {
    /// Minimum disk space (MB) recommended for cass installation.
    pub const MIN_DISK_MB: u64 = 1024; // 1 GB

    /// Minimum memory (MB) recommended for compilation.
    pub const MIN_MEMORY_MB: u64 = 2048; // 2 GB
}

/// Bash probe script that gathers all information in one SSH call.
///
/// Output format is key=value pairs, with special markers for sections.
const PROBE_SCRIPT: &str = r#"#!/bin/bash
echo "===PROBE_START==="

# System info
echo "OS=$(uname -s | tr '[:upper:]' '[:lower:]')"
echo "ARCH=$(uname -m)"
echo "HOME=$HOME"

# Distro detection (Linux only)
if [ -f /etc/os-release ]; then
    . /etc/os-release
    echo "DISTRO=$PRETTY_NAME"
fi

# Cass status
if command -v cass &> /dev/null; then
    CASS_VER=$(cass --version 2>/dev/null | head -1 | awk '{print $2}')
    echo "CASS_VERSION=$CASS_VER"

    # Get health status (JSON output)
    HEALTH=$(cass health --json 2>/dev/null)
    if [ $? -eq 0 ]; then
        echo "CASS_HEALTH=OK"
        # Try to get session count from stats
        STATS=$(cass stats --json 2>/dev/null)
        if [ $? -eq 0 ]; then
            # Extract total conversations from JSON
            SESSIONS=$(echo "$STATS" | grep -o '"total_conversations":[0-9]*' | cut -d: -f2)
            echo "CASS_SESSIONS=${SESSIONS:-0}"
        fi
    else
        echo "CASS_HEALTH=NOT_INDEXED"
    fi
else
    echo "CASS_VERSION=NOT_FOUND"
fi

# Tool availability
command -v cargo &> /dev/null && echo "HAS_CARGO=1" || echo "HAS_CARGO=0"
command -v cargo-binstall &> /dev/null && echo "HAS_BINSTALL=1" || echo "HAS_BINSTALL=0"
command -v curl &> /dev/null && echo "HAS_CURL=1" || echo "HAS_CURL=0"
command -v wget &> /dev/null && echo "HAS_WGET=1" || echo "HAS_WGET=0"

# Resource info - disk (in KB, converted later)
DISK_KB=$(df -k ~ 2>/dev/null | awk 'NR==2 {print $4}')
echo "DISK_AVAIL_KB=${DISK_KB:-0}"

# Memory info (Linux)
if [ -f /proc/meminfo ]; then
    MEM_TOTAL=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}')
    MEM_AVAIL=$(grep MemAvailable /proc/meminfo 2>/dev/null | awk '{print $2}')
    echo "MEM_TOTAL_KB=${MEM_TOTAL:-0}"
    echo "MEM_AVAIL_KB=${MEM_AVAIL:-0}"
else
    # macOS - use sysctl
    if command -v sysctl &> /dev/null; then
        MEM_BYTES=$(sysctl -n hw.memsize 2>/dev/null)
        MEM_KB=$((MEM_BYTES / 1024))
        echo "MEM_TOTAL_KB=${MEM_KB:-0}"
        echo "MEM_AVAIL_KB=${MEM_KB:-0}"  # macOS doesn't have easy available mem
    fi
fi

# Agent data detection (with sizes and file counts)
for dir in ~/.claude/projects ~/.codex/sessions ~/.cursor \
           ~/.config/Code/User/globalStorage/saoudrizwan.claude-dev \
           ~/.config/Cursor/User/globalStorage/saoudrizwan.claude-dev \
           ~/Library/Application\ Support/Code/User/globalStorage/saoudrizwan.claude-dev \
           ~/Library/Application\ Support/Cursor/User/globalStorage/saoudrizwan.claude-dev \
           ~/.gemini/tmp ~/.pi/agent/sessions ~/.aider.chat.history.md \
           ~/.local/share/opencode ~/.goose/sessions ~/.continue/sessions; do
    # Expand the path
    expanded_dir=$(eval echo "$dir" 2>/dev/null)
    if [ -e "$expanded_dir" ]; then
        SIZE=$(du -sm "$expanded_dir" 2>/dev/null | cut -f1)
        # Count JSONL files for session estimate
        if [ -d "$expanded_dir" ]; then
            COUNT=$(find "$expanded_dir" -name "*.jsonl" -o -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
        else
            COUNT=1  # Single file
        fi
        echo "AGENT_DATA=$dir|${SIZE:-0}|${COUNT:-0}"
    fi
done

echo "===PROBE_END==="
"#;

/// Probe a single SSH host.
///
/// Runs a comprehensive probe script via SSH to gather system info, cass status,
/// and detected agent data. Uses a single SSH session for efficiency.
///
/// # Arguments
/// * `host` - The discovered SSH host to probe
/// * `timeout_secs` - Connection timeout in seconds
///
/// # Returns
/// A `HostProbeResult` with all gathered information, or error details if probe failed.
pub fn probe_host(host: &DiscoveredHost, timeout_secs: u64) -> HostProbeResult {
    let start = Instant::now();

    // Build SSH command with appropriate options
    let ssh_opts = format!(
        "-o BatchMode=yes -o ConnectTimeout={} -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null",
        timeout_secs
    );

    // Use the host alias directly (SSH config handles Port, User, IdentityFile, ProxyJump, etc.)
    let mut cmd = Command::new("ssh");
    cmd.args(ssh_opts.split_whitespace())
        .arg(&host.name)
        .arg("bash -s")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Spawn the process and write probe script to stdin
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return HostProbeResult::unreachable(
                &host.name,
                format!("Failed to execute ssh: {}", e),
            );
        }
    };

    // Write probe script to stdin
    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write;
        if let Err(e) = stdin.write_all(PROBE_SCRIPT.as_bytes()) {
            return HostProbeResult::unreachable(
                &host.name,
                format!("Failed to write probe script: {}", e),
            );
        }
    }

    // Wait for completion
    let output = match child.wait_with_output() {
        Ok(o) => o,
        Err(e) => {
            return HostProbeResult::unreachable(&host.name, format!("SSH command failed: {}", e));
        }
    };

    let connection_time_ms = start.elapsed().as_millis() as u64;

    // Check for SSH failures
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let error_msg = if stderr.contains("Connection refused") {
            "Connection refused".to_string()
        } else if stderr.contains("Connection timed out") || stderr.contains("timed out") {
            "Connection timed out".to_string()
        } else if stderr.contains("Permission denied") {
            "Permission denied (key not loaded in ssh-agent?)".to_string()
        } else if stderr.contains("Host key verification failed") {
            "Host key verification failed".to_string()
        } else if stderr.contains("No route to host") {
            "No route to host".to_string()
        } else {
            format!("SSH failed: {}", stderr.trim())
        };

        return HostProbeResult::unreachable(&host.name, error_msg);
    }

    // Parse successful output
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_probe_output(&host.name, &stdout, connection_time_ms)
}

/// Parse the probe script output into a HostProbeResult.
fn parse_probe_output(host_name: &str, output: &str, connection_time_ms: u64) -> HostProbeResult {
    let mut values: HashMap<String, String> = HashMap::new();
    let mut agent_data: Vec<(String, u64, u64)> = Vec::new(); // (path, size_mb, count)

    // Check for probe markers
    if !output.contains("===PROBE_START===") || !output.contains("===PROBE_END===") {
        return HostProbeResult::unreachable(host_name, "Probe script output malformed");
    }

    // Parse key=value pairs
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("===") {
            continue;
        }

        if line.starts_with("AGENT_DATA=") {
            // Special handling for agent data: AGENT_DATA=path|size|count
            if let Some(data) = line.strip_prefix("AGENT_DATA=") {
                let parts: Vec<&str> = data.split('|').collect();
                if parts.len() >= 3 {
                    let path = parts[0].to_string();
                    let size = parts[1].parse().unwrap_or(0);
                    let count = parts[2].parse().unwrap_or(0);
                    agent_data.push((path, size, count));
                }
            }
        } else if let Some((key, value)) = line.split_once('=') {
            values.insert(key.to_string(), value.to_string());
        }
    }

    // Build CassStatus
    let cass_status = if let Some(version) = values.get("CASS_VERSION") {
        if version == "NOT_FOUND" {
            CassStatus::NotFound
        } else {
            let health = values.get("CASS_HEALTH").map(|s| s.as_str());
            if health == Some("OK") {
                let sessions = values
                    .get("CASS_SESSIONS")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                CassStatus::Indexed {
                    version: version.clone(),
                    session_count: sessions,
                    last_indexed: None,
                }
            } else {
                CassStatus::InstalledNotIndexed {
                    version: version.clone(),
                }
            }
        }
    } else {
        CassStatus::Unknown
    };

    // Build SystemInfo
    let system_info = values.get("OS").map(|os| SystemInfo {
        os: os.clone(),
        arch: values.get("ARCH").cloned().unwrap_or_default(),
        distro: values.get("DISTRO").cloned(),
        has_cargo: values.get("HAS_CARGO").map(|v| v == "1").unwrap_or(false),
        has_cargo_binstall: values
            .get("HAS_BINSTALL")
            .map(|v| v == "1")
            .unwrap_or(false),
        has_curl: values.get("HAS_CURL").map(|v| v == "1").unwrap_or(false),
        has_wget: values.get("HAS_WGET").map(|v| v == "1").unwrap_or(false),
        remote_home: values.get("HOME").cloned().unwrap_or_default(),
    });

    // Build ResourceInfo
    let resources = {
        let disk_kb = values
            .get("DISK_AVAIL_KB")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let mem_total_kb = values
            .get("MEM_TOTAL_KB")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let mem_avail_kb = values
            .get("MEM_AVAIL_KB")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        if disk_kb > 0 || mem_total_kb > 0 {
            let disk_mb = disk_kb / 1024;
            let mem_total_mb = mem_total_kb / 1024;
            let mem_avail_mb = mem_avail_kb / 1024;

            Some(ResourceInfo {
                disk_available_mb: disk_mb,
                memory_total_mb: mem_total_mb,
                memory_available_mb: mem_avail_mb,
                can_compile: disk_mb >= ResourceInfo::MIN_DISK_MB
                    && mem_total_mb >= ResourceInfo::MIN_MEMORY_MB,
            })
        } else {
            None
        }
    };

    // Build DetectedAgents
    let detected_agents: Vec<DetectedAgent> = agent_data
        .into_iter()
        .map(|(path, size_mb, count)| {
            let agent_type = infer_agent_type(&path);
            DetectedAgent {
                agent_type,
                path,
                estimated_sessions: Some(count),
                estimated_size_mb: Some(size_mb),
            }
        })
        .collect();

    HostProbeResult {
        host_name: host_name.to_string(),
        reachable: true,
        connection_time_ms,
        cass_status,
        detected_agents,
        system_info,
        resources,
        error: None,
    }
}

/// Infer agent type from path.
///
/// Note: More specific patterns must be checked first (e.g., `saoudrizwan.claude-dev`
/// contains `claude` so Cline must be checked before Claude Code).
fn infer_agent_type(path: &str) -> String {
    // Check Cline first - it contains "claude-dev" which could match ".claude"
    if path.contains("saoudrizwan.claude-dev") || path.contains("rooveterinaryinc.roo-cline") {
        "cline".to_string()
    } else if path.contains(".claude") {
        "claude_code".to_string()
    } else if path.contains(".codex") {
        "codex".to_string()
    } else if path.contains(".cursor") || path.contains("Cursor") {
        "cursor".to_string()
    } else if path.contains(".gemini") {
        "gemini".to_string()
    } else if path.contains(".pi") {
        "pi_agent".to_string()
    } else if path.contains(".aider") {
        "aider".to_string()
    } else if path.contains("opencode") {
        "opencode".to_string()
    } else if path.contains(".goose") {
        "goose".to_string()
    } else if path.contains(".continue") {
        "continue".to_string()
    } else {
        "unknown".to_string()
    }
}

/// Probe multiple hosts in parallel.
///
/// Uses rayon's parallel iterator to probe hosts concurrently, calling the
/// progress callback as each probe completes.
///
/// # Arguments
/// * `hosts` - Slice of discovered hosts to probe
/// * `timeout_secs` - Connection timeout per host
/// * `on_progress` - Callback called after each host completes: (completed, total, host_name)
///
/// # Returns
/// Vector of probe results for all hosts.
pub fn probe_hosts_parallel<F>(
    hosts: &[DiscoveredHost],
    timeout_secs: u64,
    on_progress: F,
) -> Vec<HostProbeResult>
where
    F: Fn(usize, usize, &str) + Send + Sync,
{
    use rayon::prelude::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let total = hosts.len();
    let completed = Arc::new(AtomicUsize::new(0));
    let on_progress = Arc::new(on_progress);

    // Use rayon for true parallel execution
    hosts
        .par_iter()
        .map(|host| {
            let result = probe_host(host, timeout_secs);

            let done = completed.fetch_add(1, Ordering::SeqCst) + 1;
            on_progress(done, total, &host.name);

            result
        })
        .collect()
}

/// Cache for probe results to avoid repeated probing.
#[derive(Debug, Default)]
pub struct ProbeCache {
    results: HashMap<String, (HostProbeResult, std::time::Instant)>,
    ttl_secs: u64,
}

impl ProbeCache {
    /// Create a new cache with the specified TTL in seconds.
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            results: HashMap::new(),
            ttl_secs,
        }
    }

    /// Get a cached result if still valid.
    pub fn get(&self, host_name: &str) -> Option<&HostProbeResult> {
        self.results.get(host_name).and_then(|(result, ts)| {
            if ts.elapsed().as_secs() < self.ttl_secs {
                Some(result)
            } else {
                None
            }
        })
    }

    /// Insert a result into the cache.
    pub fn insert(&mut self, result: HostProbeResult) {
        self.results.insert(
            result.host_name.clone(),
            (result, std::time::Instant::now()),
        );
    }

    /// Clear expired entries.
    pub fn clear_expired(&mut self) {
        self.results
            .retain(|_, (_, ts)| ts.elapsed().as_secs() < self.ttl_secs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cass_status_is_installed() {
        assert!(
            CassStatus::Indexed {
                version: "0.1.50".into(),
                session_count: 100,
                last_indexed: None
            }
            .is_installed()
        );

        assert!(
            CassStatus::InstalledNotIndexed {
                version: "0.1.50".into()
            }
            .is_installed()
        );

        assert!(!CassStatus::NotFound.is_installed());
        assert!(!CassStatus::Unknown.is_installed());
    }

    #[test]
    fn test_cass_status_version() {
        assert_eq!(
            CassStatus::Indexed {
                version: "0.1.50".into(),
                session_count: 0,
                last_indexed: None
            }
            .version(),
            Some("0.1.50")
        );

        assert_eq!(
            CassStatus::InstalledNotIndexed {
                version: "0.1.49".into()
            }
            .version(),
            Some("0.1.49")
        );

        assert_eq!(CassStatus::NotFound.version(), None);
    }

    #[test]
    fn test_infer_agent_type() {
        assert_eq!(infer_agent_type("~/.claude/projects"), "claude_code");
        assert_eq!(infer_agent_type("~/.codex/sessions"), "codex");
        assert_eq!(infer_agent_type("~/.cursor"), "cursor");
        assert_eq!(infer_agent_type("~/.gemini/tmp"), "gemini");
        assert_eq!(
            infer_agent_type("~/.config/Code/User/globalStorage/saoudrizwan.claude-dev"),
            "cline"
        );
        assert_eq!(infer_agent_type("/some/random/path"), "unknown");
    }

    #[test]
    fn test_parse_probe_output_success() {
        let output = r#"
===PROBE_START===
OS=linux
ARCH=x86_64
HOME=/home/user
DISTRO=Ubuntu 22.04
CASS_VERSION=0.1.50
CASS_HEALTH=OK
CASS_SESSIONS=1234
HAS_CARGO=1
HAS_BINSTALL=0
HAS_CURL=1
HAS_WGET=1
DISK_AVAIL_KB=52428800
MEM_TOTAL_KB=16777216
MEM_AVAIL_KB=8388608
AGENT_DATA=~/.claude/projects|150|42
AGENT_DATA=~/.codex/sessions|50|10
===PROBE_END===
"#;

        let result = parse_probe_output("test-host", output, 100);

        assert!(result.reachable);
        assert_eq!(result.host_name, "test-host");
        assert_eq!(result.connection_time_ms, 100);

        // Check cass status
        match &result.cass_status {
            CassStatus::Indexed {
                version,
                session_count,
                ..
            } => {
                assert_eq!(version, "0.1.50");
                assert_eq!(*session_count, 1234);
            }
            _ => panic!("Expected Indexed status"),
        }

        // Check system info
        let sys = result.system_info.as_ref().unwrap();
        assert_eq!(sys.os, "linux");
        assert_eq!(sys.arch, "x86_64");
        assert_eq!(sys.distro, Some("Ubuntu 22.04".into()));
        assert!(sys.has_cargo);
        assert!(!sys.has_cargo_binstall);
        assert!(sys.has_curl);

        // Check resources
        let res = result.resources.as_ref().unwrap();
        assert_eq!(res.disk_available_mb, 51200); // 52428800 / 1024
        assert_eq!(res.memory_total_mb, 16384); // 16777216 / 1024
        assert!(res.can_compile);

        // Check detected agents
        assert_eq!(result.detected_agents.len(), 2);
        assert_eq!(result.detected_agents[0].agent_type, "claude_code");
        assert_eq!(result.detected_agents[0].estimated_sessions, Some(42));
        assert_eq!(result.detected_agents[1].agent_type, "codex");
    }

    #[test]
    fn test_parse_probe_output_cass_not_found() {
        let output = r#"
===PROBE_START===
OS=darwin
ARCH=arm64
HOME=/Users/user
CASS_VERSION=NOT_FOUND
HAS_CARGO=0
HAS_BINSTALL=0
HAS_CURL=1
HAS_WGET=0
DISK_AVAIL_KB=10240000
MEM_TOTAL_KB=8388608
MEM_AVAIL_KB=4194304
===PROBE_END===
"#;

        let result = parse_probe_output("mac-host", output, 50);

        assert!(result.reachable);
        assert!(matches!(result.cass_status, CassStatus::NotFound));

        let sys = result.system_info.as_ref().unwrap();
        assert_eq!(sys.os, "darwin");
        assert_eq!(sys.arch, "arm64");
        assert!(!sys.has_cargo);
    }

    #[test]
    fn test_parse_probe_output_malformed() {
        let output = "random garbage";
        let result = parse_probe_output("bad-host", output, 0);

        assert!(!result.reachable);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_host_probe_result_unreachable() {
        let result = HostProbeResult::unreachable("test", "Connection refused");

        assert!(!result.reachable);
        assert_eq!(result.error, Some("Connection refused".into()));
        assert!(!result.has_cass());
        assert!(!result.has_agent_data());
    }

    #[test]
    fn test_probe_cache() {
        let mut cache = ProbeCache::new(300); // 5 minute TTL

        let result = HostProbeResult {
            host_name: "test".into(),
            reachable: true,
            connection_time_ms: 100,
            cass_status: CassStatus::NotFound,
            detected_agents: vec![],
            system_info: None,
            resources: None,
            error: None,
        };

        cache.insert(result);

        assert!(cache.get("test").is_some());
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_resource_info_can_compile() {
        let good = ResourceInfo {
            disk_available_mb: 2000,
            memory_total_mb: 4000,
            memory_available_mb: 2000,
            can_compile: true,
        };
        assert!(good.can_compile);

        let low_disk = ResourceInfo {
            disk_available_mb: 500,
            memory_total_mb: 4000,
            memory_available_mb: 2000,
            can_compile: false,
        };
        assert!(!low_disk.can_compile);
    }
}
