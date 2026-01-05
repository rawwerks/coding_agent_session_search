//! Remote cass installation via SSH.
//!
//! This module provides functionality to automatically install cass on remote
//! machines via SSH. It supports multiple installation methods with intelligent
//! fallback and robust handling of long-running installations.
//!
//! # Installation Methods (Priority Order)
//!
//! 1. **Cargo Binstall** (fastest if available) - downloads pre-built binary via cargo
//! 2. **Pre-built Binary** - direct binary download from GitHub releases
//! 3. **Cargo Install** - compile from source (most reliable fallback)
//! 4. **Full Bootstrap** - install rustup first, then compile
//!
//! # Example
//!
//! ```rust,ignore
//! use coding_agent_search::sources::install::{RemoteInstaller, InstallProgress};
//! use coding_agent_search::sources::probe::{SystemInfo, ResourceInfo};
//!
//! let installer = RemoteInstaller::new("laptop", system_info, resources);
//!
//! installer.install(|progress| {
//!     println!("{}: {}", progress.stage, progress.message);
//! })?;
//! ```

use std::io::Write as IoWrite;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::probe::{ResourceInfo, SystemInfo};

// =============================================================================
// Constants
// =============================================================================

/// Default SSH connection timeout for installation commands.
pub const DEFAULT_INSTALL_TIMEOUT_SECS: u64 = 600; // 10 minutes for cargo install

/// Minimum disk space required for installation (MB).
pub const MIN_DISK_MB: u64 = 2048; // 2 GB

/// Minimum memory recommended for compilation (MB).
pub const MIN_MEMORY_MB: u64 = 1024; // 1 GB

/// Current cass version for installation.
pub const CASS_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Package name on crates.io.
pub const CRATE_NAME: &str = "coding-agent-search";

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during remote installation.
#[derive(Error, Debug)]
pub enum InstallError {
    #[error("SSH connection failed: {0}")]
    SshFailed(String),

    #[error("SSH connection timed out after {0} seconds")]
    Timeout(u64),

    #[error("Insufficient disk space: {available_mb}MB available, {required_mb}MB required")]
    InsufficientDisk { available_mb: u64, required_mb: u64 },

    #[error("Insufficient memory: {available_mb}MB available, {required_mb}MB recommended")]
    InsufficientMemory { available_mb: u64, required_mb: u64 },

    #[error("Installation method {method} failed: {reason}")]
    MethodFailed { method: String, reason: String },

    #[error("No suitable installation method available")]
    NoMethodAvailable,

    #[error("Verification failed: {0}")]
    VerificationFailed(String),

    #[error("Missing system dependency: {dep}. Fix: {fix}")]
    MissingDependency { dep: String, fix: String },

    #[error("Installation cancelled")]
    Cancelled,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// =============================================================================
// Install Method Types
// =============================================================================

/// Installation method for cass.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum InstallMethod {
    /// Install via cargo-binstall (fastest, downloads pre-built binary).
    CargoBinstall,

    /// Download pre-built binary directly from GitHub releases.
    PrebuiltBinary {
        url: String,
        checksum: Option<String>,
    },

    /// Compile from source via cargo install.
    CargoInstall,

    /// Full bootstrap: install rustup first, then compile.
    FullBootstrap,
}

impl InstallMethod {
    /// Get display name for the method.
    pub fn display_name(&self) -> &'static str {
        match self {
            InstallMethod::CargoBinstall => "cargo-binstall",
            InstallMethod::PrebuiltBinary { .. } => "pre-built binary",
            InstallMethod::CargoInstall => "cargo install",
            InstallMethod::FullBootstrap => "full bootstrap (rustup + cargo)",
        }
    }

    /// Estimated time for this method.
    pub fn estimated_time(&self) -> Duration {
        match self {
            InstallMethod::CargoBinstall => Duration::from_secs(30),
            InstallMethod::PrebuiltBinary { .. } => Duration::from_secs(10),
            InstallMethod::CargoInstall => Duration::from_secs(300), // 5 minutes
            InstallMethod::FullBootstrap => Duration::from_secs(600), // 10 minutes
        }
    }

    /// Whether this method requires compilation.
    pub fn requires_compilation(&self) -> bool {
        matches!(
            self,
            InstallMethod::CargoInstall | InstallMethod::FullBootstrap
        )
    }
}

impl std::fmt::Display for InstallMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

// =============================================================================
// Progress Types
// =============================================================================

/// Current stage of installation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstallStage {
    /// Preparing installation (checking resources, selecting method).
    Preparing,
    /// Downloading files.
    Downloading,
    /// Compiling code.
    Compiling { crate_name: String },
    /// Installing binary.
    Installing,
    /// Verifying installation.
    Verifying,
    /// Installation complete.
    Complete,
    /// Installation failed.
    Failed { error: String },
}

impl std::fmt::Display for InstallStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstallStage::Preparing => write!(f, "Preparing"),
            InstallStage::Downloading => write!(f, "Downloading"),
            InstallStage::Compiling { crate_name } => write!(f, "Compiling {}", crate_name),
            InstallStage::Installing => write!(f, "Installing"),
            InstallStage::Verifying => write!(f, "Verifying"),
            InstallStage::Complete => write!(f, "Complete"),
            InstallStage::Failed { error } => write!(f, "Failed: {}", error),
        }
    }
}

/// Progress update during installation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallProgress {
    /// Current stage.
    pub stage: InstallStage,
    /// Human-readable message.
    pub message: String,
    /// Optional progress percentage (0-100).
    pub percent: Option<u8>,
    /// Elapsed time since start.
    pub elapsed: Duration,
}

/// Result of a successful installation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallResult {
    /// Method used for installation.
    pub method: InstallMethod,
    /// Installed version.
    pub version: String,
    /// Total installation time.
    pub duration: Duration,
    /// Installation path.
    pub install_path: Option<String>,
}

// =============================================================================
// RemoteInstaller
// =============================================================================

/// Installer for cass on remote machines.
pub struct RemoteInstaller {
    /// SSH host alias.
    host: String,
    /// System information from probe.
    system_info: SystemInfo,
    /// Resource information from probe.
    resources: ResourceInfo,
    /// Target version to install.
    target_version: String,
}

impl RemoteInstaller {
    /// Create a new installer for a remote host.
    pub fn new(host: impl Into<String>, system_info: SystemInfo, resources: ResourceInfo) -> Self {
        Self {
            host: host.into(),
            system_info,
            resources,
            target_version: CASS_VERSION.to_string(),
        }
    }

    /// Create an installer with a specific target version.
    pub fn with_version(
        host: impl Into<String>,
        system_info: SystemInfo,
        resources: ResourceInfo,
        version: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            system_info,
            resources,
            target_version: version.into(),
        }
    }

    /// Get the host name.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get the target version.
    pub fn target_version(&self) -> &str {
        &self.target_version
    }

    /// Check if resources are sufficient for compilation.
    pub fn check_resources(&self) -> Result<(), InstallError> {
        if self.resources.disk_available_mb < MIN_DISK_MB {
            return Err(InstallError::InsufficientDisk {
                available_mb: self.resources.disk_available_mb,
                required_mb: MIN_DISK_MB,
            });
        }
        // Only check memory if compilation is needed
        // Note: we check during method selection
        Ok(())
    }

    /// Check if resources are sufficient for compilation specifically.
    pub fn can_compile(&self) -> Result<(), InstallError> {
        self.check_resources()?;
        if self.resources.memory_total_mb < MIN_MEMORY_MB {
            return Err(InstallError::InsufficientMemory {
                available_mb: self.resources.memory_total_mb,
                required_mb: MIN_MEMORY_MB,
            });
        }
        Ok(())
    }

    /// Choose the best installation method based on system info.
    ///
    /// Returns `None` if no viable installation method is available.
    pub fn choose_method(&self) -> Option<InstallMethod> {
        // 1. Try cargo-binstall first (fastest)
        if self.system_info.has_cargo_binstall {
            return Some(InstallMethod::CargoBinstall);
        }

        // 2. Try pre-built binary if available for this arch
        if let Some(url) = self.get_prebuilt_url() {
            return Some(InstallMethod::PrebuiltBinary {
                url,
                checksum: None, // TODO: Add checksum support
            });
        }

        // 3. Try cargo install if cargo is available and we have resources
        if self.system_info.has_cargo && self.can_compile().is_ok() {
            return Some(InstallMethod::CargoInstall);
        }

        // 4. Full bootstrap requires curl to download rustup
        if self.system_info.has_curl {
            return Some(InstallMethod::FullBootstrap);
        }

        // No viable method available
        None
    }

    /// Get pre-built binary URL if available for this architecture.
    fn get_prebuilt_url(&self) -> Option<String> {
        // Only supported if we have a way to download
        if !self.system_info.has_curl && !self.system_info.has_wget {
            return None;
        }

        // Map arch to release asset naming
        let arch = match self.system_info.arch.as_str() {
            "x86_64" => "x86_64",
            "aarch64" | "arm64" => "aarch64",
            _ => return None, // Unsupported arch
        };

        let os = match self.system_info.os.to_lowercase().as_str() {
            "linux" => "linux",
            "darwin" => "macos",
            _ => return None, // Unsupported OS
        };

        // GitHub releases URL pattern
        Some(format!(
            "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v{}/cass-{}-{}",
            self.target_version, os, arch
        ))
    }

    /// Install cass on the remote host.
    ///
    /// Streams progress updates via the callback as installation proceeds.
    pub fn install<F>(&self, on_progress: F) -> Result<InstallResult, InstallError>
    where
        F: Fn(InstallProgress) + Send + Sync,
    {
        let start = Instant::now();

        // Check resources
        on_progress(InstallProgress {
            stage: InstallStage::Preparing,
            message: "Checking system resources...".into(),
            percent: Some(0),
            elapsed: start.elapsed(),
        });

        self.check_resources()?;

        // Choose method
        let method = self.choose_method().ok_or(InstallError::NoMethodAvailable)?;

        on_progress(InstallProgress {
            stage: InstallStage::Preparing,
            message: format!("Selected installation method: {}", method),
            percent: Some(5),
            elapsed: start.elapsed(),
        });

        // Execute installation
        let result = match &method {
            InstallMethod::CargoBinstall => self.install_via_binstall(&on_progress, start),
            InstallMethod::PrebuiltBinary { url, checksum } => {
                self.install_via_binary(url, checksum.as_deref(), &on_progress, start)
            }
            InstallMethod::CargoInstall => self.install_via_cargo(&on_progress, start),
            InstallMethod::FullBootstrap => self.install_with_bootstrap(&on_progress, start),
        };

        match result {
            Ok(install_result) => {
                on_progress(InstallProgress {
                    stage: InstallStage::Complete,
                    message: format!(
                        "Installed cass {} via {} in {:.1}s",
                        install_result.version,
                        method,
                        install_result.duration.as_secs_f64()
                    ),
                    percent: Some(100),
                    elapsed: start.elapsed(),
                });
                Ok(install_result)
            }
            Err(e) => {
                on_progress(InstallProgress {
                    stage: InstallStage::Failed {
                        error: e.to_string(),
                    },
                    message: format!("Installation failed: {}", e),
                    percent: None,
                    elapsed: start.elapsed(),
                });
                Err(e)
            }
        }
    }

    /// Install via cargo-binstall.
    fn install_via_binstall<F>(
        &self,
        on_progress: &F,
        start: Instant,
    ) -> Result<InstallResult, InstallError>
    where
        F: Fn(InstallProgress),
    {
        on_progress(InstallProgress {
            stage: InstallStage::Downloading,
            message: "Running cargo binstall...".into(),
            percent: Some(10),
            elapsed: start.elapsed(),
        });

        let script = format!(
            r#"cargo binstall --no-confirm {}@{}"#,
            CRATE_NAME, self.target_version
        );

        self.run_ssh_command(&script, Duration::from_secs(120))?;

        // Verify installation
        self.verify_installation(on_progress, start)?;

        Ok(InstallResult {
            method: InstallMethod::CargoBinstall,
            version: self.target_version.clone(),
            duration: start.elapsed(),
            install_path: Some("~/.cargo/bin/cass".into()),
        })
    }

    /// Install via pre-built binary download.
    fn install_via_binary<F>(
        &self,
        url: &str,
        _checksum: Option<&str>,
        on_progress: &F,
        start: Instant,
    ) -> Result<InstallResult, InstallError>
    where
        F: Fn(InstallProgress),
    {
        on_progress(InstallProgress {
            stage: InstallStage::Downloading,
            message: "Downloading pre-built binary...".into(),
            percent: Some(10),
            elapsed: start.elapsed(),
        });

        // Use curl or wget depending on availability
        let download_cmd = if self.system_info.has_curl {
            format!(
                r#"
mkdir -p ~/.local/bin
curl -fsSL "{}" -o ~/.local/bin/cass
chmod +x ~/.local/bin/cass
# Add to PATH only if not already present
grep -q '.local/bin' ~/.bashrc 2>/dev/null || echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
"#,
                url
            )
        } else {
            format!(
                r#"
mkdir -p ~/.local/bin
wget -q "{}" -O ~/.local/bin/cass
chmod +x ~/.local/bin/cass
# Add to PATH only if not already present
grep -q '.local/bin' ~/.bashrc 2>/dev/null || echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
"#,
                url
            )
        };

        self.run_ssh_command(&download_cmd, Duration::from_secs(60))?;

        on_progress(InstallProgress {
            stage: InstallStage::Installing,
            message: "Binary installed to ~/.local/bin/cass".into(),
            percent: Some(80),
            elapsed: start.elapsed(),
        });

        // Verify installation
        self.verify_installation(on_progress, start)?;

        Ok(InstallResult {
            method: InstallMethod::PrebuiltBinary {
                url: url.to_string(),
                checksum: None,
            },
            version: self.target_version.clone(),
            duration: start.elapsed(),
            install_path: Some("~/.local/bin/cass".into()),
        })
    }

    /// Install via cargo install (compilation).
    fn install_via_cargo<F>(
        &self,
        on_progress: &F,
        start: Instant,
    ) -> Result<InstallResult, InstallError>
    where
        F: Fn(InstallProgress),
    {
        // Check compilation resources
        self.can_compile()?;

        on_progress(InstallProgress {
            stage: InstallStage::Compiling {
                crate_name: CRATE_NAME.into(),
            },
            message: "Starting cargo install (this may take 2-5 minutes)...".into(),
            percent: Some(10),
            elapsed: start.elapsed(),
        });

        // Use nohup for long-running installation to prevent SSH timeout
        let install_script = format!(
            r#"
# Start installation in background with logging
LOG_FILE=~/.cass_install.log
rm -f "$LOG_FILE"

nohup bash -c '
# Source cargo env in case this is called after bootstrap rustup install
source "$HOME/.cargo/env" 2>/dev/null || true
cargo install {}@{} 2>&1 | tee "$HOME/.cass_install.log"
echo "===INSTALL_COMPLETE===" >> "$HOME/.cass_install.log"
' > /dev/null 2>&1 &

echo "INSTALL_PID=$!"
"#,
            CRATE_NAME, self.target_version
        );

        // Start the installation
        let output = self.run_ssh_command(&install_script, Duration::from_secs(30))?;

        // Extract PID for monitoring
        let pid = output
            .lines()
            .find(|l| l.starts_with("INSTALL_PID="))
            .and_then(|l| l.strip_prefix("INSTALL_PID="))
            .and_then(|p| p.trim().parse::<u32>().ok());

        // Poll for completion
        self.poll_installation(pid, on_progress, start)?;

        // Verify installation
        self.verify_installation(on_progress, start)?;

        Ok(InstallResult {
            method: InstallMethod::CargoInstall,
            version: self.target_version.clone(),
            duration: start.elapsed(),
            install_path: Some("~/.cargo/bin/cass".into()),
        })
    }

    /// Install with full bootstrap (rustup + cargo).
    fn install_with_bootstrap<F>(
        &self,
        on_progress: &F,
        start: Instant,
    ) -> Result<InstallResult, InstallError>
    where
        F: Fn(InstallProgress),
    {
        on_progress(InstallProgress {
            stage: InstallStage::Downloading,
            message: "Installing Rust toolchain via rustup...".into(),
            percent: Some(5),
            elapsed: start.elapsed(),
        });

        // Install rustup
        let rustup_script = r#"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env
"#;

        self.run_ssh_command(rustup_script, Duration::from_secs(300))?;

        on_progress(InstallProgress {
            stage: InstallStage::Compiling {
                crate_name: CRATE_NAME.into(),
            },
            message: "Rust installed. Starting cargo install...".into(),
            percent: Some(20),
            elapsed: start.elapsed(),
        });

        // Now install cass via cargo
        self.install_via_cargo(on_progress, start)
    }

    /// Poll for installation completion.
    fn poll_installation<F>(
        &self,
        _pid: Option<u32>,
        on_progress: &F,
        start: Instant,
    ) -> Result<(), InstallError>
    where
        F: Fn(InstallProgress),
    {
        let poll_script = r#"
LOG_FILE=~/.cass_install.log
if [ -f "$LOG_FILE" ]; then
    if grep -q "===INSTALL_COMPLETE===" "$LOG_FILE"; then
        echo "STATUS=COMPLETE"
    elif grep -q "error\[" "$LOG_FILE" || grep -q "error:" "$LOG_FILE"; then
        echo "STATUS=ERROR"
        tail -20 "$LOG_FILE"
    else
        echo "STATUS=RUNNING"
        # Show last few lines of compilation progress
        tail -5 "$LOG_FILE" | grep -E "Compiling|Downloading|Installing" | tail -1
    fi
else
    echo "STATUS=NOT_STARTED"
fi
"#;

        let max_wait = Duration::from_secs(600); // 10 minutes max
        let poll_interval = Duration::from_secs(5);
        let mut last_crate = String::new();
        let mut progress_pct: u8 = 15;

        loop {
            if start.elapsed() > max_wait {
                return Err(InstallError::Timeout(max_wait.as_secs()));
            }

            std::thread::sleep(poll_interval);

            let output = self.run_ssh_command(poll_script, Duration::from_secs(30))?;

            if output.contains("STATUS=COMPLETE") {
                return Ok(());
            }

            if output.contains("STATUS=ERROR") {
                // Extract error message
                let error_lines: Vec<&str> = output
                    .lines()
                    .filter(|l| !l.starts_with("STATUS="))
                    .collect();
                let error_msg = error_lines.join("\n");

                // Check for common dependency issues
                if let Some(fix) = detect_missing_dependency(&error_msg) {
                    return Err(InstallError::MissingDependency {
                        dep: fix.0.to_string(),
                        fix: fix.1.to_string(),
                    });
                }

                return Err(InstallError::MethodFailed {
                    method: "cargo install".into(),
                    reason: error_msg,
                });
            }

            // Extract currently compiling crate
            for line in output.lines() {
                if line.contains("Compiling")
                    && let Some(crate_name) = line.split_whitespace().nth(1)
                    && crate_name != last_crate
                {
                    last_crate = crate_name.to_string();
                    progress_pct = (progress_pct + 3).min(85);
                }
            }

            on_progress(InstallProgress {
                stage: InstallStage::Compiling {
                    crate_name: if last_crate.is_empty() {
                        "dependencies".into()
                    } else {
                        last_crate.clone()
                    },
                },
                message: format!(
                    "Compiling {}...",
                    if last_crate.is_empty() {
                        "dependencies"
                    } else {
                        &last_crate
                    }
                ),
                percent: Some(progress_pct),
                elapsed: start.elapsed(),
            });
        }
    }

    /// Verify that cass was installed correctly.
    fn verify_installation<F>(&self, on_progress: &F, start: Instant) -> Result<(), InstallError>
    where
        F: Fn(InstallProgress),
    {
        on_progress(InstallProgress {
            stage: InstallStage::Verifying,
            message: "Verifying installation...".into(),
            percent: Some(90),
            elapsed: start.elapsed(),
        });

        // Try to run cass --version
        let verify_script = r#"
source ~/.cargo/env 2>/dev/null || true
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
cass --version 2>&1 || echo "VERIFY_FAILED"
"#;

        let output = self.run_ssh_command(verify_script, Duration::from_secs(30))?;

        if output.contains("VERIFY_FAILED") {
            return Err(InstallError::VerificationFailed(
                "cass --version failed".into(),
            ));
        }

        // Check version matches
        if !output.contains(&self.target_version) {
            return Err(InstallError::VerificationFailed(format!(
                "Version mismatch: expected {}, got {}",
                self.target_version,
                output.trim()
            )));
        }

        Ok(())
    }

    /// Run an SSH command on the remote host.
    fn run_ssh_command(&self, script: &str, timeout: Duration) -> Result<String, InstallError> {
        let timeout_secs = timeout.as_secs();

        let mut cmd = Command::new("ssh");
        cmd.arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg(format!("ConnectTimeout={}", timeout_secs.min(30)))
            .arg("-o")
            .arg("StrictHostKeyChecking=accept-new")
            .arg("-o")
            .arg("LogLevel=ERROR")
            .arg("--")
            .arg(&self.host)
            .arg("bash")
            .arg("-s");

        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn()?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(script.as_bytes())?;
        }

        let output = child.wait_with_output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("Connection refused")
                || stderr.contains("Connection timed out")
                || stderr.contains("Permission denied")
            {
                return Err(InstallError::SshFailed(stderr.trim().to_string()));
            }
            // Non-zero exit might be OK for some commands, return stdout
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

/// Detect missing system dependencies from compilation errors.
fn detect_missing_dependency(error: &str) -> Option<(&'static str, &'static str)> {
    if error.contains("openssl") || error.contains("libssl") {
        Some((
            "OpenSSL development headers",
            "Ubuntu/Debian: sudo apt install libssl-dev pkg-config\nRHEL/CentOS: sudo yum install openssl-devel",
        ))
    } else if error.contains("cc") && error.contains("not found") {
        Some((
            "C compiler",
            "Ubuntu/Debian: sudo apt install build-essential\nRHEL/CentOS: sudo yum groupinstall 'Development Tools'",
        ))
    } else if error.contains("pkg-config") {
        Some((
            "pkg-config",
            "Ubuntu/Debian: sudo apt install pkg-config\nRHEL/CentOS: sudo yum install pkgconfig",
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_system_info() -> SystemInfo {
        SystemInfo {
            os: "linux".into(),
            arch: "x86_64".into(),
            distro: Some("Ubuntu 22.04".into()),
            has_cargo: true,
            has_cargo_binstall: false,
            has_curl: true,
            has_wget: false,
            remote_home: "/home/user".into(),
        }
    }

    fn mock_resources() -> ResourceInfo {
        ResourceInfo {
            disk_available_mb: 10000,
            memory_total_mb: 8000,
            memory_available_mb: 4000,
            can_compile: true,
        }
    }

    #[test]
    fn test_install_method_display() {
        assert_eq!(
            InstallMethod::CargoBinstall.display_name(),
            "cargo-binstall"
        );
        assert_eq!(InstallMethod::CargoInstall.display_name(), "cargo install");
        assert_eq!(
            InstallMethod::FullBootstrap.display_name(),
            "full bootstrap (rustup + cargo)"
        );
    }

    #[test]
    fn test_install_method_requires_compilation() {
        assert!(!InstallMethod::CargoBinstall.requires_compilation());
        assert!(
            !InstallMethod::PrebuiltBinary {
                url: "".into(),
                checksum: None
            }
            .requires_compilation()
        );
        assert!(InstallMethod::CargoInstall.requires_compilation());
        assert!(InstallMethod::FullBootstrap.requires_compilation());
    }

    #[test]
    fn test_choose_method_prefers_binstall() {
        let mut system = mock_system_info();
        system.has_cargo_binstall = true;
        let resources = mock_resources();

        let installer = RemoteInstaller::new("test", system, resources);
        assert_eq!(
            installer.choose_method(),
            Some(InstallMethod::CargoBinstall)
        );
    }

    #[test]
    fn test_choose_method_cargo_install() {
        let mut system = mock_system_info();
        // Disable curl/wget so pre-built binary is not available
        system.has_curl = false;
        system.has_wget = false;
        let resources = mock_resources();

        let installer = RemoteInstaller::new("test", system, resources);
        // With cargo but no binstall and no download tools, should choose cargo install
        assert_eq!(installer.choose_method(), Some(InstallMethod::CargoInstall));
    }

    #[test]
    fn test_choose_method_prebuilt_binary() {
        let system = mock_system_info();
        let resources = mock_resources();

        let installer = RemoteInstaller::new("test", system, resources);
        // With curl available, should prefer pre-built binary over cargo install
        assert!(matches!(
            installer.choose_method(),
            Some(InstallMethod::PrebuiltBinary { .. })
        ));
    }

    #[test]
    fn test_choose_method_bootstrap_when_no_cargo() {
        let mut system = mock_system_info();
        system.has_cargo = false;
        // curl is needed for bootstrap (to download rustup)
        system.has_curl = true;
        system.has_wget = false;
        // Use unsupported arch so prebuilt binary is not available
        system.arch = "armv7".into();
        let resources = mock_resources();

        let installer = RemoteInstaller::new("test", system, resources);
        assert_eq!(installer.choose_method(), Some(InstallMethod::FullBootstrap));
    }

    #[test]
    fn test_choose_method_none_when_no_tools() {
        let mut system = mock_system_info();
        system.has_cargo = false;
        system.has_cargo_binstall = false;
        system.has_curl = false;
        system.has_wget = false;
        let resources = mock_resources();

        let installer = RemoteInstaller::new("test", system, resources);
        // No curl means no way to download rustup, no wget/curl means no prebuilt binary
        // No cargo means no cargo install - should return None
        assert_eq!(installer.choose_method(), None);
    }

    #[test]
    fn test_check_resources_ok() {
        let system = mock_system_info();
        let resources = mock_resources();

        let installer = RemoteInstaller::new("test", system, resources);
        assert!(installer.check_resources().is_ok());
    }

    #[test]
    fn test_check_resources_insufficient_disk() {
        let system = mock_system_info();
        let mut resources = mock_resources();
        resources.disk_available_mb = 500;

        let installer = RemoteInstaller::new("test", system, resources);
        let result = installer.check_resources();
        assert!(matches!(result, Err(InstallError::InsufficientDisk { .. })));
    }

    #[test]
    fn test_can_compile_insufficient_memory() {
        let system = mock_system_info();
        let mut resources = mock_resources();
        resources.memory_total_mb = 512;

        let installer = RemoteInstaller::new("test", system, resources);
        let result = installer.can_compile();
        assert!(matches!(
            result,
            Err(InstallError::InsufficientMemory { .. })
        ));
    }

    #[test]
    fn test_get_prebuilt_url_linux_x86() {
        let system = mock_system_info();
        let resources = mock_resources();

        let installer = RemoteInstaller::new("test", system, resources);
        let url = installer.get_prebuilt_url();
        assert!(url.is_some());
        assert!(url.unwrap().contains("linux-x86_64"));
    }

    #[test]
    fn test_get_prebuilt_url_macos_arm() {
        let mut system = mock_system_info();
        system.os = "darwin".into();
        system.arch = "aarch64".into();
        let resources = mock_resources();

        let installer = RemoteInstaller::new("test", system, resources);
        let url = installer.get_prebuilt_url();
        assert!(url.is_some());
        assert!(url.unwrap().contains("macos-aarch64"));
    }

    #[test]
    fn test_detect_missing_dependency_openssl() {
        let error = "error: failed to run custom build command for `openssl-sys`";
        let result = detect_missing_dependency(error);
        assert!(result.is_some());
        assert!(result.unwrap().0.contains("OpenSSL"));
    }

    #[test]
    fn test_detect_missing_dependency_cc() {
        let error = "error: linker `cc` not found";
        let result = detect_missing_dependency(error);
        assert!(result.is_some());
        assert!(result.unwrap().0.contains("C compiler"));
    }

    #[test]
    fn test_install_stage_display() {
        assert_eq!(InstallStage::Preparing.to_string(), "Preparing");
        assert_eq!(
            InstallStage::Compiling {
                crate_name: "tokio".into()
            }
            .to_string(),
            "Compiling tokio"
        );
        assert_eq!(InstallStage::Complete.to_string(), "Complete");
    }
}
