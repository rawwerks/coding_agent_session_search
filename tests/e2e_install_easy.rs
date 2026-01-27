//! E2E Install Easy Mode Test
//!
//! This test validates install.sh logic using lightweight fixture binaries.
//! It uses fake rustc/cargo binaries to skip rustup checks, allowing fast
//! isolated testing of the install script logic (checksum verification,
//! unpacking, path setup).
//!
//! ## Real Install Testing
//!
//! Full real-world install testing with actual Rust tooling runs in CI via:
//! `.github/workflows/install-test.yml`
//!
//! The CI workflow:
//! - Builds a real release binary
//! - Creates a tarball with SHA256 checksum
//! - Runs install.sh with real rustc/cargo/sha256sum
//! - Verifies the installed binary works
//! - Uploads structured logs as artifacts
//!
//! ## Allowlist Status
//!
//! The fake binaries in this test are permanently allowlisted per the no-mock
//! policy (see `test-results/no_mock_allowlist.json`) because:
//! 1. They test install script logic in isolation
//! 2. Real CI coverage exists via install-test.yml
//! 3. Local iteration speed is preserved

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;

mod util;
use util::e2e_log::{E2eError, E2eLogger, E2eTestInfo};

fn e2e_logging_enabled() -> bool {
    std::env::var("E2E_LOG").is_ok()
}

fn run_logged_test<F>(name: &str, suite: &str, file: &str, line: u32, test_fn: F)
where
    F: FnOnce() -> Result<(), Box<dyn std::error::Error>>,
{
    let logger = if e2e_logging_enabled() {
        E2eLogger::new("rust").ok()
    } else {
        None
    };

    let test_info = E2eTestInfo::new(name, suite, file, line);
    if let Some(ref lg) = logger {
        let _ = lg.test_start(&test_info);
    }

    let start = Instant::now();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test_fn));
    let duration_ms = start.elapsed().as_millis() as u64;

    let (is_pass, error_msg, panic_type) = match &result {
        Ok(Ok(())) => (true, None, None),
        Ok(Err(e)) => (false, Some(e.to_string()), None),
        Err(panic) => {
            let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_string()
            };
            (false, Some(msg), Some("Panic"))
        }
    };

    if let Some(ref lg) = logger {
        if is_pass {
            let _ = lg.test_pass(&test_info, duration_ms, None);
        } else {
            let _ = lg.test_fail(
                &test_info,
                duration_ms,
                None,
                E2eError {
                    message: error_msg.unwrap_or_default(),
                    error_type: panic_type.map(String::from),
                    stack: None,
                    context: None,
                },
            );
        }
        let _ = lg.flush();
    }

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn fixture(name: &str) -> PathBuf {
    fs::canonicalize(PathBuf::from(name)).expect("fixture path")
}

#[test]
#[cfg_attr(not(target_os = "linux"), ignore)]
fn install_easy_mode_end_to_end() {
    run_logged_test(
        "install_easy_mode_end_to_end",
        "e2e_install_easy",
        file!(),
        line!(),
        || {
            let tar =
                fixture("tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz");
            let checksum = fs::read_to_string(
                "tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz.sha256",
            )
            .unwrap()
            .trim()
            .to_string();

            let temp_home = tempfile::TempDir::new().unwrap();
            let dest = tempfile::TempDir::new().unwrap();
            let fake_bin = temp_home.path().join("bin");
            fs::create_dir_all(&fake_bin).unwrap();

            // Fake nightly rustc + cargo to skip rustup.
            fs::write(
                fake_bin.join("rustc"),
                "#!/bin/sh\necho rustc 1.80.0-nightly\n",
            )
            .unwrap();
            fs::write(
                fake_bin.join("cargo"),
                "#!/bin/sh\necho cargo 1.80.0-nightly\n",
            )
            .unwrap();
            fs::write(
                fake_bin.join("sha256sum"),
                "#!/bin/sh\n/usr/bin/sha256sum \"$@\"\n",
            )
            .unwrap();
            for f in [&"rustc", &"cargo", &"sha256sum"] {
                let p = fake_bin.join(f);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mut perms = fs::metadata(&p).unwrap().permissions();
                    perms.set_mode(0o755);
                    fs::set_permissions(&p, perms).unwrap();
                }
                #[cfg(windows)]
                {
                    let mut perms = fs::metadata(&p).unwrap().permissions();
                    perms.set_readonly(false);
                    fs::set_permissions(&p, perms).unwrap();
                }
            }

            let output = Command::new("timeout")
                .arg("30s")
                .arg("bash")
                .arg("install.sh")
                .arg("--version")
                .arg("vtest")
                .arg("--easy-mode")
                .arg("--verify")
                .arg("--dest")
                .arg(dest.path())
                .env("HOME", temp_home.path())
                .env(
                    "PATH",
                    format!(
                        "{}:{}",
                        fake_bin.display(),
                        std::env::var("PATH").unwrap_or_default()
                    ),
                )
                .env("ARTIFACT_URL", format!("file://{}", tar.display()))
                .env("CHECKSUM", checksum)
                .env("RUSTUP_INIT_SKIP", "1")
                .output()
                .expect("run installer");

            assert!(output.status.success(), "installer should succeed");

            // Verify installation
            let bin = dest.path().join("cass");
            assert!(bin.exists(), "Binary not found at expected path");

            // Verify self-test worked (printed version)
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(stdout.contains("fixture-linux"));
            assert!(stdout.contains("Done. Run: cass"));
            Command::new(&bin)
                .arg("--help")
                .status()
                .expect("run binary");

            Ok(())
        },
    );
}
