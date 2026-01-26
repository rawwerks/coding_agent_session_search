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

fn fixture(name: &str) -> PathBuf {
    fs::canonicalize(PathBuf::from(name)).expect("fixture path")
}

#[test]
#[cfg_attr(not(target_os = "linux"), ignore)]
fn install_easy_mode_end_to_end() {
    let tar = fixture("tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz");
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
}
