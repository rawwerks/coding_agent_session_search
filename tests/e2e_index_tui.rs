use assert_cmd::cargo::cargo_bin_cmd;
use std::fs;
use std::path::Path;
use std::time::Instant;

mod util;
use util::EnvGuard;
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

fn make_codex_fixture(root: &Path) {
    let sessions = root.join("sessions/2025/11/21");
    fs::create_dir_all(&sessions).unwrap();
    let file = sessions.join("rollout-1.jsonl");
    let sample = r#"{"role":"user","timestamp":1700000000000,"content":"hello"}
{"role":"assistant","timestamp":1700000001000,"content":"hi"}
"#;
    fs::write(file, sample).unwrap();
}

#[test]
fn index_then_tui_once_headless() {
    run_logged_test(
        "index_then_tui_once_headless",
        "e2e_index_tui",
        file!(),
        line!(),
        || {
            let tmp = tempfile::TempDir::new().unwrap();
            // Isolate from the developer machine's real session dirs (HOME-based connectors).
            let home = tmp.path().join("home");
            fs::create_dir_all(&home).unwrap();
            let _guard_home = EnvGuard::set("HOME", home.to_string_lossy());

            let xdg = tmp.path().join("xdg");
            fs::create_dir_all(&xdg).unwrap();
            let _guard_xdg = EnvGuard::set("XDG_DATA_HOME", xdg.to_string_lossy());

            let data_dir = tmp.path().join("data");
            fs::create_dir_all(&data_dir).unwrap();

            // Codex fixture under CODEX_HOME to satisfy detection.
            let _guard_codex = EnvGuard::set("CODEX_HOME", data_dir.to_string_lossy());
            make_codex_fixture(&data_dir);

            // Run index --full against this data dir.
            cargo_bin_cmd!("cass")
                .arg("index")
                .arg("--full")
                .arg("--data-dir")
                .arg(&data_dir)
                // Avoid connector detection from the repository CWD (e.g. `.aider.chat.history.md`).
                .current_dir(&home)
                .assert()
                .success();

            // Smoke run TUI once in headless mode using the same data dir.
            cargo_bin_cmd!("cass")
                .arg("tui")
                .arg("--data-dir")
                .arg(&data_dir)
                .arg("--once")
                // Avoid connector detection from the repository CWD (e.g. `.aider.chat.history.md`).
                .current_dir(&home)
                .env("TUI_HEADLESS", "1")
                .assert()
                .success();

            // Ensure index artifacts exist.
            assert!(data_dir.join("agent_search.db").exists());
            assert!(data_dir.join("index/v6").exists());

            Ok(())
        },
    );
}
