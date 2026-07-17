use rust_sqlite_conformance::{
    CORPUS_SHA256_V1, DEFAULT_SUMMARY_PATH, Status, corpus_digest, hard_stop_exit_code, run_all,
    run_core_differential, run_interoperability, run_policy_capabilities, run_session_cross_apply,
    run_snapshot_backup, run_timeout_probe, run_wal_reopen,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

struct RemoveFileOnDrop(PathBuf);

impl Drop for RemoveFileOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn fixed_corpus_has_stable_sha256_identity() {
    assert_eq!(corpus_digest(), CORPUS_SHA256_V1);
    assert_eq!(DEFAULT_SUMMARY_PATH, "results/conformance-summary.json");
}

#[test]
fn observable_results_match_for_fixed_corpus_and_returning() {
    assert_eq!(run_core_differential().status, Status::Pass);
}

#[test]
fn sqlite_files_interoperate_in_both_directions() {
    assert_eq!(run_interoperability().status, Status::Pass);
}

#[test]
fn wal_reopen_and_reference_wal_overlay_are_observable() {
    assert_eq!(run_wal_reopen().status, Status::Pass);
}

#[test]
fn snapshots_backups_and_changesets_cross_engine_boundaries() {
    assert_eq!(run_snapshot_backup().status, Status::Pass);
    assert_eq!(run_session_cross_apply().status, Status::Pass);
}

#[test]
fn timed_out_candidate_is_killed_and_next_case_runs() {
    let exe = Path::new(env!("CARGO_BIN_EXE_rust-sqlite-conformance"));
    assert_eq!(run_timeout_probe(exe).status, Status::Pass);
}

#[test]
fn missing_policy_and_cancellation_capabilities_are_never_silent() {
    let checks = run_policy_capabilities();
    for required in [
        "fine_grained_authorizer",
        "deterministic_progress_handler",
        "interrupt_handle",
        "complete_explain_opcode_policy",
        "trusted_schema",
        "compile_options",
    ] {
        assert!(checks.iter().any(|check| check.name == required));
    }
    assert!(checks.iter().all(|check| matches!(
        check.status,
        Status::Pass | Status::Fail | Status::BlockedCapability
    )));
}

#[test]
fn run_all_hard_stop_requires_explicit_diagnostic_override() {
    let exe = Path::new(env!("CARGO_BIN_EXE_rust-sqlite-conformance"));
    let summary = run_all(exe, 2);
    assert!(summary.hard_stop);
    assert_eq!(hard_stop_exit_code(summary.hard_stop, false), 2);
    assert_eq!(hard_stop_exit_code(summary.hard_stop, true), 0);
}

#[test]
fn cli_writes_summary_before_enforcing_hard_stop_exit() {
    let exe = Path::new(env!("CARGO_BIN_EXE_rust-sqlite-conformance"));
    let output = RemoveFileOnDrop(std::env::temp_dir().join(format!(
        "rust-sqlite-conformance-cli-gate-{}.json",
        std::process::id()
    )));
    let _ = fs::remove_file(&output.0);

    let blocked = Command::new(exe)
        .args([
            "--output",
            output.0.to_str().unwrap(),
            "--bench-iterations",
            "1",
        ])
        .status()
        .unwrap();
    assert_eq!(blocked.code(), Some(2));
    assert!(output.0.exists(), "summary must be written before exit 2");

    let allowed = Command::new(exe)
        .args([
            "--output",
            output.0.to_str().unwrap(),
            "--bench-iterations",
            "1",
            "--allow-hard-stop",
        ])
        .status()
        .unwrap();
    assert!(allowed.success());
}
