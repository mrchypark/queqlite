use rust_sqlite_conformance::{DEFAULT_SUMMARY_PATH, print_table, run_adversarial_child, run_all};
use std::path::{Path, PathBuf};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--adversarial-child") {
        let path = args.get(2).expect("--adversarial-child requires DB path");
        if let Err(error) = run_adversarial_child(Path::new(path)) {
            eprintln!("{error}");
            std::process::exit(2);
        }
        return;
    }

    let mut output = PathBuf::from(DEFAULT_SUMMARY_PATH);
    let mut bench_iterations = 200;
    let mut allow_hard_stop = false;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--output" => {
                index += 1;
                output = PathBuf::from(args.get(index).expect("--output requires a path"));
            }
            "--bench-iterations" => {
                index += 1;
                bench_iterations = args
                    .get(index)
                    .expect("--bench-iterations requires a number")
                    .parse()
                    .expect("invalid --bench-iterations");
            }
            "--allow-hard-stop" => allow_hard_stop = true,
            other => panic!("unknown argument: {other}"),
        }
        index += 1;
    }

    let exe = std::env::current_exe().expect("current executable path");
    let summary = run_all(&exe, bench_iterations);
    print_table(&summary);
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).expect("create summary directory");
    }
    std::fs::write(
        &output,
        serde_json::to_vec_pretty(&summary).expect("serialize summary"),
    )
    .expect("write summary");
    println!("summary={}", output.display());
    let exit_code =
        rust_sqlite_conformance::hard_stop_exit_code(summary.hard_stop, allow_hard_stop);
    if exit_code != 0 {
        eprintln!(
            "hard stop is active; summary was written, exiting {exit_code}. Use --allow-hard-stop only to generate diagnostic artifacts."
        );
        std::process::exit(exit_code);
    }
}
