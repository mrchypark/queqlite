#!/usr/bin/env python3
"""Build and run balanced Linux Recorder WAL fdatasync/fsync comparisons."""

import argparse
import hashlib
import json
import platform
import shlex
import statistics
import subprocess
import sys
import time
from pathlib import Path


CANDIDATES = ("native-fdatasync", "fsync-preload")
DEFAULT_IMAGE = "rust:1.89.0-bookworm"
DEFAULT_RUNTIME_IMAGE = "debian:bookworm-slim"
DEFAULT_PLATFORM = "linux/arm64"
DEFAULT_PAIRS = 12
DEFAULT_OPERATIONS = 800
DEFAULT_WARMUP = 100
DEFAULT_PAYLOAD_BYTES = 128
MAX_PROVENANCE_PATHS = 32
SOURCE_SUFFIXES = {".c", ".lock", ".md", ".py", ".rs", ".sh", ".toml", ".yaml", ".yml"}
GENERATED_PATH_PARTS = {"__pycache__", "target"}
BUILD_REUSE_HASH_KEYS = (
    "workspace_cargo_toml_sha256",
    "cargo_lock_sha256",
    "rhiza_quepaxa_cargo_toml_sha256",
    "rhiza_quepaxa_lib_source_sha256",
    "rhiza_core_cargo_toml_sha256",
    "rhiza_core_lib_source_sha256",
    "recorder_sync_bench_source_sha256",
    "shim_source_sha256",
    "benchmark_binary_sha256",
    "shim_binary_sha256",
)


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("requires a positive integer")
    return parsed


def even_positive_int(value):
    parsed = positive_int(value)
    if parsed % 2:
        raise argparse.ArgumentTypeError("requires an even value for balanced positions")
    return parsed


def candidate_order(pair_index):
    shift = pair_index % len(CANDIDATES)
    return list(CANDIDATES[shift:] + CANDIDATES[:shift])


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def canonical_json_sha256(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256_bytes(encoded)


def command_text(command):
    return shlex.join(str(item) for item in command)


def run_checked(command, *, capture=True):
    result = subprocess.run(
        command,
        check=False,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {command_text(command)}\n"
            f"stdout:\n{result.stdout or ''}\nstderr:\n{result.stderr or ''}"
        )
    return result


def run_bytes(command):
    result = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {command_text(command)}\n"
            f"stderr:\n{result.stderr.decode('utf-8', errors='replace')}"
        )
    return result.stdout


def is_generated_path(path):
    return any(part in GENERATED_PATH_PARTS for part in path.parts) or path.suffix == ".pyc"


def is_relevant_source(path):
    return not is_generated_path(path) and path.suffix.lower() in SOURCE_SUFFIXES


def git_provenance(repo):
    head = run_checked(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], capture=True
    ).stdout.strip()
    tracked_diff = run_bytes(["git", "-C", str(repo), "diff", "--binary", "HEAD", "--"])
    untracked_raw = run_bytes(
        ["git", "-C", str(repo), "ls-files", "--others", "--exclude-standard", "-z"]
    )
    untracked = sorted(
        Path(item.decode("utf-8")) for item in untracked_raw.split(b"\0") if item
    )
    relevant = [path for path in untracked if is_relevant_source(path)]
    generated_count = sum(is_generated_path(path) for path in untracked)
    relevant_digest = hashlib.sha256()
    for relative in relevant:
        relevant_digest.update(relative.as_posix().encode("utf-8"))
        relevant_digest.update(b"\0")
        relevant_digest.update((repo / relative).read_bytes())
        relevant_digest.update(b"\0")
    concise_paths = [path.as_posix() for path in relevant[:MAX_PROVENANCE_PATHS]]
    return {
        "head": head,
        "dirty": bool(tracked_diff) or bool(untracked),
        "tracked_diff_sha256": sha256_bytes(tracked_diff),
        "tracked_diff_bytes": len(tracked_diff),
        "untracked_total_count": len(untracked),
        "untracked_generated_count": generated_count,
        "untracked_relevant_source_count": len(relevant),
        "untracked_relevant_source_sha256": relevant_digest.hexdigest(),
        "untracked_relevant_source_paths": concise_paths,
        "untracked_relevant_source_paths_omitted": len(relevant) - len(concise_paths),
    }


def docker_provenance(image):
    inspected = json.loads(
        run_checked(["docker", "image", "inspect", image], capture=True).stdout
    )[0]
    info = json.loads(
        run_checked(["docker", "info", "--format", "{{json .}}"], capture=True).stdout
    )
    return {
        "image_tag": image,
        "image_id": inspected.get("Id"),
        "image_repo_digests": inspected.get("RepoDigests", []),
        "docker_operating_system": info.get("OperatingSystem"),
        "docker_name": info.get("Name"),
        "docker_driver": info.get("Driver"),
        "host_platform": platform.platform(),
    }


def immutable_image_reference(docker):
    repo_digests = docker.get("image_repo_digests", [])
    reference = repo_digests[0] if repo_digests else docker.get("image_id")
    if not reference:
        raise SystemExit("Docker provenance has no immutable image identity")
    return reference


def select_docker_provenance(reuse_build, previous_provenance, image, inspector=docker_provenance):
    if not reuse_build:
        return inspector(image), image
    docker = previous_provenance.get("docker") if previous_provenance else None
    if not docker:
        raise SystemExit("reused build summary has no Docker provenance")
    return docker, immutable_image_reference(docker)


def validate_count(report, candidate, observed):
    expected = report["warmup"] + report["operations"]
    if candidate != "fsync-preload":
        if observed is not None:
            raise ValueError("native run unexpectedly produced an intercept count")
        return {"expected": None, "observed": None, "validated": True}
    if observed != expected:
        raise ValueError(
            f"fdatasync intercept count mismatch: expected {expected}, observed {observed}"
        )
    return {"expected": expected, "observed": observed, "validated": True}


def validate_report(report, candidate, operations, warmup, payload_bytes):
    errors = []
    if report.get("benchmark") != "recorder_wal_record":
        errors.append("benchmark identity mismatch")
    if report.get("sync_variant") != candidate:
        errors.append("sync variant mismatch")
    if report.get("command_mode") != "inline":
        errors.append("command mode is not inline")
    if report.get("operations") != operations or report.get("warmup") != warmup:
        errors.append("operation counts mismatch")
    if report.get("payload_bytes") != payload_bytes:
        errors.append("payload size mismatch")
    if report.get("completed") != operations or report.get("errors") != 0:
        errors.append("record operations failed")
    if report.get("latency_scope") != "successful_calls_only":
        errors.append("latency scope is not explicit")
    if report.get("platform", {}).get("os") != "linux":
        errors.append("benchmark did not run on Linux")
    if report.get("platform", {}).get("ld_preload") != (candidate == "fsync-preload"):
        errors.append("LD_PRELOAD state mismatch")
    wal = report.get("wal", {})
    if wal.get("frames") != operations + warmup:
        errors.append("WAL frame count mismatch")
    if wal.get("checkpoint_avoided_observed") is not True:
        errors.append("WAL checkpoint avoidance was not observed")
    if errors:
        raise ValueError("; ".join(errors))


def validate_reused_hashes(previous_hashes, current_hashes):
    mismatches = [
        key
        for key in BUILD_REUSE_HASH_KEYS
        if previous_hashes.get(key) != current_hashes.get(key)
    ]
    if mismatches:
        raise SystemExit(f"reused build provenance mismatch: {', '.join(mismatches)}")


def read_count(path):
    if not path.exists():
        return None
    value = int(path.read_text(encoding="utf-8").strip())
    path.unlink()
    return value


def build_command(repo, output, image, docker_platform):
    script = (
        "CARGO_TARGET_DIR=/artifacts/target cargo build --locked --release "
        "-p rhiza-quepaxa --example recorder_sync_bench && "
        "cc -std=c11 -O2 -Wall -Wextra -Werror -fPIC -shared "
        "-o /artifacts/fdatasync-as-fsync.so /work/bench/support/fdatasync-as-fsync.c"
    )
    return [
        "docker",
        "run",
        "--rm",
        "--platform",
        docker_platform,
        "-v",
        f"{repo}:/work:ro",
        "-v",
        f"{output}:/artifacts",
        "-w",
        "/work",
        image,
        "sh",
        "-ec",
        script,
    ]


def benchmark_command(
    build_artifacts,
    output,
    image,
    docker_platform,
    candidate,
    count_name,
    operations,
    warmup,
    payload,
):
    command = [
        "docker",
        "run",
        "--rm",
        "--platform",
        docker_platform,
        "-v",
        f"{build_artifacts}:/artifacts:ro",
        "-v",
        f"{output}:/results",
    ]
    if candidate == "fsync-preload":
        command.extend(
            [
                "-e",
                "LD_PRELOAD=/artifacts/fdatasync-as-fsync.so",
                "-e",
                f"RHIZA_FDATASYNC_COUNT_FILE=/results/{count_name}",
            ]
        )
    command.extend(
        [
            "--entrypoint",
            "/artifacts/target/release/examples/recorder_sync_bench",
            image,
            "--operations",
            str(operations),
            "--warmup",
            str(warmup),
            "--payload-bytes",
            str(payload),
            "--command-mode",
            "inline",
            "--label",
            candidate,
        ]
    )
    return command


def aggregate(rows):
    by_candidate = {}
    for candidate in CANDIDATES:
        reports = [row["report"] for row in rows if row["candidate"] == candidate]
        by_candidate[candidate] = {
            "runs": len(reports),
            "median_ops_per_second": statistics.median(
                report["ops_per_second"] for report in reports
            ),
            "median_latency_p50_ns": statistics.median(
                report["latency_ns"]["p50"] for report in reports
            ),
            "median_latency_p95_ns": statistics.median(
                report["latency_ns"]["p95"] for report in reports
            ),
            "median_latency_p99_ns": statistics.median(
                report["latency_ns"]["p99"] for report in reports
            ),
            "errors_total": sum(report["errors"] for report in reports),
        }
    ratios = []
    for pair_id in sorted({row["pair_id"] for row in rows}):
        pair = {row["candidate"]: row for row in rows if row["pair_id"] == pair_id}
        ratios.append(
            {
                "pair_id": pair_id,
                "fsync_preload_over_native_throughput": pair["fsync-preload"]["report"][
                    "ops_per_second"
                ]
                / pair["native-fdatasync"]["report"]["ops_per_second"],
            }
        )
    return {
        "candidates": by_candidate,
        "paired_ratios": ratios,
        "median_fsync_preload_over_native_throughput": statistics.median(
            ratio["fsync_preload_over_native_throughput"] for ratio in ratios
        ),
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", type=even_positive_int, default=DEFAULT_PAIRS)
    parser.add_argument("--operations", type=positive_int, default=DEFAULT_OPERATIONS)
    parser.add_argument("--warmup", type=positive_int, default=DEFAULT_WARMUP)
    parser.add_argument("--payload-bytes", type=positive_int, default=DEFAULT_PAYLOAD_BYTES)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--runtime-image", default=DEFAULT_RUNTIME_IMAGE)
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--reuse-build",
        type=Path,
        help="reuse a prior runner artifact directory after source-hash validation",
    )
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def self_test():
    assert candidate_order(0) == ["native-fdatasync", "fsync-preload"]
    assert candidate_order(1) == ["fsync-preload", "native-fdatasync"]
    assert candidate_order(2) == ["native-fdatasync", "fsync-preload"]
    assert validate_count({"warmup": 2, "operations": 3}, "fsync-preload", 5)[
        "validated"
    ]
    try:
        validate_count({"warmup": 2, "operations": 3}, "fsync-preload", 4)
    except ValueError:
        pass
    else:
        raise AssertionError("count mismatch must fail validation")
    rows = []
    for pair_index in range(2):
        pair_id = f"pair-{pair_index + 1:03d}"
        for candidate, throughput in (
            ("native-fdatasync", 100.0),
            ("fsync-preload", 80.0),
        ):
            rows.append(
                {
                    "pair_id": pair_id,
                    "candidate": candidate,
                    "report": {
                        "ops_per_second": throughput,
                        "errors": 0,
                        "latency_ns": {"p50": 1, "p95": 2, "p99": 3},
                    },
                }
            )
    assert aggregate(rows)["median_fsync_preload_over_native_throughput"] == 0.8
    assert is_generated_path(Path("experiments/example/target/debug/binary"))
    assert not is_relevant_source(Path("tool/__pycache__/helper.py"))
    assert is_relevant_source(Path("bench/support/shim.c"))
    assert canonical_json_sha256({"b": 2, "a": 1}) == canonical_json_sha256(
        {"a": 1, "b": 2}
    )
    prior_docker = {"image_id": "sha256:prior", "image_tag": DEFAULT_IMAGE}

    def forbidden_inspect(_image):
        raise AssertionError("reuse must not inspect the current Docker image")

    assert select_docker_provenance(
        True, {"docker": prior_docker}, DEFAULT_IMAGE, forbidden_inspect
    ) == (prior_docker, "sha256:prior")
    command = benchmark_command(
        Path("/build"),
        Path("/results"),
        "debian:bookworm-slim",
        "linux/arm64",
        "native-fdatasync",
        "unused.count",
        3,
        2,
        128,
    )
    assert command[command.index("--entrypoint") + 1] == (
        "/artifacts/target/release/examples/recorder_sync_bench"
    )
    assert command[command.index("--command-mode") + 1] == "inline"
    valid_report = {
        "benchmark": "recorder_wal_record",
        "sync_variant": "native-fdatasync",
        "command_mode": "inline",
        "operations": 3,
        "warmup": 2,
        "payload_bytes": 128,
        "completed": 3,
        "errors": 0,
        "latency_scope": "successful_calls_only",
        "platform": {"os": "linux", "ld_preload": False},
        "wal": {"frames": 5, "checkpoint_avoided_observed": True},
        "future_additive_field": {"accepted": True},
    }
    validate_report(valid_report, "native-fdatasync", 3, 2, 128)
    for invalid_mode in ("pre-stored", None):
        invalid_report = dict(valid_report)
        if invalid_mode is None:
            invalid_report.pop("command_mode")
        else:
            invalid_report["command_mode"] = invalid_mode
        try:
            validate_report(invalid_report, "native-fdatasync", 3, 2, 128)
        except ValueError as error:
            assert "command mode is not inline" in str(error)
        else:
            raise AssertionError("wrong or missing command mode must fail validation")
    matching_hashes = {key: "same" for key in BUILD_REUSE_HASH_KEYS}
    mismatched_hashes = dict(matching_hashes)
    mismatched_hashes["rhiza_core_lib_source_sha256"] = "changed"
    try:
        validate_reused_hashes(matching_hashes, mismatched_hashes)
    except SystemExit as error:
        assert "rhiza_core_lib_source_sha256" in str(error)
    else:
        raise AssertionError("core source mismatch must reject build reuse")
    mismatched_hashes = dict(matching_hashes)
    mismatched_hashes["rhiza_core_cargo_toml_sha256"] = "changed"
    try:
        validate_reused_hashes(matching_hashes, mismatched_hashes)
    except SystemExit as error:
        assert "rhiza_core_cargo_toml_sha256" in str(error)
    else:
        raise AssertionError("core manifest mismatch must reject build reuse")


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        print("recorder sync Linux runner self-test passed")
        return
    if args.operations + args.warmup > 1_000:
        raise SystemExit("warmup + operations must not exceed 1000")

    repo = Path(__file__).resolve().parents[1]
    output = (
        args.output
        or repo / "target" / f"recorder-sync-linux-{int(time.time())}"
    ).resolve()
    output.mkdir(parents=True, exist_ok=False)
    raw_path = output / "raw.jsonl"
    summary_path = output / "summary.json"

    pull = ["docker", "pull", "--platform", args.platform, args.image]
    runtime_pull = [
        "docker",
        "pull",
        "--platform",
        args.platform,
        args.runtime_image,
    ]
    build = build_command(repo, output, args.image, args.platform)
    build_artifacts = args.reuse_build.resolve() if args.reuse_build else output
    previous_provenance = None
    previous_conditions = None
    if args.reuse_build:
        previous_summary = build_artifacts / "summary.json"
        if previous_summary.exists():
            previous = json.loads(previous_summary.read_text(encoding="utf-8"))
            previous_provenance = previous.get("provenance")
            previous_conditions = previous.get("conditions")
    else:
        run_checked(pull, capture=False)
        run_checked(build, capture=False)
    run_checked(runtime_pull, capture=False)

    binary = build_artifacts / "target/release/examples/recorder_sync_bench"
    shim_binary = build_artifacts / "fdatasync-as-fsync.so"
    if not binary.is_file() or not shim_binary.is_file():
        raise SystemExit(f"build artifacts are incomplete: {build_artifacts}")
    current_hashes = {
        "recorder_sync_bench_source_sha256": sha256_file(
            repo / "crates/rhiza-quepaxa/examples/recorder_sync_bench.rs"
        ),
        "shim_source_sha256": sha256_file(repo / "bench/support/fdatasync-as-fsync.c"),
        "runner_source_sha256": sha256_file(Path(__file__).resolve()),
        "rhiza_quepaxa_lib_source_sha256": sha256_file(
            repo / "crates/rhiza-quepaxa/src/lib.rs"
        ),
        "rhiza_quepaxa_cargo_toml_sha256": sha256_file(
            repo / "crates/rhiza-quepaxa/Cargo.toml"
        ),
        "rhiza_core_lib_source_sha256": sha256_file(repo / "crates/rhiza-core/src/lib.rs"),
        "rhiza_core_cargo_toml_sha256": sha256_file(repo / "crates/rhiza-core/Cargo.toml"),
        "workspace_cargo_toml_sha256": sha256_file(repo / "Cargo.toml"),
        "cargo_lock_sha256": sha256_file(repo / "Cargo.lock"),
        "benchmark_binary_sha256": sha256_file(binary),
        "shim_binary_sha256": sha256_file(shim_binary),
    }
    if args.reuse_build:
        if previous_provenance is None or previous_conditions is None:
            raise SystemExit("--reuse-build requires the prior summary.json for provenance")
        previous_hashes = previous_provenance.get("hashes", {})
        validate_reused_hashes(previous_hashes, current_hashes)
        if previous_conditions.get("platform") != args.platform:
            raise SystemExit("reused build platform does not match --platform")
    docker, _build_image = select_docker_provenance(
        bool(args.reuse_build), previous_provenance, args.image
    )
    runtime_docker = docker_provenance(args.runtime_image)
    runtime_image = immutable_image_reference(runtime_docker)
    provenance = {
        "git": git_provenance(repo),
        "hashes": current_hashes,
        "docker": docker,
        "runtime_docker": runtime_docker,
        "commands": {
            "pull": (
                previous_provenance["commands"]["pull"]
                if previous_provenance
                else {"argv": pull, "shell": command_text(pull)}
            ),
            "build": (
                previous_provenance["commands"]["build"]
                if previous_provenance
                else {"argv": build, "shell": command_text(build)}
            ),
            "runtime_pull": {
                "argv": runtime_pull,
                "shell": command_text(runtime_pull),
            },
        },
        "reused_build_artifacts": str(build_artifacts) if args.reuse_build else None,
    }
    provenance_sha256 = canonical_json_sha256(provenance)
    provenance_reference = {
        "location": "summary.json#provenance",
        "sha256": provenance_sha256,
    }

    rows = []
    with raw_path.open("w", encoding="utf-8") as raw:
        for pair_index in range(args.pairs):
            pair_id = f"pair-{pair_index + 1:03d}"
            order = candidate_order(pair_index)
            for position, candidate in enumerate(order):
                count_name = f"{pair_id}-{candidate}.count"
                count_path = output / count_name
                count_path.unlink(missing_ok=True)
                command = benchmark_command(
                    build_artifacts,
                    output,
                    runtime_image,
                    args.platform,
                    candidate,
                    count_name,
                    args.operations,
                    args.warmup,
                    args.payload_bytes,
                )
                result = run_checked(command, capture=True)
                lines = [line for line in result.stdout.splitlines() if line.strip()]
                if len(lines) != 1:
                    raise RuntimeError(f"benchmark emitted {len(lines)} non-empty stdout lines")
                report = json.loads(lines[0])
                validate_report(
                    report, candidate, args.operations, args.warmup, args.payload_bytes
                )
                count = validate_count(report, candidate, read_count(count_path))
                row = {
                    "schema_version": 1,
                    "pair_id": pair_id,
                    "pair_index": pair_index,
                    "order": order,
                    "position": position,
                    "candidate": candidate,
                    "command": {"argv": command, "shell": command_text(command)},
                    "fdatasync_intercepts": count,
                    "report": report,
                    "provenance_ref": provenance_reference,
                }
                rows.append(row)
                raw.write(json.dumps(row, sort_keys=True) + "\n")
                raw.flush()

    summary = {
        "schema_version": 1,
        "production_valid": False,
        "scope": "Docker virtual-filesystem syscall diagnostic; not physical durability evidence",
        "conditions": {
            "pairs": args.pairs,
            "operations": args.operations,
            "warmup": args.warmup,
            "payload_bytes": args.payload_bytes,
            "platform": args.platform,
            "runtime_image": runtime_image,
            "balanced_order": [candidate_order(index) for index in range(args.pairs)],
        },
        "provenance": provenance,
        "provenance_sha256": provenance_sha256,
        "raw_jsonl": str(raw_path),
        "aggregate": aggregate(rows),
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"raw_jsonl": str(raw_path), "summary": str(summary_path)}))


if __name__ == "__main__":
    main()
