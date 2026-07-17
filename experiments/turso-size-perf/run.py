#!/usr/bin/env python3
"""Build, size, and benchmark the two isolated embedded database binaries."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import sqlite3
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"
ARTIFACTS = ROOT / "artifacts"
BACKENDS = {
    "turso": ("turso-backend", "turso-size-perf"),
    "rusqlite": ("rusqlite-backend", "rusqlite-size-perf"),
}
SCENARIOS = [
    {"scenario": "cold_open", "count": 1, "writers": 1},
    {"scenario": "warm_open", "count": 1, "writers": 1},
    {"scenario": "point_insert", "count": 32, "writers": 1},
    {"scenario": "point_update", "count": 32, "writers": 1},
    {"scenario": "point_read", "count": 500, "writers": 1},
    {"scenario": "ordered_scan", "count": 1_000, "writers": 1},
    {"scenario": "transaction_batch", "count": 500, "writers": 1},
    *[
        {"scenario": "multi_writer", "count": 16, "writers": writers}
        for writers in (1, 2, 4, 8)
    ],
]


def source_inputs(root: Path = ROOT) -> list[str]:
    files = ["Cargo.toml", "Cargo.lock", "run.py"]
    files.extend(str(path.relative_to(root)) for path in sorted((root / "src").rglob("*.rs")))
    files.extend(
        str(path.relative_to(root)) for path in sorted((root / "tests").rglob("*.py"))
    )
    return files


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_tree_digest(root: Path, files: list[str]) -> str:
    digest = hashlib.sha256()
    for relative in sorted(files):
        content = (root / relative).read_bytes()
        encoded = relative.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def collect_provenance(
    root: Path,
    inputs: list[str],
    artifacts: dict[str, dict[str, Path]],
    invocations: list[list[str]],
) -> dict:
    return {
        "source_tree": {
            "algorithm": "sha256(path-length,path,content-length,content; sorted paths)",
            "digest": source_tree_digest(root, inputs),
            "files": sorted(inputs),
        },
        "cargo_lock_sha256": sha256_file(root / "Cargo.lock"),
        "binaries": {
            backend: {
                flavor: {"path": str(path), "sha256": sha256_file(path)}
                for flavor, path in paths.items()
            }
            for backend, paths in artifacts.items()
        },
        "benchmark_invocations": invocations,
    }


def validate_provenance(
    provenance: dict,
    root: Path,
    inputs: list[str],
    artifacts: dict[str, dict[str, Path]],
    invocations: list[list[str]],
) -> None:
    if provenance["source_tree"]["files"] != sorted(inputs) or provenance["source_tree"][
        "digest"
    ] != source_tree_digest(root, inputs):
        raise ValueError("source-tree provenance mismatch")
    if provenance["cargo_lock_sha256"] != sha256_file(root / "Cargo.lock"):
        raise ValueError("Cargo.lock provenance mismatch")
    for backend, paths in artifacts.items():
        for flavor, path in paths.items():
            observed = provenance["binaries"][backend][flavor]
            if observed["path"] != str(path) or observed["sha256"] != sha256_file(path):
                raise ValueError(f"binary provenance mismatch: {backend}/{flavor}")
    if provenance["benchmark_invocations"] != invocations:
        raise ValueError("invocation provenance mismatch")


def validate_balanced_counts(samples: int, warmups: int) -> None:
    if samples < 6 or samples % 2:
        raise ValueError("samples must be even and at least 6")
    if warmups < 2 or warmups % 2:
        raise ValueError("warmups must be even and at least 2")


def command(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=ROOT,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def command_text(args: list[str]) -> str:
    return " ".join(args)


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return f"{value:.2f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def parse_human_size(value: str) -> int:
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*(B|KiB|MiB|GiB)", value)
    if not match:
        raise ValueError(f"invalid size: {value}")
    factors = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}
    return round(float(match.group(1)) * factors[match.group(2)])


def collect_environment(commands: list[str]) -> dict:
    def output(*args: str) -> str:
        commands.append(command_text(list(args)))
        return command(list(args)).stdout.strip()

    return {
        "captured_at_unix": int(time.time()),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "rustc": output("rustc", "-Vv"),
        "cargo": output("cargo", "-V"),
        "target": output("rustc", "--print", "host-tuple"),
        "git_head": output("git", "rev-parse", "HEAD"),
        "git_origin_main": output("git", "rev-parse", "origin/main"),
        "git_dirty": bool(output("git", "status", "--porcelain")),
        "crate_pins": {
            "turso": "=0.7.0 (default-features=false)",
            "rusqlite": "=0.40.1 (bundled)",
            "tokio": "=1.48.0 (rt-multi-thread,sync; both binaries)",
        },
        "durability": {
            "requested": "journal_mode=WAL; synchronous=FULL; busy_timeout=0ms",
            "stratum": "FULL only",
        },
    }


def build_and_size(commands: list[str]) -> tuple[dict, dict[str, dict[str, Path]]]:
    RESULTS.mkdir(exist_ok=True)
    ARTIFACTS.mkdir(exist_ok=True)
    artifacts: dict[str, dict[str, Path]] = {}
    size_results: dict[str, dict] = {}
    for backend, (feature, binary_name) in BACKENDS.items():
        build = [
            "cargo",
            "build",
            "--locked",
            "--release",
            "--no-default-features",
            "--features",
            feature,
            "--bin",
            binary_name,
        ]
        commands.append(command_text(build))
        command(build)
        source = ROOT / "target" / "release" / binary_name
        backend_dir = ARTIFACTS / backend
        backend_dir.mkdir(parents=True, exist_ok=True)
        unstripped = backend_dir / f"{binary_name}.unstripped"
        stripped = backend_dir / binary_name
        shutil.copy2(source, unstripped)
        shutil.copy2(source, stripped)
        strip_cmd = ["strip", "-x", str(stripped)]
        commands.append(command_text(strip_cmd))
        command(strip_cmd)
        artifacts[backend] = {"unstripped": unstripped, "stripped": stripped}

        tree_cmd = [
            "cargo",
            "tree",
            "--locked",
            "--prefix",
            "none",
            "--no-default-features",
            "--features",
            feature,
            "-e",
            "normal",
        ]
        commands.append(command_text(tree_cmd))
        tree = command(tree_cmd).stdout
        (RESULTS / f"cargo-tree-{backend}.txt").write_text(tree)
        feature_tree_cmd = [*tree_cmd[:-1], "features"]
        commands.append(command_text(feature_tree_cmd))
        (RESULTS / f"cargo-tree-{backend}-features.txt").write_text(
            command(feature_tree_cmd).stdout
        )
        dependencies = {
            (match.group(1), match.group(2))
            for line in tree.splitlines()
            if (match := re.match(r"^([A-Za-z0-9_-]+) v([^\s]+)", line))
            and match.group(1) != "turso-size-perf"
        }
        otool_cmd = ["otool", "-L", str(stripped)]
        commands.append(command_text(otool_cmd))
        dynamic = command(otool_cmd).stdout.splitlines()[1:]
        size_results[backend] = {
            "unstripped_bytes": unstripped.stat().st_size,
            "unstripped_human": human_size(unstripped.stat().st_size),
            "stripped_bytes": stripped.stat().st_size,
            "stripped_human": human_size(stripped.stat().st_size),
            "normal_dependency_count": len(dependencies),
            "dynamic_libraries": [line.strip().split(" (")[0] for line in dynamic],
            "payload_note": (
                "Mach-O current-target binary plus listed macOS system libraries; "
                "not a Linux container payload and not a Rhiza image measurement"
            ),
        }
    for flavor in ("unstripped_bytes", "stripped_bytes"):
        size_results["ratios"] = size_results.get("ratios", {})
        size_results["ratios"][f"turso_over_rusqlite_{flavor}"] = (
            size_results["turso"][flavor] / size_results["rusqlite"][flavor]
        )
    return size_results, artifacts


def checksum_rows(rows: list[tuple[int, str]]) -> int:
    checksum = 0xCBF29CE484222325
    for row_id, value in rows:
        for byte in row_id.to_bytes(8, "little", signed=True) + value.encode():
            checksum ^= byte
            checksum = (checksum * 0x100000001B3) & ((1 << 64) - 1)
    return checksum


def independent_db_observation(path: Path) -> tuple[int, int]:
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
        rows = conn.execute("SELECT id,value FROM kv ORDER BY id").fetchall()
    return len(rows), checksum_rows(rows)


def run_one(binary: Path, case: dict) -> dict:
    with tempfile.TemporaryDirectory(prefix="turso-size-perf-") as temp:
        db = Path(temp) / "sample.db"
        args = [
            str(binary),
            "--db",
            str(db),
            "--scenario",
            case["scenario"],
            "--count",
            str(case["count"]),
            "--writers",
            str(case["writers"]),
        ]
        started = time.perf_counter_ns()
        result = command(args)
        external = time.perf_counter_ns() - started
        parsed = json.loads(result.stdout)
        independent_row_count, independent_checksum = independent_db_observation(db)
        parsed["external_process_ns"] = external
        parsed["invocation"] = args
        parsed["independent_row_count"] = independent_row_count
        parsed["independent_checksum"] = independent_checksum
        return parsed


def case_key(record: dict) -> str:
    return f"{record['scenario']}:writers={record['writers']}:count={record['count']}"


def validate_checksums(records: list[dict]) -> None:
    by_case: dict[str, dict[str, set[int]]] = {}
    for record in records:
        if record["row_count"] != record["independent_row_count"]:
            raise ValueError("persisted row count mismatch")
        if record["checksum"] != record["independent_checksum"]:
            raise ValueError("independent checksum mismatch")
        if record["scenario"] == "multi_writer":
            expected = record["writers"] * record["count"]
            if record["successes"] + record["errors"] != expected:
                raise ValueError("multi-writer accounting mismatch")
            if record["row_count"] != record["successes"]:
                raise ValueError("multi-writer persisted row count mismatch")
            if record["busy"] > record["errors"]:
                raise ValueError("busy count exceeds error count")
            continue
        by_case.setdefault(case_key(record), {}).setdefault(record["backend"], set()).add(
            record["checksum"]
        )
    for key, backends in by_case.items():
        if set(backends) != set(BACKENDS):
            raise ValueError(f"missing backend for {key}")
        if len(backends["turso"] | backends["rusqlite"]) != 1:
            raise ValueError(f"checksum mismatch for {key}: {backends}")


def summarize(records: list[dict], samples: int) -> dict:
    grouped: dict[str, dict[str, list[dict]]] = {}
    for record in records:
        grouped.setdefault(case_key(record), {}).setdefault(record["backend"], []).append(record)
    summary: dict[str, dict] = {}
    for key, backends in grouped.items():
        summary[key] = {
            "primary_metric": "open_ns" if key.startswith("warm_open:") else "operation_ns"
        }
        for backend, rows in backends.items():
            metrics = {}
            for metric in (
                "external_process_ns",
                "runtime_init_ns",
                "open_ns",
                "setup_ns",
                "operation_ns",
            ):
                values = [row[metric] for row in rows]
                metrics[metric] = {
                    "median": int(statistics.median(values)),
                    "min": min(values),
                    "max": max(values),
                }
            throughput = [
                row["successes"] * 1_000_000_000 / row["operation_ns"]
                if row["operation_ns"]
                else 0.0
                for row in rows
            ]
            attempts = sum(row["successes"] + row["errors"] for row in rows)
            summary[key][backend] = {
                "samples": len(rows),
                "metrics": metrics,
                "successes": sum(row["successes"] for row in rows),
                "errors": sum(row["errors"] for row in rows),
                "busy": sum(row["busy"] for row in rows),
                "error_rate": sum(row["errors"] for row in rows) / attempts,
                "busy_rate": sum(row["busy"] for row in rows) / attempts,
                "successful_ops_per_second": {
                    "median": statistics.median(throughput),
                    "min": min(throughput),
                    "max": max(throughput),
                },
                "observed_journal_modes": sorted({row["journal_mode"] for row in rows}),
                "observed_synchronous": sorted({row["synchronous"] for row in rows}),
            }
        summary[key]["ratios"] = {
            metric: (
                summary[key]["turso"]["metrics"][metric]["median"]
                / summary[key]["rusqlite"]["metrics"][metric]["median"]
            )
            for metric in (
                "external_process_ns",
                "runtime_init_ns",
                "open_ns",
                "setup_ns",
                "operation_ns",
            )
            if summary[key]["rusqlite"]["metrics"][metric]["median"]
        }
    validate_summary(summary, samples)
    return summary


def validate_summary(summary: dict, minimum_samples: int = 6) -> None:
    if not summary:
        raise ValueError("summary is empty")
    for key, case in summary.items():
        expected_primary = "open_ns" if key.startswith("warm_open:") else "operation_ns"
        if case.get("primary_metric") != expected_primary:
            raise ValueError(f"incorrect primary metric for {key}")
        for backend in BACKENDS:
            if backend not in case:
                raise ValueError(f"{key} missing {backend}")
            if case[backend]["samples"] < minimum_samples:
                raise ValueError(f"{key}/{backend} has fewer than {minimum_samples} samples")
            for values in case[backend]["metrics"].values():
                if not values["min"] <= values["median"] <= values["max"]:
                    raise ValueError(f"invalid min/median/max for {key}/{backend}")
            if key.startswith("warm_open:") and any(
                case[backend]["metrics"]["operation_ns"][name] != 0
                for name in ("min", "median", "max")
            ):
                raise ValueError(f"warm reopen operation timing must be zero for {backend}")


def validate_summary_consistency(records: list[dict], summary: dict, samples: int) -> None:
    if summary != summarize(records, samples):
        raise ValueError("summary does not match raw records")


def validate_order_balance(records: list[dict]) -> None:
    first_by_sample: dict[tuple[str, int], str] = {}
    for record in records:
        first_by_sample[(case_key(record), record["sample"])] = record["execution_order"][0]
    by_case: dict[str, list[str]] = {}
    for (key, _), first in first_by_sample.items():
        by_case.setdefault(key, []).append(first)
    for key, firsts in by_case.items():
        if firsts.count("turso") != firsts.count("rusqlite"):
            raise ValueError(f"unbalanced retained execution order for {key}")


def validate_invocation_list(
    invocations: list[list[str]],
    artifacts: dict[str, dict[str, Path]],
    samples: int,
    warmups: int,
) -> None:
    expected_total = len(SCENARIOS) * (samples + warmups) * len(BACKENDS)
    if len(invocations) != expected_total:
        raise ValueError("generated invocation count mismatch")
    allowed = {str(paths["unstripped"]) for paths in artifacts.values()}
    counts: dict[tuple[str, str, str, str], int] = {}
    for invocation in invocations:
        if len(invocation) != 9 or invocation[0] not in allowed:
            raise ValueError("generated invocation shape mismatch")
        if invocation[1::2] != ["--db", "--scenario", "--count", "--writers"]:
            raise ValueError("generated invocation arguments mismatch")
        key = (invocation[0], invocation[4], invocation[6], invocation[8])
        counts[key] = counts.get(key, 0) + 1
    if any(value != samples + warmups for value in counts.values()):
        raise ValueError("generated invocation case balance mismatch")
    if len(counts) != len(SCENARIOS) * len(BACKENDS):
        raise ValueError("generated invocation coverage mismatch")
    backend_by_binary = {
        str(paths["unstripped"]): backend for backend, paths in artifacts.items()
    }
    order_by_case: dict[tuple[str, str, str], list[str]] = {}
    for invocation in invocations:
        key = (invocation[4], invocation[6], invocation[8])
        order_by_case.setdefault(key, []).append(backend_by_binary[invocation[0]])
    for sequence in order_by_case.values():
        pairs = [sequence[index : index + 2] for index in range(0, len(sequence), 2)]
        if any(set(pair) != set(BACKENDS) for pair in pairs):
            raise ValueError("generated invocation pair does not contain both backends")
        warmup_first = [pair[0] for pair in pairs[:warmups]]
        retained_first = [pair[0] for pair in pairs[warmups:]]
        if warmup_first.count("turso") != warmup_first.count("rusqlite"):
            raise ValueError("unbalanced warmup order")
        if retained_first.count("turso") != retained_first.count("rusqlite"):
            raise ValueError("unbalanced retained invocation order")


def benchmark(
    binaries: dict[str, Path], samples: int, warmups: int
) -> tuple[list[dict], list[list[str]]]:
    records = []
    invocations = []
    for case in SCENARIOS:
        for index in range(warmups + samples):
            order = list(BACKENDS)
            if index % 2:
                order.reverse()
            for backend in order:
                record = run_one(binaries[backend], case)
                invocations.append(record["invocation"])
                if index >= warmups:
                    record["sample"] = index - warmups
                    record["execution_order"] = order
                    records.append(record)
    validate_checksums(records)
    validate_order_balance(records)
    return records, invocations


def validate_saved_payload(payload: dict) -> None:
    provenance = payload["provenance"]
    inputs = provenance["source_tree"]["files"]
    artifacts = {
        backend: {
            flavor: Path(details["path"])
            for flavor, details in paths.items()
        }
        for backend, paths in provenance["binaries"].items()
    }
    invocations = provenance["benchmark_invocations"]
    samples = payload["method"]["samples"]
    warmups = payload["method"]["warmups"]
    validate_balanced_counts(samples, warmups)
    validate_invocation_list(invocations, artifacts, samples, warmups)
    validate_provenance(provenance, ROOT, inputs, artifacts, invocations)
    validate_checksums(payload["records"])
    validate_order_balance(payload["records"])
    validate_summary(payload["summary"], samples)
    validate_summary_consistency(payload["records"], payload["summary"], samples)
    if payload.get("authoritative", {}).get("status") != "validated_at_generation":
        raise ValueError("summary is not marked authoritative")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args()
    if args.verify:
        payload = json.loads(args.verify.read_text())
        validate_saved_payload(payload)
        print(f"validated: {args.verify}")
        return
    try:
        validate_balanced_counts(args.samples, args.warmups)
    except ValueError as error:
        parser.error(str(error))
    commands: list[str] = []
    environment = collect_environment(commands)
    sizes, artifacts = build_and_size(commands)
    binaries = {backend: paths["unstripped"] for backend, paths in artifacts.items()}
    records, invocations = benchmark(binaries, args.samples, args.warmups)
    summary = summarize(records, args.samples)
    inputs = source_inputs()
    provenance = collect_provenance(ROOT, inputs, artifacts, invocations)
    validate_invocation_list(invocations, artifacts, args.samples, args.warmups)
    validate_provenance(provenance, ROOT, inputs, artifacts, invocations)
    payload = {
        "schema_version": 1,
        "authoritative": {
            "status": "validated_at_generation",
            "revalidate_command": "python3 run.py --verify results/summary.json",
        },
        "method": {
            "samples": args.samples,
            "warmups": args.warmups,
            "alternating_backend_order": True,
            "fresh_temporary_database_per_process": True,
            "tokio_runtime_worker_threads_both_binaries": 8,
            "primary_ratio_definition": "turso median / rusqlite median; >1 means Turso slower/larger",
            "benchmark_command_template": (
                "<artifact> --db <fresh-temp>/sample.db --scenario <name> "
                "--count <N> --writers <1|2|4|8>"
            ),
            "performance_artifact": "unstripped release binary; stripped copy is size-only",
            "commands": commands,
        },
        "environment": environment,
        "provenance": provenance,
        "sizes": sizes,
        "summary": summary,
        "records": records,
    }
    RESULTS.mkdir(exist_ok=True)
    validate_saved_payload(payload)
    (RESULTS / "summary.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(RESULTS / "summary.json")


if __name__ == "__main__":
    main()
