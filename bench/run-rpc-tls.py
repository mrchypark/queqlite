#!/usr/bin/env python3
"""Run and validate the three-run rhiza TLS RPC microbenchmark."""

import argparse
import hashlib
import itertools
import json
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path


CANDIDATES = (
    "https-json",
    "https-postcard",
    "https-prost",
    "tcp-tls-postcard",
    "quinn-rpc-stream",
    "quinn-lane",
)
OFFSETS = (0, 2, 4)
MEDIAN_FIELDS = (
    "throughput_ops_per_second",
    "p50_us",
    "p95_us",
    "p99_us",
    "p999_us",
)


def positive_csv(value):
    values = tuple(int(item) for item in value.split(","))
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("requires comma-separated positive integers")
    return values


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def expected_order(offset):
    shift = offset % len(CANDIDATES)
    return list(CANDIDATES[shift:] + CANDIDATES[:shift])


def validate_report(report, offset, operations, warmup, payloads, concurrencies):
    errors = []
    conditions = report.get("conditions", {})
    if report.get("schema_version") != 2:
        errors.append("schema_version must be 2")
    if conditions.get("candidate_order_offset") != offset:
        errors.append("candidate_order_offset mismatch")
    if conditions.get("candidates") != expected_order(offset):
        errors.append("effective candidate order mismatch")
    tls = conditions.get("tls", {})
    if tls.get("protocol_version") != "TLS 1.3 only":
        errors.append("TLS version is not pinned to TLS 1.3")
    if tls.get("https_alpn") != "http/1.1":
        errors.append("HTTPS ALPN mismatch")
    if tls.get("tcp_rpc_alpn") != "rhiza-bench/1" or tls.get("quinn_alpn") != "rhiza-bench/1":
        errors.append("RPC ALPN mismatch")
    if len(tls.get("certificate_sha256", "")) != 64:
        errors.append("certificate SHA-256 is missing")
    if tls.get("negotiation_observation") != (
        "HTTPS/TCP negotiated TLS 1.3 and ALPN observed; Quinn ALPN observed with TLS 1.3 "
        "enforced by QUIC invariant and config"
    ):
        errors.append("TLS negotiation is not observed and verified")

    expected = set(itertools.product(CANDIDATES, payloads, concurrencies))
    rows = report.get("metrics", [])
    actual = {
        (row.get("candidate"), row.get("payload_bytes"), row.get("concurrency"))
        for row in rows
    }
    if len(rows) != len(expected) or actual != expected:
        errors.append("metric row set is incomplete or duplicated")
    for row in rows:
        key = (row.get("candidate"), row.get("payload_bytes"), row.get("concurrency"))
        if row.get("attempts") != operations:
            errors.append(f"{key}: measured attempts mismatch")
        if row.get("warmup_attempts") != warmup:
            errors.append(f"{key}: warmup attempts mismatch")
        if row.get("errors") != 0 or row.get("warmup_errors") != 0:
            errors.append(f"{key}: request errors recorded")
        if row.get("tls_handshakes_during_measurement") != 0:
            errors.append(f"{key}: TLS handshake occurred during measurement")
        if not isinstance(row.get("tls_handshakes_before_measurement"), int) or row.get(
            "tls_handshakes_before_measurement", 0
        ) <= 0:
            errors.append(f"{key}: no successful TLS handshake was observed before measurement")
        if row.get("tls_negotiation_mismatches") != 0:
            errors.append(f"{key}: negotiated TLS/ALPN mismatch")
        expected_observation = (
            "server-side ALPN observed and verified; TLS 1.3 verified by QUIC invariant and "
            "TLS1.3-only config"
            if row.get("candidate") in ("quinn-rpc-stream", "quinn-lane")
            else "server-side negotiated TLS 1.3 and ALPN observed and verified"
        )
        if row.get("tls_negotiation_observation") != expected_observation:
            errors.append(f"{key}: negotiation was not observed and verified")
        if row.get("diagnostic_valid") is not True:
            errors.append(f"{key}: diagnostic_valid is false")
    if report.get("diagnostic_valid") is not True:
        errors.append("raw report diagnostic_valid is false")
    return errors


def aggregate(reports):
    grouped = {}
    for report in reports:
        for row in report["metrics"]:
            key = (row["candidate"], row["payload_bytes"], row["concurrency"])
            grouped.setdefault(key, []).append(row)
    result = []
    for (candidate, payload, concurrency), rows in sorted(grouped.items()):
        item = {
            "candidate": candidate,
            "payload_bytes": payload,
            "concurrency": concurrency,
            "runs": len(rows),
        }
        for field in MEDIAN_FIELDS:
            item[f"median_{field}"] = statistics.median(row[field] for row in rows)
        item["worst_max_us"] = max(row["max_us"] for row in rows)
        result.append(item)
    return result


def self_test():
    rows = []
    for value in (3.0, 1.0, 2.0):
        row = {
            "candidate": CANDIDATES[0],
            "payload_bytes": 128,
            "concurrency": 1,
            "max_us": value + 10,
        }
        row.update({field: value for field in MEDIAN_FIELDS})
        rows.append({"metrics": [row]})
    result = aggregate(rows)[0]
    assert result["median_p99_us"] == 2.0
    assert result["median_throughput_ops_per_second"] == 2.0
    assert result["worst_max_us"] == 13.0
    assert expected_order(2)[0] == CANDIDATES[2]
    print("run-rpc-tls self-test: ok")


def parse_args():
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=root / "target/release/rhiza-transport")
    parser.add_argument("--output-dir", type=Path, default=root / "rpc-tls-results")
    parser.add_argument("--warmup", type=int, default=4096)
    parser.add_argument("--operations", type=int, default=60000)
    parser.add_argument("--payloads", type=positive_csv, default=positive_csv("128,4096"))
    parser.add_argument("--concurrency", type=positive_csv, default=positive_csv("1,8,64"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    if args.warmup <= 0 or args.operations <= 0:
        raise SystemExit("warmup and operations must be positive")
    if args.warmup < max(args.concurrency):
        raise SystemExit("warmup must be at least maximum concurrency")
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary not found: {binary}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    binary_sha = sha256_file(binary)
    reports = []
    validation_errors = []
    run_order = []
    for run_index, offset in enumerate(OFFSETS, start=1):
        command = [
            str(binary),
            "--warmup", str(args.warmup),
            "--operations", str(args.operations),
            "--payloads", ",".join(map(str, args.payloads)),
            "--concurrency", ",".join(map(str, args.concurrency)),
            "--candidates", ",".join(CANDIDATES),
            "--candidate-order-offset", str(offset),
        ]
        completed = subprocess.run(command, check=False, capture_output=True, text=True)
        raw_path = args.output_dir / f"raw-{run_index}-offset-{offset}.json"
        raw_path.write_text(completed.stdout, encoding="utf-8")
        if completed.returncode != 0:
            raise SystemExit(f"run {run_index} failed ({completed.returncode}): {completed.stderr.strip()}")
        try:
            report = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise SystemExit(f"run {run_index} emitted invalid JSON: {error}") from error
        errors = validate_report(
            report, offset, args.operations, args.warmup, args.payloads, args.concurrency
        )
        validation_errors.extend(f"run {run_index}: {error}" for error in errors)
        reports.append(report)
        run_order.append({
            "run": run_index,
            "offset": offset,
            "effective_candidates": report["conditions"]["candidates"],
            "raw_file": raw_path.name,
            "certificate_sha256": report["conditions"]["tls"]["certificate_sha256"],
        })

    git_commit = reports[0]["environment"].get("git_commit")
    git_dirty = reports[0]["environment"].get("git_dirty")
    consistent_git = all(
        report["environment"].get("git_commit") == git_commit
        and report["environment"].get("git_dirty") == git_dirty
        for report in reports
    )
    diagnostic_valid = not validation_errors and consistent_git and len(reports) == 3
    blockers = []
    if validation_errors:
        blockers.append("raw-run validation failed")
    if not consistent_git:
        blockers.append("Git provenance changed between runs")
    if git_dirty is not False:
        blockers.append("Git tree is dirty or its state is unknown")
    summary = {
        "schema_version": 1,
        "diagnostic_valid": diagnostic_valid,
        "comparison_valid": diagnostic_valid and not blockers,
        "production_valid": False,
        "comparison_blockers": blockers,
        "validation_errors": validation_errors,
        "provenance": {
            "binary": {"path": str(binary), "sha256": binary_sha},
            "git": {"commit": git_commit, "dirty": git_dirty, "consistent_across_runs": consistent_git},
            "environment": {
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "rustc": reports[0]["environment"].get("rustc"),
                "os": reports[0]["environment"].get("os"),
                "cpu": reports[0]["environment"].get("cpu"),
                "cwd": os.getcwd(),
            },
            "run_order": run_order,
        },
        "aggregation": "median of three per-run metrics; worst max; latency samples are not pooled",
        "metrics": aggregate(reports),
    }
    summary_path = args.output_dir / "summary.json"
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    summary_path.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if diagnostic_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
