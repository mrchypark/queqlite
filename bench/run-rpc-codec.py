#!/usr/bin/env python3
"""Run and validate the TCP Postcard/Prost codec comparison."""

import argparse
from collections import Counter
import hashlib
import itertools
import json
import math
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path


CANDIDATES = (
    "tcp-postcard",
    "tcp-prost",
    "tcp-tls-postcard",
    "tcp-tls-prost",
)
OFFSETS = (0, 1, 2, 3)
MEDIAN_FIELDS = (
    "throughput_ops_per_second",
    "p50_us",
    "p95_us",
    "p99_us",
    "p999_us",
)
CODECS = {
    "tcp-postcard": "application/vnd.rhiza.postcard",
    "tcp-prost": "application/vnd.rhiza.protobuf",
    "tcp-tls-postcard": "application/vnd.rhiza.postcard",
    "tcp-tls-prost": "application/vnd.rhiza.protobuf",
}
TLS_CANDIDATES = {"tcp-tls-postcard", "tcp-tls-prost"}
COMPARISON_PAIRS = (
    ("plaintext", "tcp-postcard", "tcp-prost"),
    ("tls", "tcp-tls-postcard", "tcp-tls-prost"),
)
RATIO_FIELDS = MEDIAN_FIELDS


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


def positive_finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


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
    if tls.get("tcp_rpc_alpn") != "rhiza-bench/1":
        errors.append("TCP RPC ALPN mismatch")
    if len(tls.get("certificate_sha256", "")) != 64:
        errors.append("certificate SHA-256 is missing")

    expected = set(itertools.product(CANDIDATES, payloads, concurrencies))
    rows = report.get("metrics", [])
    actual = [
        (row.get("candidate"), row.get("payload_bytes"), row.get("concurrency"))
        for row in rows
    ]
    counts = Counter(actual)
    if set(actual) != expected or any(count != 1 for count in counts.values()):
        errors.append("metric row set is incomplete or duplicated")
    for row in rows:
        candidate = row.get("candidate")
        key = (candidate, row.get("payload_bytes"), row.get("concurrency"))
        if row.get("codec") != CODECS.get(candidate):
            errors.append(f"{key}: codec mismatch")
        if row.get("transport") != "length-prefixed TCP":
            errors.append(f"{key}: transport mismatch")
        if row.get("topology") != "one warmed persistent connection per worker":
            errors.append(f"{key}: topology mismatch")
        if row.get("attempts") != operations or row.get("warmup_attempts") != warmup:
            errors.append(f"{key}: attempt count mismatch")
        if row.get("successes") != row.get("attempts"):
            errors.append(f"{key}: successes do not equal attempts")
        if row.get("errors") != 0 or row.get("warmup_errors") != 0:
            errors.append(f"{key}: request errors recorded")
        if row.get("diagnostic_valid") is not True:
            errors.append(f"{key}: diagnostic_valid is false")
        if not isinstance(row.get("encoded_request_bytes"), int) or not 4 < row.get(
            "encoded_request_bytes", 0
        ) <= 1024 * 1024 + 4:
            errors.append(f"{key}: request frame is invalid")
        if not isinstance(row.get("encoded_response_bytes"), int) or not 4 < row.get(
            "encoded_response_bytes", 0
        ) <= 1024 * 1024 + 4:
            errors.append(f"{key}: response frame is invalid")
        for field in (*RATIO_FIELDS, "wall_seconds", "max_us"):
            if not positive_finite(row.get(field)):
                errors.append(f"{key}: {field} must be positive and finite")
        if candidate in TLS_CANDIDATES:
            if row.get("tls") != "TLS server authentication; shared benchmark certificate":
                errors.append(f"{key}: TLS authentication mismatch")
            if row.get("tls_handshakes_during_measurement") != 0:
                errors.append(f"{key}: TLS handshake occurred during measurement")
            if not isinstance(row.get("tls_handshakes_before_measurement"), int) or row.get(
                "tls_handshakes_before_measurement", 0
            ) <= 0:
                errors.append(f"{key}: no TLS handshake observed before measurement")
            if row.get("tls_negotiation_mismatches") != 0:
                errors.append(f"{key}: TLS/ALPN mismatch")
        else:
            if row.get("tls") != "none":
                errors.append(f"{key}: plaintext candidate unexpectedly reports TLS")
            for field in (
                "tls_handshakes_before_measurement",
                "tls_handshakes_after_measurement",
                "tls_handshakes_during_measurement",
                "tls_negotiation_mismatches",
            ):
                if row.get(field) is not None:
                    errors.append(f"{key}: plaintext {field} must be null")
    if report.get("diagnostic_valid") is not True:
        errors.append("raw report diagnostic_valid is false")
    return errors


def validate_across_runs(reports):
    errors = []
    if len(reports) != len(CANDIDATES):
        return [f"expected {len(CANDIDATES)} reports for balanced candidate positions"]
    positions = {candidate: [] for candidate in CANDIDATES}
    for report in reports:
        order = report.get("conditions", {}).get("candidates", [])
        for position, candidate in enumerate(order):
            if candidate in positions:
                positions[candidate].append(position)
    expected_positions = list(range(len(CANDIDATES)))
    for candidate, actual_positions in positions.items():
        if sorted(actual_positions) != expected_positions:
            errors.append(f"{candidate}: candidate positions are not fully balanced")

    encoded_sizes = {}
    for report in reports:
        for row in report.get("metrics", []):
            key = (row.get("candidate"), row.get("payload_bytes"), row.get("concurrency"))
            encoded_sizes.setdefault(key, set()).add(
                (row.get("encoded_request_bytes"), row.get("encoded_response_bytes"))
            )
    for key, sizes in encoded_sizes.items():
        if len(sizes) != 1:
            errors.append(f"{key}: encoded sizes changed between runs")
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
            "encoded_request_bytes": rows[0]["encoded_request_bytes"],
            "encoded_response_bytes": rows[0]["encoded_response_bytes"],
        }
        for field in MEDIAN_FIELDS:
            item[f"median_{field}"] = statistics.median(row[field] for row in rows)
        item["worst_max_us"] = max(row["max_us"] for row in rows)
        result.append(item)
    return result


def paired_comparison_groups(reports, blockers):
    indexes = [
        {
            (row["candidate"], row["payload_bytes"], row["concurrency"]): row
            for row in report["metrics"]
        }
        for report in reports
    ]
    cells = sorted(
        {
            (row["payload_bytes"], row["concurrency"])
            for row in reports[0]["metrics"]
        }
    )
    groups = []
    for stratum, postcard, prost in COMPARISON_PAIRS:
        group_cells = []
        for payload, concurrency in cells:
            per_field = {field: [] for field in RATIO_FIELDS}
            for run, index in enumerate(indexes, start=1):
                postcard_row = index[(postcard, payload, concurrency)]
                prost_row = index[(prost, payload, concurrency)]
                for field in RATIO_FIELDS:
                    per_field[field].append(
                        {
                            "run": run,
                            "ratio": prost_row[field] / postcard_row[field],
                        }
                    )
            ratios = {}
            for field, per_run in per_field.items():
                values = [item["ratio"] for item in per_run]
                median = statistics.median(values)
                ratios[field] = {
                    "median": median,
                    "min": min(values),
                    "max": max(values),
                    "per_run": per_run,
                    "percent_delta": (median - 1.0) * 100.0,
                }
            group_cells.append(
                {
                    "payload_bytes": payload,
                    "concurrency": concurrency,
                    "ratios": ratios,
                }
            )
        geometric_means = {}
        for field in RATIO_FIELDS:
            medians = [cell["ratios"][field]["median"] for cell in group_cells]
            ratio = math.exp(sum(math.log(value) for value in medians) / len(medians))
            geometric_means[field] = {
                "ratio": ratio,
                "percent_delta": (ratio - 1.0) * 100.0,
            }
        groups.append(
            {
                "name": f"{stratum}_postcard_vs_prost",
                "security_stratum": stratum,
                "baseline": postcard,
                "candidate": prost,
                "ratio_direction": "prost/postcard",
                "interpretation": "throughput above 1 favors Prost; latency below 1 favors Prost",
                "valid": not blockers,
                "blockers": list(blockers),
                "cells": group_cells,
                "equal_weight_cell_geometric_mean": geometric_means,
            }
        )
    return groups


def blocked_comparison_groups(blockers):
    return [
        {
            "name": f"{stratum}_postcard_vs_prost",
            "security_stratum": stratum,
            "baseline": postcard,
            "candidate": prost,
            "ratio_direction": "prost/postcard",
            "interpretation": "throughput above 1 favors Prost; latency below 1 favors Prost",
            "valid": False,
            "blockers": list(blockers),
            "cells": [],
            "equal_weight_cell_geometric_mean": {},
        }
        for stratum, postcard, prost in COMPARISON_PAIRS
    ]


def self_test():
    reports = []
    small_cell_ratios = (1.0, 2.0, 3.0, 4.0)
    for run, offset in enumerate(OFFSETS):
        rows = []
        for candidate in CANDIDATES:
            for payload, concurrency in ((128, 1), (4096, 64)):
                ratio = 1.0
                if candidate == "tcp-prost":
                    ratio = small_cell_ratios[run] if payload == 128 else 0.5
                value = 100.0 * ratio
                row = {
                    "candidate": candidate,
                    "payload_bytes": payload,
                    "concurrency": concurrency,
                    "encoded_request_bytes": payload + 12,
                    "encoded_response_bytes": 50,
                    "max_us": value + 10.0,
                }
                row.update({field: value for field in MEDIAN_FIELDS})
                rows.append(row)
        reports.append(
            {
                "conditions": {"candidates": expected_order(offset)},
                "metrics": rows,
            }
        )
    assert validate_across_runs(reports) == []
    result = aggregate(reports)
    assert len(result) == len(CANDIDATES) * 2
    groups = paired_comparison_groups(reports, [])
    plaintext = groups[0]
    small = next(cell for cell in plaintext["cells"] if cell["payload_bytes"] == 128)
    large = next(cell for cell in plaintext["cells"] if cell["payload_bytes"] == 4096)
    assert small["ratios"]["throughput_ops_per_second"]["median"] == 2.5
    assert large["concurrency"] == 64
    assert large["ratios"]["throughput_ops_per_second"]["median"] == 0.5
    assert len(small["ratios"]["p99_us"]["per_run"]) == 4
    geometric_mean = plaintext["equal_weight_cell_geometric_mean"][
        "throughput_ops_per_second"
    ]["ratio"]
    assert math.isclose(geometric_mean, math.sqrt(2.5 * 0.5))
    assert small["ratios"]["throughput_ops_per_second"]["median"] > 1
    assert large["ratios"]["throughput_ops_per_second"]["median"] < 1
    assert expected_order(1)[0] == CANDIDATES[1]
    print("run-rpc-codec self-test: ok")


def parse_args():
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=root / "target/release/rhiza-transport")
    parser.add_argument("--output-dir", type=Path, default=root / "rpc-codec-results")
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
            raise SystemExit(
                f"run {run_index} failed ({completed.returncode}): {completed.stderr.strip()}"
            )
        try:
            report = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise SystemExit(f"run {run_index} emitted invalid JSON: {error}") from error
        errors = validate_report(
            report, offset, args.operations, args.warmup, args.payloads, args.concurrency
        )
        validation_errors.extend(f"run {run_index}: {error}" for error in errors)
        reports.append(report)
        run_order.append(
            {
                "run": run_index,
                "offset": offset,
                "effective_candidates": report["conditions"]["candidates"],
                "raw_file": raw_path.name,
            }
        )

    validation_errors.extend(
        f"cross-run: {error}" for error in validate_across_runs(reports)
    )

    git_commit = reports[0]["environment"].get("git_commit")
    git_dirty = reports[0]["environment"].get("git_dirty")
    consistent_git = all(
        report["environment"].get("git_commit") == git_commit
        and report["environment"].get("git_dirty") == git_dirty
        for report in reports
    )
    diagnostic_valid = not validation_errors and len(reports) == 4
    blockers = []
    if validation_errors:
        blockers.append("raw-run validation failed")
    if not consistent_git:
        blockers.append("Git provenance changed between runs")
    if git_dirty is not False:
        blockers.append("Git tree is dirty or its state is unknown")
    comparison_groups = (
        paired_comparison_groups(reports, blockers)
        if diagnostic_valid
        else blocked_comparison_groups(blockers)
    )
    comparison_valid = (
        diagnostic_valid
        and consistent_git
        and git_dirty is False
        and all(group["valid"] for group in comparison_groups)
    )
    summary = {
        "schema_version": 2,
        "diagnostic_valid": diagnostic_valid,
        "comparison_valid": comparison_valid,
        "comparison_scope": "declared codec pairs within one security stratum only",
        "cross_security_comparison_valid": False,
        "production_valid": False,
        "comparison_blockers": blockers,
        "comparison_groups": comparison_groups,
        "validation_errors": validation_errors,
        "provenance": {
            "binary": {"path": str(binary), "sha256": binary_sha},
            "git": {
                "commit": git_commit,
                "dirty": git_dirty,
                "consistent_across_runs": consistent_git,
            },
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
        "aggregation": "median of four per-run metrics; worst max; latency samples are not pooled",
        "metrics": aggregate(reports),
    }
    summary_path = args.output_dir / "summary.json"
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    summary_path.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if diagnostic_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
