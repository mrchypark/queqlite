#!/usr/bin/env python3
"""Run balanced production RecorderRpc transport A/B pairs by security stratum."""

import argparse
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


STRATA = {
    "plaintext": ("tcp-postcard", "tcp-postcard-rpc"),
    "tls": ("tcp-tls-postcard", "tcp-tls-postcard-rpc"),
}
WORKLOADS = ("record", "inspect_record_summary")
RATIO_FIELDS = (
    "attempt_throughput_per_second",
    "success_throughput_per_second",
    "successful_latency_p50_us",
    "successful_latency_p95_us",
    "successful_latency_p99_us",
    "successful_latency_p999_us",
    "successful_latency_max_us",
)
MAX_DISTINCT_ERROR_MESSAGES = 8


def positive_csv(value):
    values = tuple(int(item) for item in value.split(","))
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("requires comma-separated positive integers")
    return values


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("requires a positive integer")
    return parsed


def security_csv(value):
    values = tuple(value.split(","))
    if not values or any(item not in STRATA for item in values) or len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("requires unique comma-separated values from plaintext,tls")
    return values


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def expected_order(stratum, offset):
    candidates = STRATA[stratum]
    shift = offset % len(candidates)
    return list(candidates[shift:] + candidates[:shift])


def validate_report(report, stratum, offset, operations, warmup, concurrencies):
    errors = []
    conditions = report.get("conditions", {})
    candidates = STRATA[stratum]
    if report.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if report.get("production_valid") is not True:
        errors.append("production_valid is false")
    if conditions.get("candidate_order_offset") != offset:
        errors.append("candidate_order_offset mismatch")
    if conditions.get("candidates") != expected_order(stratum, offset):
        errors.append("effective candidate order mismatch")
    if conditions.get("workloads") != list(WORKLOADS):
        errors.append("workload list mismatch")
    if conditions.get("concurrency") != list(concurrencies):
        errors.append("concurrency list mismatch")
    if conditions.get("postcard_rpc_lane_in_flight") != 8:
        errors.append("postcard-rpc lane in-flight metadata mismatch")
    if conditions.get("postcard_rpc_bridge_depth") != 128:
        errors.append("postcard-rpc bridge depth metadata mismatch")
    if conditions.get("recorder_server_operation_cap") != 32:
        errors.append("recorder server operation cap metadata mismatch")
    if "never aggregate" not in conditions.get("scope", ""):
        errors.append("framework-only non-aggregation scope is missing")

    rows = report.get("metrics", [])
    expected = set(itertools.product(candidates, WORKLOADS, concurrencies))
    actual = {
        (row.get("candidate"), row.get("workload"), row.get("concurrency"))
        for row in rows
    }
    if len(rows) != len(expected) or actual != expected:
        errors.append("metric row set is incomplete or duplicated")
    for row in rows:
        key = (row.get("candidate"), row.get("workload"), row.get("concurrency"))
        attempts = row.get("attempts")
        successes = row.get("successes")
        failures = row.get("errors")
        classified = sum(row.get("error_classes", {}).values())
        messages = row.get("error_messages", [])
        captured_messages = sum(entry.get("count", 0) for entry in messages)
        omitted_messages = row.get("unrecorded_error_message_occurrences", 0)
        if attempts != operations or successes + failures != attempts:
            errors.append(f"{key}: measured attempt accounting mismatch")
        if failures != classified:
            errors.append(f"{key}: error class accounting mismatch")
        if len(messages) > MAX_DISTINCT_ERROR_MESSAGES:
            errors.append(f"{key}: too many distinct error messages retained")
        if failures != captured_messages + omitted_messages:
            errors.append(f"{key}: error message accounting mismatch")
        if row.get("warmup_attempts") != warmup or row.get("warmup_errors") != 0:
            errors.append(f"{key}: warmup mismatch")
        if row.get("lane_prewarm_attempts") != 2 or row.get("lane_prewarm_errors") != 0:
            errors.append(f"{key}: both-lane prewarm mismatch")
        if row.get("diagnostic_valid") is not True:
            errors.append(f"{key}: diagnostic_valid is false")
        if row.get("length_prefix_bytes") != 4:
            errors.append(f"{key}: frame length prefix mismatch")
        if row.get("candidate") == candidates[1]:
            if row.get("postcard_rpc_header_bytes") != 13:
                errors.append(f"{key}: production Key8/Seq4 header mismatch")
        elif row.get("postcard_rpc_header_bytes") is not None:
            errors.append(f"{key}: legacy candidate reports a postcard-rpc header")
    if report.get("diagnostic_valid") is not True:
        errors.append("raw report diagnostic_valid is false")
    return errors


def validate_across_runs(reports, stratum, pairs):
    errors = []
    candidates = STRATA[stratum]
    expected_positions = [0] * pairs + [1] * pairs
    for candidate in candidates:
        positions = [report["conditions"]["candidates"].index(candidate) for report in reports]
        if sorted(positions) != expected_positions:
            errors.append(f"{candidate}: positions are not balanced")
    return errors


def grouped_rows(reports):
    grouped = {}
    for report in reports:
        for row in report["metrics"]:
            key = (row["candidate"], row["workload"], row["concurrency"])
            grouped.setdefault(key, []).append(row)
    return grouped


def aggregate(reports):
    result = []
    for (candidate, workload, concurrency), rows in sorted(grouped_rows(reports).items()):
        error_classes = {}
        error_messages = []
        omitted_error_messages = 0
        for row in rows:
            for name, count in row["error_classes"].items():
                error_classes[name] = error_classes.get(name, 0) + count
            omitted_error_messages += row["unrecorded_error_message_occurrences"]
            for observed in row["error_messages"]:
                retained = next(
                    (entry for entry in error_messages if entry["message"] == observed["message"]),
                    None,
                )
                if retained:
                    retained["count"] += observed["count"]
                elif len(error_messages) < MAX_DISTINCT_ERROR_MESSAGES:
                    error_messages.append(dict(observed))
                else:
                    omitted_error_messages += observed["count"]
        item = {
            "candidate": candidate,
            "workload": workload,
            "concurrency": concurrency,
            "runs": len(rows),
            "attempts_total": sum(row["attempts"] for row in rows),
            "successes_total": sum(row["successes"] for row in rows),
            "errors_total": sum(row["errors"] for row in rows),
            "error_classes_total": error_classes,
            "error_messages_total": error_messages,
            "unrecorded_error_message_occurrences_total": omitted_error_messages,
            "median_success_rate": statistics.median(
                row["successes"] / row["attempts"] for row in rows
            ),
        }
        for field in RATIO_FIELDS:
            values = [row[field] for row in rows if row[field] is not None]
            item[f"median_{field}"] = statistics.median(values) if values else None
        result.append(item)
    return result


def paired_ratios(reports, stratum):
    legacy, candidate = STRATA[stratum]
    indexes = [
        {
            (row["candidate"], row["workload"], row["concurrency"]): row
            for row in report["metrics"]
        }
        for report in reports
    ]
    cells = sorted(
        {(row["workload"], row["concurrency"]) for row in reports[0]["metrics"]}
    )
    result = []
    for workload, concurrency in cells:
        ratios = {}
        for field in RATIO_FIELDS:
            per_run = []
            for index in indexes:
                numerator = index[(candidate, workload, concurrency)][field]
                denominator = index[(legacy, workload, concurrency)][field]
                if numerator is not None and denominator not in (None, 0):
                    per_run.append(numerator / denominator)
            ratios[field] = None
            if per_run:
                median = statistics.median(per_run)
                ratios[field] = {
                    "direction": f"{candidate} / {legacy}",
                    "median": median,
                    "min": min(per_run),
                    "max": max(per_run),
                    "percent_delta": (median - 1.0) * 100.0,
                    "per_run": per_run,
                }
        result.append({"workload": workload, "concurrency": concurrency, "ratios": ratios})

    geometric_means = {}
    for field in RATIO_FIELDS:
        medians = [cell["ratios"][field]["median"] for cell in result if cell["ratios"][field]]
        geometric_means[field] = None
        if medians and all(value > 0 for value in medians):
            ratio = math.exp(sum(math.log(value) for value in medians) / len(medians))
            geometric_means[field] = {
                "direction": f"{candidate} / {legacy}",
                "ratio": ratio,
                "percent_delta": (ratio - 1.0) * 100.0,
            }
    return {"cells": result, "equal_weight_cell_geometric_mean": geometric_means}


def fake_report(stratum, offset, run):
    candidates = STRATA[stratum]
    metrics = []
    for candidate, workload in itertools.product(candidates, WORKLOADS):
        is_rpc = candidate == candidates[1]
        attempts = 100
        failures = 10 if is_rpc else 0
        metrics.append(
            {
                "candidate": candidate,
                "workload": workload,
                "concurrency": 4,
                "attempts": attempts,
                "successes": attempts - failures,
                "errors": failures,
                "error_classes": {"bridge_overloaded": failures} if failures else {},
                "error_messages": (
                    [{"message": "QuePaxa io failed: recorder postcard-rpc bridge overloaded", "count": failures}]
                    if failures else []
                ),
                "unrecorded_error_message_occurrences": 0,
                "warmup_attempts": 20,
                "warmup_errors": 0,
                "lane_prewarm_attempts": 2,
                "lane_prewarm_errors": 0,
                "diagnostic_valid": True,
                "length_prefix_bytes": 4,
                "postcard_rpc_header_bytes": 13 if is_rpc else None,
                "attempt_throughput_per_second": 1000.0 + run,
                "success_throughput_per_second": (900.0 if is_rpc else 1000.0) + run,
                "successful_latency_p50_us": 12.0 if is_rpc else 10.0,
                "successful_latency_p95_us": 12.0 if is_rpc else 10.0,
                "successful_latency_p99_us": 12.0 if is_rpc else 10.0,
                "successful_latency_p999_us": 12.0 if is_rpc else 10.0,
                "successful_latency_max_us": 12.0 if is_rpc else 10.0,
            }
        )
    return {
        "schema_version": 1,
        "production_valid": True,
        "diagnostic_valid": True,
        "conditions": {
            "candidate_order_offset": offset,
            "candidates": expected_order(stratum, offset),
            "workloads": list(WORKLOADS),
            "concurrency": [4],
            "postcard_rpc_lane_in_flight": 8,
            "postcard_rpc_bridge_depth": 128,
            "recorder_server_operation_cap": 32,
            "scope": "production only; never aggregate with framework-only",
        },
        "metrics": metrics,
    }


def self_test():
    reports = [fake_report("plaintext", offset, run) for run, offset in enumerate((0, 1) * 3)]
    for report, offset in zip(reports, (0, 1) * 3):
        assert not validate_report(report, "plaintext", offset, 100, 20, (4,))
    assert not validate_across_runs(reports, "plaintext", 3)
    comparison = paired_ratios(reports, "plaintext")
    assert math.isclose(comparison["cells"][0]["ratios"]["successful_latency_p99_us"]["median"], 1.2)
    assert aggregate(reports)[0]["runs"] == 6
    print("run-recorder-transport self-test: ok")


def parse_args():
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--binary", type=Path, default=root / "target/release/rhiza-recorder-transport"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=root / "target/recorder-transport-results"
    )
    parser.add_argument("--warmup", type=positive_int, default=1000)
    parser.add_argument("--operations", type=positive_int, default=10000)
    parser.add_argument("--concurrency", type=positive_csv, default=positive_csv("1,4,32"))
    parser.add_argument("--security", type=security_csv, default=security_csv("plaintext"))
    parser.add_argument("--pairs", type=positive_int, default=3)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def run_stratum(args, binary, stratum):
    output_dir = args.output_dir / stratum
    output_dir.mkdir(parents=True, exist_ok=True)
    reports = []
    validation_errors = []
    run_order = []
    for run_index, offset in enumerate((0, 1) * args.pairs, start=1):
        command = [
            str(binary),
            "--warmup", str(args.warmup),
            "--operations", str(args.operations),
            "--concurrency", ",".join(map(str, args.concurrency)),
            "--candidates", ",".join(STRATA[stratum]),
            "--candidate-order-offset", str(offset),
        ]
        completed = subprocess.run(command, check=False, capture_output=True, text=True)
        raw_path = output_dir / f"raw-{run_index}-offset-{offset}.json"
        raw_path.write_text(completed.stdout, encoding="utf-8")
        if completed.returncode != 0:
            raise SystemExit(
                f"{stratum} run {run_index} failed ({completed.returncode}): {completed.stderr.strip()}"
            )
        try:
            report = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise SystemExit(f"{stratum} run {run_index} emitted invalid JSON: {error}") from error
        validation_errors.extend(
            f"run {run_index}: {error}"
            for error in validate_report(
                report, stratum, offset, args.operations, args.warmup, args.concurrency
            )
        )
        reports.append(report)
        run_order.append(
            {
                "run": run_index,
                "pair": (run_index + 1) // 2,
                "offset": offset,
                "effective_candidates": report["conditions"]["candidates"],
                "raw_file": str(raw_path.relative_to(args.output_dir)),
            }
        )
    validation_errors.extend(
        f"cross-run: {error}"
        for error in validate_across_runs(reports, stratum, args.pairs)
    )
    return reports, validation_errors, run_order


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary not found: {binary}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    strata = {}
    all_reports = []
    validation_errors = []
    for stratum in args.security:
        reports, errors, run_order = run_stratum(args, binary, stratum)
        all_reports.extend(reports)
        validation_errors.extend(f"{stratum}: {error}" for error in errors)
        strata[stratum] = {
            "ratio_direction": f"{STRATA[stratum][1]} / {STRATA[stratum][0]}",
            "comparison": paired_ratios(reports, stratum) if not errors else None,
            "metrics": aggregate(reports),
            "run_order": run_order,
        }

    git_commit = all_reports[0]["environment"].get("git_commit")
    git_dirty = all_reports[0]["environment"].get("git_dirty")
    consistent_git = all(
        report["environment"].get("git_commit") == git_commit
        and report["environment"].get("git_dirty") == git_dirty
        for report in all_reports
    )
    diagnostic_valid = not validation_errors and consistent_git
    blockers = []
    if validation_errors:
        blockers.append("raw-run validation failed")
    if not consistent_git:
        blockers.append("Git provenance changed between runs")
    if git_dirty is not False:
        blockers.append("Git tree is dirty or its state is unknown")
    comparison_valid = diagnostic_valid and not blockers
    summary = {
        "schema_version": 1,
        "diagnostic_valid": diagnostic_valid,
        "comparison_valid": comparison_valid,
        "publishable": comparison_valid,
        "production_valid": True,
        "comparison_scope": "production RecorderRpc adapters only, separated by security stratum; framework-only rhiza-transport results are never aggregated",
        "comparison_blockers": blockers,
        "validation_errors": validation_errors,
        "aggregation": f"{args.pairs} balanced A/B pairs per stratum ({args.pairs * 2} runs); paired per-run ratios and per-cell medians; samples are not pooled",
        "strata": strata,
        "provenance": {
            "binary": {"path": str(binary), "sha256": sha256_file(binary)},
            "git": {
                "commit": git_commit,
                "dirty": git_dirty,
                "consistent_across_runs": consistent_git,
            },
            "environment": {
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "rustc": all_reports[0]["environment"].get("rustc"),
                "os": all_reports[0]["environment"].get("os"),
                "cpu": all_reports[0]["environment"].get("cpu"),
                "cwd": os.getcwd(),
            },
        },
    }
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    (args.output_dir / "summary.json").write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if diagnostic_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
