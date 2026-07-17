#!/usr/bin/env python3
"""Run a balanced tcp-postcard versus tcp-postcard-rpc framework A/B."""

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


CANDIDATES = ("tcp-postcard", "tcp-postcard-rpc")
RATIO_FIELDS = (
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


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("requires a positive integer")
    return parsed


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
    if conditions.get("postcard_rpc_version") != "0.12.1 (use-std)":
        errors.append("postcard-rpc version/features mismatch")
    if conditions.get("postcard_rpc_endpoint_paths") != [
        "rhiza/record",
        "rhiza/record/replicate",
    ]:
        errors.append("postcard-rpc endpoint paths mismatch")

    rows = report.get("metrics", [])
    expected = set(itertools.product(CANDIDATES, payloads, concurrencies))
    actual = {
        (row.get("candidate"), row.get("payload_bytes"), row.get("concurrency"))
        for row in rows
    }
    if len(rows) != len(expected) or actual != expected:
        errors.append("metric row set is incomplete or duplicated")
    for row in rows:
        key = (row.get("candidate"), row.get("payload_bytes"), row.get("concurrency"))
        if row.get("attempts") != operations or row.get("warmup_attempts") != warmup:
            errors.append(f"{key}: attempt count mismatch")
        if row.get("errors") != 0 or row.get("warmup_errors") != 0:
            errors.append(f"{key}: request errors recorded")
        if row.get("diagnostic_valid") is not True:
            errors.append(f"{key}: diagnostic_valid is false")
        if row.get("codec") != "application/vnd.rhiza.postcard":
            errors.append(f"{key}: codec mismatch")
        if row.get("transport") != "length-prefixed TCP" or row.get("tls") != "none":
            errors.append(f"{key}: transport/security stratum mismatch")
        if row.get("length_prefix_bytes_per_frame") != 4:
            errors.append(f"{key}: length prefix is not four bytes")
        if row.get("candidate") == "tcp-postcard-rpc":
            if row.get("postcard_rpc_request_header_bytes") != 6:
                errors.append(f"{key}: postcard-rpc request header mismatch")
            if row.get("postcard_rpc_response_header_bytes") != 6:
                errors.append(f"{key}: postcard-rpc response header mismatch")
            if "multiplexed" not in row.get("topology", ""):
                errors.append(f"{key}: multiplexed session topology not reported")
        elif row.get("postcard_rpc_request_header_bytes") is not None or row.get(
            "postcard_rpc_response_header_bytes"
        ) is not None:
            errors.append(f"{key}: raw postcard unexpectedly reports framework headers")
    if report.get("diagnostic_valid") is not True:
        errors.append("raw report diagnostic_valid is false")
    return errors


def validate_across_runs(reports, pairs):
    errors = []
    expected_positions = [0] * pairs + [1] * pairs
    for candidate in CANDIDATES:
        positions = [report["conditions"]["candidates"].index(candidate) for report in reports]
        if sorted(positions) != expected_positions:
            errors.append(f"{candidate}: candidate positions are not balanced")

    sizes = {}
    for report in reports:
        index = {
            (row["candidate"], row["payload_bytes"], row["concurrency"]): row
            for row in report["metrics"]
        }
        for payload, concurrency in itertools.product(
            report["conditions"]["payload_bytes"], report["conditions"]["concurrency"]
        ):
            raw = index[("tcp-postcard", payload, concurrency)]
            rpc = index[("tcp-postcard-rpc", payload, concurrency)]
            if rpc["encoded_request_bytes"] - raw["encoded_request_bytes"] != 6:
                errors.append(f"{payload}/{concurrency}: request size delta is not header size")
            if rpc["encoded_response_bytes"] - raw["encoded_response_bytes"] != 6:
                errors.append(f"{payload}/{concurrency}: response size delta is not header size")
            sizes.setdefault((payload, concurrency), set()).add(
                (
                    raw["encoded_request_bytes"],
                    raw["encoded_response_bytes"],
                    rpc["encoded_request_bytes"],
                    rpc["encoded_response_bytes"],
                )
            )
    for key, observed in sizes.items():
        if len(observed) != 1:
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
        for field in RATIO_FIELDS:
            item[f"median_{field}"] = statistics.median(row[field] for row in rows)
        item["worst_max_us"] = max(row["max_us"] for row in rows)
        result.append(item)
    return result


def paired_ratios(reports):
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
    result = []
    for payload, concurrency in cells:
        ratios = {}
        for field in RATIO_FIELDS:
            per_run = [
                index[("tcp-postcard-rpc", payload, concurrency)][field]
                / index[("tcp-postcard", payload, concurrency)][field]
                for index in indexes
            ]
            median = statistics.median(per_run)
            ratios[field] = {
                "direction": "tcp-postcard-rpc / tcp-postcard",
                "median": median,
                "min": min(per_run),
                "max": max(per_run),
                "percent_delta": (median - 1.0) * 100.0,
                "per_run": per_run,
            }
        result.append(
            {"payload_bytes": payload, "concurrency": concurrency, "ratios": ratios}
        )
    geometric_means = {}
    for field in RATIO_FIELDS:
        medians = [cell["ratios"][field]["median"] for cell in result]
        ratio = math.exp(sum(math.log(value) for value in medians) / len(medians))
        geometric_means[field] = {
            "direction": "tcp-postcard-rpc / tcp-postcard",
            "ratio": ratio,
            "percent_delta": (ratio - 1.0) * 100.0,
        }
    return {"cells": result, "equal_weight_cell_geometric_mean": geometric_means}


def self_test():
    reports = []
    for run, offset in enumerate((0, 1, 0, 1, 0, 1), start=1):
        metrics = []
        for candidate, throughput, p99, request_bytes, response_bytes in (
            ("tcp-postcard", 100.0, 10.0, 201, 72),
            ("tcp-postcard-rpc", 80.0 + run, 12.0, 207, 78),
        ):
            row = {
                "candidate": candidate,
                "payload_bytes": 128,
                "concurrency": 8,
                "encoded_request_bytes": request_bytes,
                "encoded_response_bytes": response_bytes,
                "max_us": p99 + 1,
            }
            row.update(
                {field: throughput if field == RATIO_FIELDS[0] else p99 for field in RATIO_FIELDS}
            )
            metrics.append(row)
        reports.append(
            {
                "conditions": {
                    "candidates": expected_order(offset),
                    "payload_bytes": [128],
                    "concurrency": [8],
                },
                "metrics": metrics,
            }
        )
    assert not validate_across_runs(reports, 3)
    cell = paired_ratios(reports)["cells"][0]
    assert math.isclose(cell["ratios"]["p99_us"]["median"], 1.2)
    assert math.isclose(cell["ratios"]["throughput_ops_per_second"]["median"], 0.835)
    assert aggregate(reports)[0]["runs"] == 6
    print("run-rpc-framework self-test: ok")


def parse_args():
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=root / "target/release/rhiza-transport")
    parser.add_argument("--output-dir", type=Path, default=root / "rpc-framework-results")
    parser.add_argument("--warmup", type=positive_int, default=4096)
    parser.add_argument("--operations", type=positive_int, default=60000)
    parser.add_argument("--payloads", type=positive_csv, default=positive_csv("128,4096"))
    parser.add_argument("--concurrency", type=positive_csv, default=positive_csv("1,8,64"))
    parser.add_argument("--pairs", type=positive_int, default=3)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    if args.warmup < max(args.concurrency):
        raise SystemExit("warmup must be at least maximum concurrency")
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary not found: {binary}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    reports = []
    validation_errors = []
    run_order = []
    for run_index, offset in enumerate((0, 1) * args.pairs, start=1):
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
        validation_errors.extend(
            f"run {run_index}: {error}"
            for error in validate_report(
                report, offset, args.operations, args.warmup, args.payloads, args.concurrency
            )
        )
        reports.append(report)
        run_order.append(
            {
                "run": run_index,
                "pair": (run_index + 1) // 2,
                "offset": offset,
                "effective_candidates": report["conditions"]["candidates"],
                "raw_file": raw_path.name,
            }
        )

    validation_errors.extend(
        f"cross-run: {error}" for error in validate_across_runs(reports, args.pairs)
    )
    git_commit = reports[0]["environment"].get("git_commit")
    git_dirty = reports[0]["environment"].get("git_dirty")
    consistent_git = all(
        report["environment"].get("git_commit") == git_commit
        and report["environment"].get("git_dirty") == git_dirty
        for report in reports
    )
    diagnostic_valid = not validation_errors and consistent_git and len(reports) == args.pairs * 2
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
        "production_valid": False,
        "comparison_scope": "local plaintext TCP framework overhead only",
        "ratio_direction": "tcp-postcard-rpc / tcp-postcard",
        "comparison_blockers": blockers,
        "validation_errors": validation_errors,
        "comparison": paired_ratios(reports) if diagnostic_valid else None,
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
                "rustc": reports[0]["environment"].get("rustc"),
                "os": reports[0]["environment"].get("os"),
                "cpu": reports[0]["environment"].get("cpu"),
                "cwd": os.getcwd(),
            },
            "run_order": run_order,
        },
        "aggregation": f"{args.pairs} balanced A/B pairs ({args.pairs * 2} runs); paired per-run ratios, per-cell medians, equal-cell geometric means; samples are not pooled",
        "metrics": aggregate(reports),
    }
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    (args.output_dir / "summary.json").write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if diagnostic_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
