import importlib.util
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("benchmark_run", ROOT / "run.py")
RUN = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(RUN)


class RunnerTests(unittest.TestCase):
    def test_size_parser_accepts_binary_units(self):
        self.assertEqual(RUN.parse_human_size("1 B"), 1)
        self.assertEqual(RUN.parse_human_size("1.50 KiB"), 1536)
        self.assertEqual(RUN.parse_human_size("2 MiB"), 2 * 1024 * 1024)
        with self.assertRaises(ValueError):
            RUN.parse_human_size("12 MB")

    def test_summary_validation_rejects_too_few_samples(self):
        metric = {"median": 2, "min": 1, "max": 3}
        backend = {
            "samples": 5,
            "metrics": {"operation_ns": metric},
        }
        with self.assertRaisesRegex(ValueError, "fewer than 6"):
            RUN.validate_summary(
                {
                    "case": {
                        "primary_metric": "operation_ns",
                        "turso": backend,
                        "rusqlite": backend,
                    }
                }
            )

    def test_balanced_counts_require_even_samples_and_warmups(self):
        RUN.validate_balanced_counts(8, 2)
        for samples, warmups in ((7, 2), (8, 1), (5, 2)):
            with self.subTest(samples=samples, warmups=warmups):
                with self.assertRaises(ValueError):
                    RUN.validate_balanced_counts(samples, warmups)

    def test_generated_invocations_balance_warmup_and_retained_first_position(self):
        artifacts = {
            "turso": {
                "unstripped": Path("/tmp/turso"),
                "stripped": Path("/tmp/turso.stripped"),
            },
            "rusqlite": {
                "unstripped": Path("/tmp/rusqlite"),
                "stripped": Path("/tmp/rusqlite.stripped"),
            },
        }

        def invocation(backend, case, serial):
            return [
                str(artifacts[backend]["unstripped"]),
                "--db",
                f"/tmp/db-{serial}",
                "--scenario",
                case["scenario"],
                "--count",
                str(case["count"]),
                "--writers",
                str(case["writers"]),
            ]

        balanced = []
        unbalanced_warmups = []
        for case_index, case in enumerate(RUN.SCENARIOS):
            for index in range(10):
                order = ["turso", "rusqlite"] if index % 2 == 0 else ["rusqlite", "turso"]
                for backend in order:
                    balanced.append(invocation(backend, case, (case_index, index, backend)))
                bad_order = ["turso", "rusqlite"] if index < 2 else order
                for backend in bad_order:
                    unbalanced_warmups.append(
                        invocation(backend, case, (case_index, index, backend, "bad"))
                    )
        RUN.validate_invocation_list(balanced, artifacts, 8, 2)
        with self.assertRaisesRegex(ValueError, "warmup order"):
            RUN.validate_invocation_list(unbalanced_warmups, artifacts, 8, 2)

    def test_warm_reopen_uses_open_as_primary_and_zero_operation(self):
        records = []
        for backend in RUN.BACKENDS:
            for _ in range(6):
                records.append(
                    {
                        "backend": backend,
                        "scenario": "warm_open",
                        "count": 1,
                        "writers": 1,
                        "external_process_ns": 20,
                        "runtime_init_ns": 2,
                        "open_ns": 7,
                        "setup_ns": 3,
                        "operation_ns": 0,
                        "successes": 1,
                        "errors": 0,
                        "busy": 0,
                        "journal_mode": "wal",
                        "synchronous": "2",
                    }
                )
        summary = RUN.summarize(records, 6)
        case = summary["warm_open:writers=1:count=1"]
        self.assertEqual(case["primary_metric"], "open_ns")
        self.assertEqual(case["turso"]["metrics"]["operation_ns"]["median"], 0)
        self.assertEqual(case["rusqlite"]["metrics"]["operation_ns"]["median"], 0)
        RUN.validate_summary_consistency(records, summary, 6)
        case["turso"]["metrics"]["open_ns"]["median"] = 999
        with self.assertRaisesRegex(ValueError, "does not match raw records"):
            RUN.validate_summary_consistency(records, summary, 6)

    def test_source_tree_digest_and_file_hash_detect_input_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "a.txt").write_text("alpha")
            (root / "b.txt").write_text("beta")
            files = ["a.txt", "b.txt"]
            first = RUN.source_tree_digest(root, files)
            self.assertEqual(first, RUN.source_tree_digest(root, files))
            self.assertEqual(RUN.sha256_file(root / "a.txt"), RUN.sha256_file(root / "a.txt"))
            (root / "b.txt").write_text("changed")
            self.assertNotEqual(first, RUN.source_tree_digest(root, files))

    def test_provenance_validation_rejects_changed_binary_or_invocation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name, value in {
                "Cargo.toml": "manifest",
                "Cargo.lock": "lock",
                "run.py": "runner",
                "turso": "binary-a",
                "turso.stripped": "binary-b",
            }.items():
                (root / name).write_text(value)
            inputs = ["Cargo.toml", "Cargo.lock", "run.py"]
            artifacts = {
                "turso": {
                    "unstripped": root / "turso",
                    "stripped": root / "turso.stripped",
                }
            }
            invocation = ["artifact", "--scenario", "warm_open"]
            provenance = RUN.collect_provenance(root, inputs, artifacts, [invocation])
            RUN.validate_provenance(provenance, root, inputs, artifacts, [invocation])
            (root / "turso").write_text("mutated")
            with self.assertRaisesRegex(ValueError, "binary provenance"):
                RUN.validate_provenance(provenance, root, inputs, artifacts, [invocation])
            (root / "turso").write_text("binary-a")
            with self.assertRaisesRegex(ValueError, "invocation provenance"):
                RUN.validate_provenance(
                    provenance, root, inputs, artifacts, [["different"]]
                )

    def test_checksum_validation_requires_equal_observable_results(self):
        base = {
            "scenario": "ordered_scan",
            "writers": 1,
            "count": 10,
            "successes": 10,
            "errors": 0,
            "busy": 0,
            "row_count": 10,
            "independent_row_count": 10,
        }
        good = [
            {
                **base,
                "backend": "turso",
                "checksum": 7,
                "independent_checksum": 7,
            },
            {
                **base,
                "backend": "rusqlite",
                "checksum": 7,
                "independent_checksum": 7,
            },
        ]
        RUN.validate_checksums(good)
        good[1]["checksum"] = 8
        with self.assertRaisesRegex(ValueError, "independent checksum mismatch"):
            RUN.validate_checksums(good)

    def test_multiwriter_requires_independent_persisted_row_count_and_checksum(self):
        record = {
            "scenario": "multi_writer",
            "writers": 2,
            "count": 4,
            "backend": "turso",
            "successes": 5,
            "errors": 3,
            "busy": 3,
            "row_count": 5,
            "checksum": 11,
            "independent_row_count": 5,
            "independent_checksum": 11,
        }
        RUN.validate_checksums([record])
        for field in ("row_count", "independent_row_count", "independent_checksum"):
            broken = {**record, field: 999}
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    RUN.validate_checksums([broken])


if __name__ == "__main__":
    unittest.main()
