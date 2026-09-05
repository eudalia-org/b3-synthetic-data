import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import rollback_load  # noqa: E402


class TestPkChunkRanges:
    def test_covers_range_no_gaps(self):
        assert rollback_load.pk_chunk_ranges(100, 250, 50) == [
            (101, 150),
            (151, 200),
            (201, 250),
        ]

    def test_single_short_range(self):
        assert rollback_load.pk_chunk_ranges(10, 12, 50) == [(11, 12)]

    def test_empty_when_upper_not_above_lower(self):
        assert rollback_load.pk_chunk_ranges(100, 100, 50) == []
        assert rollback_load.pk_chunk_ranges(100, 80, 50) == []

    def test_exact_multiple(self):
        assert rollback_load.pk_chunk_ranges(0, 100, 50) == [(1, 50), (51, 100)]


class TestDeleteAboveSql:
    def test_builds_delete(self):
        assert rollback_load.delete_above_sql("ADMIN", "LANCAMENTO", "NUM_ID", 11, 50) == (
            "DELETE FROM ADMIN.LANCAMENTO WHERE NUM_ID BETWEEN 11 AND 50"
        )

    def test_rejects_non_integer_bounds(self):
        with pytest.raises(ValueError):
            rollback_load.delete_above_sql("ADMIN", "T", "PK", "11", 50)

    def test_rejects_bad_identifier(self):
        with pytest.raises(ValueError):
            rollback_load.delete_above_sql("ADMIN", "T; DROP", "PK", 1, 2)


class TestRollbackOrder:
    def test_reverses_to_children_first(self):
        # manifest lists tables parent-first (load order); rollback deletes children first
        entries = [{"table": "PARENT"}, {"table": "MID"}, {"table": "CHILD"}]
        assert [e["table"] for e in rollback_load.rollback_order(entries)] == [
            "CHILD",
            "MID",
            "PARENT",
        ]

    def test_empty(self):
        assert rollback_load.rollback_order([]) == []


class TestDryRunArg:
    def test_dry_run_parses(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["rollback_load", "--run-id", "R1", "--dry-run"])
        args = rollback_load.parse_arguments()
        assert args.dry_run is True and args.run_id == "R1"

    def test_dry_run_default_false(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["rollback_load", "--run-id", "R1"])
        args = rollback_load.parse_arguments()
        assert args.dry_run is False

    def test_explicit_manifest_is_mutually_exclusive_with_run_id(self, monkeypatch):
        monkeypatch.setattr(
            sys, "argv", ["rollback_load", "--manifest-uri", "m.json", "--run-id", "R1"]
        )
        with pytest.raises(SystemExit):
            rollback_load.parse_arguments()


class TestExplicitManifest:
    def test_reads_exact_local_object(self, tmp_path):
        path = tmp_path / "manifest.json"
        expected = {"schema_version": 1, "kind": "load-attempt", "tables": []}
        path.write_text(json.dumps(expected))
        assert rollback_load.read_manifest(None, {}, manifest_uri=str(path)) == expected

    def test_explicit_uri_does_not_require_load_base(self, monkeypatch):
        for name in list(os.environ):
            if name.startswith("DATAGEN_"):
                monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("DATAGEN_TARGET_JDBC_URL", "jdbc:oracle:thin:@host")
        monkeypatch.setenv("DATAGEN_TARGET_DB_PASSWORD", "secret")
        config = rollback_load.get_env(require_load_base=False)
        assert "DATAGEN_LOAD_BASE_URI" not in config

    def test_legacy_run_id_still_reads_text_directory(self):
        context = type(
            "Context",
            (),
            {
                "textFile": lambda _self, path: type(
                    "RDD", (), {"collect": lambda _self: ['{"run_id":"R1","tables":[]}']}
                )()
            },
        )()
        spark = type("Spark", (), {"sparkContext": context})()
        manifest = rollback_load.read_manifest(
            spark, {"DATAGEN_LOAD_BASE_URI": "oci://bucket/load"}, run_id="R1"
        )
        assert manifest["run_id"] == "R1"

    def test_legacy_manifest_cannot_execute_unsafe_rollback(self):
        with pytest.raises(ValueError, match="cannot be rolled back safely"):
            rollback_load.require_exact_load_manifest({"run_id": "R1", "tables": []})

    def test_rejects_malformed_new_manifest(self):
        with pytest.raises(ValueError, match="schema or kind"):
            rollback_load.is_exact_load_manifest({"schema_version": 1, "tables": []})


class TestExactRangeRollback:
    ENTRY = {
        "table": "LANCAMENTO",
        "owner": "CETIP",
        "name": "LANCAMENTO",
        "pk_col": "NUM_ID",
        "synthetic_pk_min": 101,
        "synthetic_pk_max": 112,
        "rollbackable": True,
    }

    def test_new_manifest_shape_accepts_exact_ranges(self):
        manifest = {
            "schema_version": 1,
            "kind": "load-attempt",
            "tables": [self.ENTRY],
        }
        assert rollback_load.is_exact_load_manifest(manifest) is True

    def test_deletes_only_recorded_range_without_querying_current_max(self, monkeypatch):
        statements = []
        monkeypatch.setattr(
            rollback_load,
            "read_rows",
            lambda *_args, **_kwargs: pytest.fail("exact rollback queried Oracle MAX/MIN"),
        )
        monkeypatch.setattr(
            rollback_load,
            "execute_statement",
            lambda _spark, _properties, sql: statements.append(sql),
        )
        chunks = rollback_load.rollback_table(
            object(), {}, self.ENTRY, chunk_size=5, index=1, total=1
        )
        assert chunks == 3
        assert statements == [
            "DELETE FROM CETIP.LANCAMENTO WHERE NUM_ID BETWEEN 101 AND 105",
            "DELETE FROM CETIP.LANCAMENTO WHERE NUM_ID BETWEEN 106 AND 110",
            "DELETE FROM CETIP.LANCAMENTO WHERE NUM_ID BETWEEN 111 AND 112",
        ]

    def test_dry_run_executes_no_delete(self, monkeypatch):
        monkeypatch.setattr(
            rollback_load,
            "execute_statement",
            lambda *_args: pytest.fail("dry run executed DELETE"),
        )
        assert rollback_load.rollback_table(object(), {}, self.ENTRY, 5, 1, 1, dry_run=True) == 3
