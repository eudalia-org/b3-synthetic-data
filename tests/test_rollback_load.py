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
            "CHILD", "MID", "PARENT"]

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
