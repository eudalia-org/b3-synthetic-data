import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import parallel_extract as P  # noqa: E402


class TestJdbcUrlToDsn:
    def test_sid_colon_form(self):
        assert P.jdbc_url_to_dsn("jdbc:oracle:thin:@dbhost:1521:ORCL") == "dbhost:1521/ORCL"

    def test_service_slashes_form(self):
        assert P.jdbc_url_to_dsn(
            "jdbc:oracle:thin:@//dbhost:1521/PROD.cetip") == "dbhost:1521/PROD.cetip"

    def test_default_port_when_absent(self):
        assert P.jdbc_url_to_dsn("jdbc:oracle:thin:@dbhost:ORCL") == "dbhost:1521/ORCL"

    def test_rejects_non_jdbc(self):
        with pytest.raises(ValueError):
            P.jdbc_url_to_dsn("postgres://x")


class TestOwnerSplit:
    def test_qualified(self):
        assert P.split_owner_table("CETIP.OPERACAO", "DEFOWNER") == ("CETIP", "OPERACAO")

    def test_unqualified_uses_default(self):
        assert P.split_owner_table("OPERACAO", "CETIP") == ("CETIP", "OPERACAO")

    def test_uppercases(self):
        assert P.split_owner_table("cetip.operacao", "x") == ("CETIP", "OPERACAO")


class TestValidIdentifier:
    def test_accepts(self):
        assert P.valid_identifier("CETIP") == "CETIP"

    def test_rejects_injection(self):
        with pytest.raises(ValueError):
            P.valid_identifier("OPER; DROP TABLE X")


class TestParseTables:
    def test_comma_split_and_dedup(self):
        assert P.parse_tables("A, B ,A", None) == ["A", "B"]

    def test_file_with_comments_and_blanks(self, tmp_path):
        f = tmp_path / "t.txt"
        f.write_text("A\n# comment\n\nB\n")
        assert P.parse_tables(None, str(f)) == ["A", "B"]


class TestMergeSizeTiers:
    def test_earlier_tier_wins(self):
        keys = [("CETIP", "A"), ("CETIP", "B")]
        tiers = [{("CETIP", "A"): 100.0}, {("CETIP", "A"): 999.0, ("CETIP", "B"): 50.0}]
        assert P.merge_size_tiers(keys, tiers) == {("CETIP", "A"): 100.0, ("CETIP", "B"): 50.0}

    def test_missing_key_gets_median(self):
        keys = [("CETIP", "A"), ("CETIP", "B"), ("CETIP", "C")]
        tiers = [{("CETIP", "A"): 10.0, ("CETIP", "B"): 30.0}]   # C unresolved
        out = P.merge_size_tiers(keys, tiers)
        assert out[("CETIP", "C")] == 20.0          # median(10, 30)

    def test_all_unresolved_default_one(self):
        keys = [("CETIP", "A")]
        assert P.merge_size_tiers(keys, []) == {("CETIP", "A"): 1.0}

    def test_ignores_non_positive(self):
        keys = [("CETIP", "A"), ("CETIP", "B")]
        tiers = [{("CETIP", "A"): 0.0, ("CETIP", "B"): 40.0}]    # 0 -> treat as unresolved
        out = P.merge_size_tiers(keys, tiers)
        assert out[("CETIP", "A")] == 40.0          # median of the single resolved value


class TestBinPack:
    def test_balances_and_covers_all(self):
        # 6,5,4,3 (sum 18) splits evenly: A+D=9 | B+C=9
        weights = {("S", "A"): 6.0, ("S", "B"): 5.0, ("S", "C"): 4.0, ("S", "D"): 3.0}
        buckets = P.bin_pack(weights, 2)
        assert len(buckets) == 2
        flat = sorted(k for b in buckets for k in b)
        assert flat == sorted(weights)                       # disjoint + complete
        totals = sorted(sum(weights[k] for k in b) for b in buckets)
        assert totals == [9.0, 9.0]                          # greedy LPT: A,D | B,C

    def test_deterministic_tie_break_by_name(self):
        weights = {("S", "A"): 5.0, ("S", "B"): 5.0}
        assert P.bin_pack(weights, 2) == P.bin_pack(weights, 2)

    def test_more_buckets_than_tables(self):
        weights = {("S", "A"): 1.0}
        buckets = P.bin_pack(weights, 3)
        assert sum(len(b) for b in buckets) == 1             # no table duplicated
        assert len(buckets) == 3                             # empty buckets preserved

    def test_single_bucket(self):
        weights = {("S", "A"): 1.0, ("S", "B"): 2.0}
        assert sorted(P.bin_pack(weights, 1)[0]) == [("S", "A"), ("S", "B")]


class TestSizeProvenance:
    def test_reports_resolving_tier_index_else_median(self):
        keys = [("S", "A"), ("S", "B"), ("S", "C")]
        tiers = [{("S", "A"): 10.0}, {("S", "B"): 20.0}]        # C unresolved
        prov = P.size_provenance(keys, tiers, tier_labels=["dba_segments", "all_tables"])
        assert prov == {("S", "A"): "dba_segments", ("S", "B"): "all_tables",
                        ("S", "C"): "median"}
