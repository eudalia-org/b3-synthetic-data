import json
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


class TestParseArgs:
    def test_dry_run_and_defaults(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", [
            "parallel_extract", "--application-id", "app", "--compartment-id", "cmp",
            "--tables", "A,B", "--dry-run"])
        a = P.parse_arguments()
        assert a.dry_run is True and a.max_concurrent_runs == 4 and a.tables == "A,B"

    def test_env_satisfies_required(self, monkeypatch):
        monkeypatch.setenv("DATAGEN_DATAFLOW_APP_ID", "envapp")
        monkeypatch.setenv("DATAGEN_OCI_COMPARTMENT_ID", "envcmp")
        monkeypatch.setattr(sys, "argv", ["parallel_extract", "--tables", "A"])
        a = P.parse_arguments()
        assert a.application_id == "envapp" and a.compartment_id == "envcmp"

    def test_missing_application_and_compartment_errors(self, monkeypatch):
        monkeypatch.delenv("DATAGEN_DATAFLOW_APP_ID", raising=False)
        monkeypatch.delenv("DATAGEN_OCI_COMPARTMENT_ID", raising=False)
        monkeypatch.setattr(sys, "argv", ["parallel_extract", "--tables", "A"])
        with pytest.raises(SystemExit):
            P.parse_arguments()


class TestPlanReport:
    def _opts(self):
        return dict(application_id="app", compartment_id="cmp", num_executors=2,
                    driver_shape="d", executor_shape="e", driver_shape_config=None,
                    executor_shape_config=None, passthrough=[])

    def test_plan_lists_buckets_commands_and_skew(self):
        weights = {("S", "A"): 9.0, ("S", "B"): 1.0}
        prov = {("S", "A"): "all_tables", ("S", "B"): "median"}
        plan = P.build_plan(weights, num_buckets=2, opts=self._opts(), provenance=prov)
        assert len(plan["buckets"]) == 2
        assert all({"command", "tables", "weight"} <= set(b) for b in plan["buckets"])
        assert any(b["tables"] == ["S.A"] for b in plan["buckets"])    # heaviest isolated
        assert plan["balance_skew"] == 9.0                             # max/min bucket weight
        assert plan["sizes_report"][("S", "A")] == {"weight": 9.0, "tier": "all_tables"}


class TestLifecycleClassify:
    @pytest.mark.parametrize("state,kind", [
        ("SUCCEEDED", "success"), ("FAILED", "failure"), ("CANCELED", "failure"),
        ("STOPPED", "failure"), ("ACCEPTED", "pending"), ("IN_PROGRESS", "pending"),
        ("CANCELING", "pending"), ("STOPPING", "pending"), ("WAT", "pending")])
    def test_classify(self, state, kind):
        assert P.classify_state(state) == kind


class TestRunBuckets:
    def test_retries_failed_then_succeeds(self):
        # bucket 0 fails once then succeeds; bucket 1 succeeds first try
        calls = {"submit": 0}
        seq = {0: ["FAILED", "SUCCEEDED"], 1: ["SUCCEEDED"]}
        attempt = {0: 0, 1: 0}

        def fake_submit(bucket, index, opts):
            calls["submit"] += 1
            return f"run-{index}-{attempt[index]}"

        def fake_poll(run_id, opts=None):
            index = int(run_id.split("-")[1])
            state = seq[index][attempt[index]]
            return state

        def on_terminal(index):
            attempt[index] += 1

        results = P.run_buckets(
            [[("S", "A")], [("S", "B")]], opts=dict(max_concurrent_runs=2, max_retries=2,
            poll_seconds=0), submit=fake_submit, poll=fake_poll, _after_terminal=on_terminal)
        assert results[0]["state"] == "SUCCEEDED" and results[0]["retries"] == 1
        assert results[1]["state"] == "SUCCEEDED" and results[1]["retries"] == 0
        assert calls["submit"] == 3                          # 2 + 1 retry

    def test_gives_up_after_max_retries(self):
        results = P.run_buckets(
            [[("S", "A")]], opts=dict(max_concurrent_runs=1, max_retries=1, poll_seconds=0),
            submit=lambda b, i, o: "r", poll=lambda r, o=None: "FAILED")
        assert results[0]["state"] == "FAILED" and results[0]["retries"] == 1


class TestTier4Sql:
    def test_interpolates_validated_identifiers(self):
        sql = P.tier4_count_sql("CETIP", "OPERACAO")
        assert sql == "SELECT COUNT(*) FROM CETIP.OPERACAO SAMPLE (0.1)"

    def test_rejects_bad_identifier(self):
        with pytest.raises(ValueError):
            P.tier4_count_sql("CETIP", "OPER; DROP")


class TestBytesToRows:
    def test_divides_by_nominal_row_len(self):
        # NOMINAL_AVG_ROW_LEN bytes/row; only relative ordering matters
        assert P.bytes_to_rows(P.NOMINAL_AVG_ROW_LEN * 5) == 5.0

    def test_zero_bytes(self):
        assert P.bytes_to_rows(0) == 0.0


class TestResolveSizes:
    def test_unreachable_without_flag_exits(self):
        def boom():
            raise RuntimeError("no route to host")
        with pytest.raises(SystemExit):
            P.resolve_sizes([("S", "A")], connect=boom, allow_fallback=False)

    def test_unreachable_with_flag_equal_weight(self):
        def boom():
            raise RuntimeError("no route to host")
        weights, prov = P.resolve_sizes([("S", "A"), ("S", "B")], connect=boom,
                                        allow_fallback=True)
        assert weights == {("S", "A"): 1.0, ("S", "B"): 1.0}
        assert set(prov.values()) == {"equal-weight-fallback"}


class TestBuildRunCreateCommand:
    def _opts(self, **kw):
        base = dict(application_id="ocid1.dataflowapplication.x",
                    compartment_id="ocid1.compartment.y", num_executors=2,
                    driver_shape="VM.Standard.E4.Flex", executor_shape="VM.Standard.E4.Flex",
                    driver_shape_config=None, executor_shape_config=None, passthrough=[])
        base.update(kw)
        return base

    def test_includes_ids_and_display_name(self):
        cmd = P.build_run_create_command([("CETIP", "A"), ("CETIP", "B")], 0, self._opts())
        assert cmd[:3] == ["oci", "data-flow", "run"]
        assert "create" in cmd
        joined = " ".join(cmd)
        assert "ocid1.dataflowapplication.x" in joined
        assert "ocid1.compartment.y" in joined
        assert "extract-bucket-0" in joined

    def test_arguments_is_json_array_of_tables(self):
        cmd = P.build_run_create_command([("CETIP", "A"), ("CETIP", "B")], 1, self._opts())
        idx = cmd.index("--arguments")
        args = json.loads(cmd[idx + 1])
        assert args == ["--tables", "CETIP.A,CETIP.B"]

    def test_passthrough_flags_appended_to_arguments(self):
        cmd = P.build_run_create_command(
            [("CETIP", "A")], 0, self._opts(passthrough=["--continue-on-error"]))
        idx = cmd.index("--arguments")
        assert json.loads(cmd[idx + 1]) == ["--tables", "CETIP.A", "--continue-on-error"]

    def test_shape_config_included_when_present(self):
        cfg = '{"ocpus": 2, "memoryInGBs": 16}'
        cmd = P.build_run_create_command(
            [("CETIP", "A")], 0, self._opts(executor_shape_config=cfg))
        assert cfg in cmd


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


class TestTablesFromSpecs:
    def test_returns_all_keys_order_preserved(self, tmp_path):
        import json as _j
        f = tmp_path / "specs.json"
        f.write_text(_j.dumps({"OPERACAO": {"static": False}, "TIPO_IF": {"static": True}}))
        assert P.tables_from_specs(str(f)) == ["OPERACAO", "TIPO_IF"]

    def test_empty_specs_raises(self, tmp_path):
        f = tmp_path / "e.json"
        f.write_text("{}")
        with pytest.raises(ValueError):
            P.tables_from_specs(str(f))


class TestSpecsSource:
    def test_specs_accepted_as_source(self, monkeypatch):
        monkeypatch.setenv("DATAGEN_DATAFLOW_APP_ID", "a")
        monkeypatch.setenv("DATAGEN_OCI_COMPARTMENT_ID", "c")
        monkeypatch.setattr(sys, "argv", ["parallel_extract", "--specs", "specs.json"])
        a = P.parse_arguments()
        assert a.specs == "specs.json"

    def test_specs_mutually_exclusive_with_tables(self, monkeypatch):
        monkeypatch.setenv("DATAGEN_DATAFLOW_APP_ID", "a")
        monkeypatch.setenv("DATAGEN_OCI_COMPARTMENT_ID", "c")
        monkeypatch.setattr(sys, "argv",
                            ["parallel_extract", "--specs", "s.json", "--tables", "A"])
        with pytest.raises(SystemExit):
            P.parse_arguments()


class TestOciAuthFlags:
    def test_emits_only_set_flags(self):
        opts = dict(profile="DEV", config_file=None, auth="security_token", cert_bundle=None)
        assert P.oci_auth_flags(opts) == ["--profile", "DEV", "--auth", "security_token"]

    def test_empty_when_none_set(self):
        assert P.oci_auth_flags(dict()) == []

    def test_all_four(self):
        opts = dict(profile="P", config_file="/c", auth="api_key", cert_bundle="/b")
        assert P.oci_auth_flags(opts) == [
            "--profile", "P", "--config-file", "/c", "--auth", "api_key", "--cert-bundle", "/b"]


class TestAuthFlagsInCommands:
    def test_run_create_includes_auth_flags(self):
        opts = dict(application_id="app", compartment_id="cmp", num_executors=2,
                    driver_shape="d", executor_shape="e", driver_shape_config=None,
                    executor_shape_config=None, passthrough=[],
                    profile="DEV", config_file=None, auth="security_token", cert_bundle=None)
        cmd = P.build_run_create_command([("S", "A")], 0, opts)
        assert cmd[-4:] == ["--profile", "DEV", "--auth", "security_token"]

    def test_poll_run_includes_auth_flags(self, monkeypatch):
        captured = {}

        def fake_oci_json(cmd):
            captured["cmd"] = cmd
            return {"data": {"lifecycle-state": "SUCCEEDED"}}

        monkeypatch.setattr(P, "_oci_json", fake_oci_json)
        P.poll_run("run-x", dict(profile="DEV"))
        assert captured["cmd"][:5] == ["oci", "data-flow", "run", "get", "--run-id"]
        assert captured["cmd"][-2:] == ["--profile", "DEV"]
