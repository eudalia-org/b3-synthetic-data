import json
import sys

import pytest

import engorda_tables


def test_module_imports():
    assert engorda_tables.REQUIRED_ENV_VARS == (
        "DATAGEN_RAW_BASE_URI",
        "DATAGEN_SYNTHETIC_BASE_URI",
        "DATAGEN_SPECS_URI",
    )


class TestPaths:
    CONFIG = {
        "DATAGEN_RAW_BASE_URI": "oci://raw@ns",
        "DATAGEN_RAW_PREFIX": "datagen/raw",
        "DATAGEN_SYNTHETIC_BASE_URI": "oci://syn@ns",
        "DATAGEN_SYNTHETIC_PREFIX": "",
    }

    def test_table_path_name_strips_schema(self):
        assert engorda_tables.table_path_name("ADMIN.ORDERS") == "ORDERS"
        assert engorda_tables.table_path_name("ORDERS") == "ORDERS"

    def test_raw_path_with_prefix(self):
        assert (
            engorda_tables.raw_path(self.CONFIG, "ORDERS")
            == "oci://raw@ns/datagen/raw/ORDERS"
        )

    def test_raw_path_reduces_dotted_name(self):
        assert (
            engorda_tables.raw_path(self.CONFIG, "ADMIN.ORDERS")
            == "oci://raw@ns/datagen/raw/ORDERS"
        )

    def test_synthetic_base_without_prefix(self):
        assert engorda_tables.synthetic_base_path(self.CONFIG) == "oci://syn@ns"

    def test_synthetic_base_with_prefix(self):
        cfg = dict(self.CONFIG, DATAGEN_SYNTHETIC_PREFIX="datagen/synthetic")
        assert (
            engorda_tables.synthetic_base_path(cfg) == "oci://syn@ns/datagen/synthetic"
        )


class TestGetEngordaEnv:
    def test_reads_required_and_normalizes(self, monkeypatch):
        monkeypatch.setenv("DATAGEN_RAW_BASE_URI", "oci://raw@ns/")
        monkeypatch.setenv("DATAGEN_SYNTHETIC_BASE_URI", "oci://syn@ns/")
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://cfg@ns/specs.json")
        monkeypatch.setenv("DATAGEN_RAW_PREFIX", "/datagen/raw/")
        monkeypatch.delenv("DATAGEN_SYNTHETIC_PREFIX", raising=False)
        config = engorda_tables.get_engorda_env()
        assert config["DATAGEN_RAW_BASE_URI"] == "oci://raw@ns"
        assert config["DATAGEN_RAW_PREFIX"] == "datagen/raw"
        assert config["DATAGEN_SYNTHETIC_PREFIX"] == ""
        assert config["DATAGEN_SPECS_URI"] == "oci://cfg@ns/specs.json"

    def test_exits_when_required_missing(self, monkeypatch):
        for name in engorda_tables.REQUIRED_ENV_VARS:
            monkeypatch.delenv(name, raising=False)
        with pytest.raises(SystemExit):
            engorda_tables.get_engorda_env()


class TestNormalizeSpecs:
    def test_reduces_keys_and_parent_table(self):
        raw = {
            "ADMIN.ORDERS": {
                "pk_cols": ["ORDER_ID"],
                "foreign_keys": [
                    {"columns": ["CUSTOMER_ID"], "parent_table": "ADMIN.CUSTOMERS"}
                ],
            },
            "ADMIN.CUSTOMERS": {"pk_cols": ["CUSTOMER_ID"], "static": True},
        }
        out = engorda_tables.normalize_specs(raw)
        assert set(out) == {"ORDERS", "CUSTOMERS"}
        assert out["ORDERS"]["foreign_keys"][0]["parent_table"] == "CUSTOMERS"

    def test_handles_fks_alias_key(self):
        raw = {
            "ORDERS": {
                "pk_cols": ["ORDER_ID"],
                "fks": [{"columns": ["C_ID"], "parent_table": "X.CUSTOMERS"}],
            }
        }
        out = engorda_tables.normalize_specs(raw)
        assert out["ORDERS"]["fks"][0]["parent_table"] == "CUSTOMERS"

    def test_rejects_collision(self):
        raw = {
            "A.ORDERS": {"pk_cols": ["ID"]},
            "B.ORDERS": {"pk_cols": ["ID"]},
        }
        with pytest.raises(ValueError):
            engorda_tables.normalize_specs(raw)

    def test_passes_through_when_no_schema(self):
        raw = {"ORDERS": {"pk_cols": ["ID"], "n_rows": 10}}
        assert engorda_tables.normalize_specs(raw) == raw


class TestConnectedComponents:
    def _comps(self, specs):
        return sorted(sorted(c) for c in engorda_tables.connected_components(specs))

    def test_chain_is_one_component(self):
        specs = {
            "CUSTOMERS": {"pk_cols": ["CID"]},
            "ORDERS": {"pk_cols": ["OID"],
                       "foreign_keys": [{"columns": ["CID"], "parent_table": "CUSTOMERS"}]},
            "ITEMS": {"pk_cols": ["IID"],
                      "foreign_keys": [{"columns": ["OID"], "parent_table": "ORDERS"}]},
        }
        assert self._comps(specs) == [["CUSTOMERS", "ITEMS", "ORDERS"]]

    def test_disjoint_components(self):
        specs = {
            "A": {"pk_cols": ["ID"]},
            "B": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["AID"], "parent_table": "A"}]},
            "C": {"pk_cols": ["ID"]},
        }
        assert self._comps(specs) == [["A", "B"], ["C"]]

    def test_isolated_node(self):
        specs = {"LOG": {"pk_cols": ["ID"]}}
        assert self._comps(specs) == [["LOG"]]

    def test_fk_to_absent_parent_is_no_edge(self):
        specs = {
            "ORDERS": {"pk_cols": ["OID"],
                       "foreign_keys": [{"columns": ["CID"], "parent_table": "MISSING"}]},
            "OTHER": {"pk_cols": ["ID"]},
        }
        # MISSING is not a node, so ORDERS stays isolated from OTHER.
        assert self._comps(specs) == [["ORDERS"], ["OTHER"]]


class TestEffectiveNRows:
    SPECS = {
        "CUSTOMERS": {"pk_cols": ["CID"]},  # parent (referenced by ORDERS)
        "ORDERS": {"pk_cols": ["OID"],
                   "foreign_keys": [{"columns": ["CID"], "parent_table": "CUSTOMERS"}]},
    }

    def test_scales_non_static(self):
        counts = {"CUSTOMERS": 100, "ORDERS": 1000}
        out = engorda_tables.effective_n_rows(self.SPECS, counts, scale_factor=3.0)
        assert out["ORDERS"] == 3000

    def test_parent_floor_blocks_shrink(self):
        counts = {"CUSTOMERS": 100, "ORDERS": 1000}
        out = engorda_tables.effective_n_rows(self.SPECS, counts, scale_factor=0.5)
        # CUSTOMERS is an FK parent: cannot go below its source count.
        assert out["CUSTOMERS"] == 100
        # ORDERS is a leaf: free to scale down.
        assert out["ORDERS"] == 500

    def test_override_wins_for_non_static(self):
        specs = {"BIG": {"pk_cols": ["ID"], "n_rows": 50}}
        out = engorda_tables.effective_n_rows(specs, {"BIG": 10}, scale_factor=3.0)
        assert out["BIG"] == 50

    def test_static_is_one_to_one_override_ignored(self):
        specs = {"REF": {"pk_cols": ["ID"], "static": True, "n_rows": 999}}
        out = engorda_tables.effective_n_rows(specs, {"REF": 7}, scale_factor=3.0)
        assert out["REF"] == 7

    def test_empty_source_is_zero(self):
        specs = {"EMPTY": {"pk_cols": ["ID"], "n_rows": 100}}
        out = engorda_tables.effective_n_rows(specs, {"EMPTY": 0}, scale_factor=3.0)
        assert out["EMPTY"] == 0


class TestParseArguments:
    def test_defaults(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["engorda_tables.py"])
        args = engorda_tables.parse_arguments()
        assert args.scale_factor == 1.0
        assert args.seed == 42
        assert args.continue_on_error is False
        assert args.specs is None

    def test_overrides(self, monkeypatch):
        monkeypatch.setattr(
            sys, "argv",
            ["engorda_tables.py", "--scale-factor", "3", "--seed", "7",
             "--continue-on-error", "--specs", "oci://cfg@ns/s.json"],
        )
        args = engorda_tables.parse_arguments()
        assert args.scale_factor == 3.0
        assert args.seed == 7
        assert args.continue_on_error is True
        assert args.specs == "oci://cfg@ns/s.json"


class TestLoadSpecs:
    def _fake_spark(self, records):
        class _RDD:
            def collect(self_inner):
                return records
        class _SC:
            def wholeTextFiles(self_inner, uri):
                return _RDD()
        class _Spark:
            sparkContext = _SC()
        return _Spark()

    def test_loads_and_normalizes(self):
        content = json.dumps({"ADMIN.ORDERS": {"pk_cols": ["OID"]}})
        spark = self._fake_spark([("oci://cfg/specs.json", content)])
        specs = engorda_tables.load_specs(spark, "oci://cfg/specs.json")
        assert set(specs) == {"ORDERS"}

    def test_rejects_zero_records(self):
        spark = self._fake_spark([])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/specs.json")

    def test_rejects_multiple_records(self):
        spark = self._fake_spark([("a", "{}"), ("b", "{}")])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/")

    def test_rejects_empty_dict(self):
        spark = self._fake_spark([("a", "{}")])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/specs.json")

    def test_rejects_malformed_json(self):
        spark = self._fake_spark([("a", "{not json")])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/specs.json")


class TestEngordaLoop:
    def _config(self):
        return {
            "DATAGEN_RAW_BASE_URI": "oci://raw@ns", "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": "oci://syn@ns", "DATAGEN_SYNTHETIC_PREFIX": "",
        }

    def test_processes_each_component_and_releases(self, monkeypatch):
        specs = {
            "A": {"pk_cols": ["ID"]},
            "B": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["AID"], "parent_table": "A"}]},
            "C": {"pk_cols": ["ID"]},
        }
        synth_calls = []
        released = []

        class FakeDF:
            def __init__(self, name): self.name = name
            def count(self): return 10

        monkeypatch.setattr(engorda_tables, "read_parquet",
                            lambda spark, path: FakeDF(path))
        monkeypatch.setattr(engorda_tables, "release",
                            lambda *dfs: released.extend(dfs))

        def fake_run(tables, comp_specs, **kwargs):
            synth_calls.append((set(comp_specs), kwargs["n_rows_by_table"]))
            return {t: FakeDF(t) for t in comp_specs}

        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)

        engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                               scale_factor=2.0, seed=42, continue_on_error=False)

        processed = sorted(sorted(s) for s, _ in synth_calls)
        assert processed == [["A", "B"], ["C"]]
        assert released  # something was released between/after components

    def test_continue_on_error_collects_and_exits(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}, "C": {"pk_cols": ["ID"]}}

        class FakeDF:
            def count(self): return 5
        monkeypatch.setattr(engorda_tables, "read_parquet", lambda s, p: FakeDF())
        monkeypatch.setattr(engorda_tables, "release", lambda *dfs: None)

        def fake_run(tables, comp_specs, **kwargs):
            raise RuntimeError("boom")
        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)

        with pytest.raises(SystemExit):
            engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                                   scale_factor=1.0, seed=42, continue_on_error=True)
