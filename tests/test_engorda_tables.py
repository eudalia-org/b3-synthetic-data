import json
import sys

import pytest

from datagen import engorda_tables


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


class TestTopoOrderTables:
    def _pos(self, order):
        return {t: i for i, t in enumerate(order)}

    def test_parents_before_children(self):
        specs = {
            "ITEMS": {"pk_cols": ["IID"],
                      "foreign_keys": [{"columns": ["OID"], "parent_table": "ORDERS"}]},
            "ORDERS": {"pk_cols": ["OID"],
                       "foreign_keys": [{"columns": ["CID"], "parent_table": "CUSTOMERS"}]},
            "CUSTOMERS": {"pk_cols": ["CID"]},
        }
        pos = self._pos(engorda_tables.topo_order_tables(specs))
        assert pos["CUSTOMERS"] < pos["ORDERS"] < pos["ITEMS"]

    def test_self_reference_ignored(self):
        specs = {"USUARIO": {"pk_cols": ["ID"],
                             "foreign_keys": [{"columns": ["MGR"], "parent_table": "USUARIO"}]}}
        assert engorda_tables.topo_order_tables(specs) == ["USUARIO"]

    def test_cycle_is_broken_and_covers_all(self):
        specs = {
            "A": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["B"], "parent_table": "B"}]},
            "B": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["A"], "parent_table": "A"}]},
        }
        assert sorted(engorda_tables.topo_order_tables(specs)) == ["A", "B"]


class TestTopologicalOrder:
    """_topological_order shares topo_order_tables' cycle policy: it breaks
    cycles instead of raising (cycles are sanitized/expected, not fatal)."""

    def _spec(self, name, parents=()):
        fks = tuple(
            engorda_tables.ForeignKeySpec(columns=(f"FK_{p}",), parent_table=p,
                                          parent_columns=("ID",))
            for p in parents
        )
        return engorda_tables.TableSpec(name=name, pk_cols=("ID",), foreign_keys=fks)

    def test_parents_before_children(self):
        specs = {
            "ITEMS": self._spec("ITEMS", ["ORDERS"]),
            "ORDERS": self._spec("ORDERS", ["CUSTOMERS"]),
            "CUSTOMERS": self._spec("CUSTOMERS"),
        }
        order = engorda_tables._topological_order(specs)
        pos = {t: i for i, t in enumerate(order)}
        assert pos["CUSTOMERS"] < pos["ORDERS"] < pos["ITEMS"]

    def test_cycle_is_broken_and_warns(self):
        specs = {
            "A": self._spec("A", ["B"]),
            "B": self._spec("B", ["A"]),
        }
        with pytest.warns(UserWarning, match="[Cc]iclo"):
            order = engorda_tables._topological_order(specs)
        assert sorted(order) == ["A", "B"]

    def test_acyclic_does_not_warn(self):
        import warnings as _w

        specs = {"P": self._spec("P"), "C": self._spec("C", ["P"])}
        with _w.catch_warnings():
            _w.simplefilter("error")
            assert engorda_tables._topological_order(specs) == ["P", "C"]


class TestFkIsWholePk:
    def test_pk_equals_fk(self):
        fk = {"columns": ["NUM_CONDICAO_IF"], "parent_table": "CONDICAO_IF"}
        assert engorda_tables._fk_is_whole_pk(["NUM_CONDICAO_IF"], fk) is True

    def test_composite_pk_equals_fk_any_order(self):
        fk = {"columns": ["B", "A"]}
        assert engorda_tables._fk_is_whole_pk(["A", "B"], fk) is True

    def test_fk_is_subset_of_pk_is_false(self):
        fk = {"columns": ["A"]}
        assert engorda_tables._fk_is_whole_pk(["A", "B"], fk) is False

    def test_ordinary_fk_is_false(self):
        fk = {"columns": ["CUSTOMER_ID"]}
        assert engorda_tables._fk_is_whole_pk(["ORDER_ID"], fk) is False


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
        assert args.limit is None
        assert args.pk_offset is None
        assert args.pk_safety_band is None

    def test_overrides(self, monkeypatch):
        monkeypatch.setattr(
            sys, "argv",
            ["engorda_tables.py", "--scale-factor", "3", "--seed", "7",
             "--continue-on-error", "--limit", "1000", "--pk-offset", "10000000000000",
             "--pk-safety-band", "1000000", "--specs", "oci://cfg@ns/s.json"],
        )
        args = engorda_tables.parse_arguments()
        assert args.scale_factor == 3.0
        assert args.seed == 7
        assert args.continue_on_error is True
        assert args.limit == 1000
        assert args.pk_offset == 10_000_000_000_000
        assert args.pk_safety_band == 1_000_000
        assert args.specs == "oci://cfg@ns/s.json"

    def test_rejects_non_positive_limit(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["engorda_tables.py", "--limit", "0"])
        with pytest.raises(SystemExit):
            engorda_tables.parse_arguments()


class TestReadParquet:
    class _DF:
        def __init__(self): self.limit_arg = None
        def limit(self, n):
            self.limit_arg = n
            return self

    class _Spark:
        def __init__(self, df): self._df = df
        @property
        def read(self):
            outer = self
            class _Reader:
                def parquet(self_inner, path): return outer._df
            return _Reader()

    def test_no_limit_returns_full_df(self):
        df = self._DF()
        out = engorda_tables.read_parquet(self._Spark(df), "p")
        assert out is df and df.limit_arg is None

    def test_applies_limit(self):
        df = self._DF()
        out = engorda_tables.read_parquet(self._Spark(df), "p", limit=250)
        assert out is df and df.limit_arg == 250


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

        writes = []
        monkeypatch.setattr(engorda_tables, "read_parquet",
                            lambda spark, path, limit=None: FakeDF(path))
        monkeypatch.setattr(engorda_tables, "release",
                            lambda *dfs: released.extend(dfs))
        monkeypatch.setattr(engorda_tables, "write_synthetic_table",
                            lambda spark, df, out_path: writes.append(out_path))
        monkeypatch.setattr(engorda_tables, "compute_pk_maxes", lambda *a, **k: {})
        monkeypatch.setattr(engorda_tables, "bind_shared_key_children",
                            lambda synthetic, comp_specs: synthetic)
        monkeypatch.setattr(engorda_tables, "null_orphan_fks",
                            lambda synthetic, comp_specs: synthetic)

        def fake_run(tables, comp_specs, **kwargs):
            synth_calls.append((set(comp_specs), kwargs["n_rows_by_table"]))
            return {t: FakeDF(t) for t in comp_specs}

        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)

        engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                               scale_factor=2.0, seed=42, continue_on_error=False)

        processed = sorted(sorted(s) for s, _ in synth_calls)
        assert processed == [["A", "B"], ["C"]]
        assert released  # something was released between/after components
        # every table written to its own distinct prefix
        assert sorted(writes) == [
            "oci://syn@ns/A",
            "oci://syn@ns/B",
            "oci://syn@ns/C",
        ]
        assert len(writes) == len(set(writes))

    def test_continue_on_error_collects_and_exits(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}, "C": {"pk_cols": ["ID"]}}

        class FakeDF:
            def count(self): return 5
        monkeypatch.setattr(engorda_tables, "read_parquet", lambda s, p, limit=None: FakeDF())
        monkeypatch.setattr(engorda_tables, "release", lambda *dfs: None)
        monkeypatch.setattr(engorda_tables, "compute_pk_maxes", lambda *a, **k: {})

        def fake_run(tables, comp_specs, **kwargs):
            raise RuntimeError("boom")
        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)

        with pytest.raises(SystemExit):
            engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                                   scale_factor=1.0, seed=42, continue_on_error=True)

    def test_limit_uses_referential_sample(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        seen = {}

        class FakeDF:
            def count(self): return 3

        def _no_read(*a, **k):
            raise AssertionError("read_parquet used despite --limit")

        # plain read_parquet must NOT be used when --limit is set
        monkeypatch.setattr(engorda_tables, "read_parquet", _no_read)
        monkeypatch.setattr(engorda_tables, "referential_sample",
                            lambda spark, config, comp_specs, limit:
                                seen.update(limit=limit) or {t: FakeDF() for t in comp_specs})
        monkeypatch.setattr(engorda_tables, "release", lambda *dfs: None)
        monkeypatch.setattr(engorda_tables, "write_synthetic_table", lambda s, d, p: None)
        monkeypatch.setattr(engorda_tables, "compute_pk_maxes", lambda *a, **k: {})
        monkeypatch.setattr(engorda_tables, "bind_shared_key_children",
                            lambda synthetic, cs: synthetic)
        monkeypatch.setattr(engorda_tables, "null_orphan_fks", lambda synthetic, cs: synthetic)
        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables",
                            lambda tables, comp_specs, **kwargs: {t: FakeDF() for t in comp_specs})

        engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                               scale_factor=1.0, seed=42, continue_on_error=False, limit=500)
        assert seen == {"limit": 500}

    def _run_capturing(self, monkeypatch, pk_offset, pk_maxes):
        seen = {}
        floors = []

        class FakeDF:
            def count(self): return 3

        monkeypatch.setattr(engorda_tables, "read_parquet", lambda s, p, limit=None: FakeDF())
        monkeypatch.setattr(engorda_tables, "release", lambda *dfs: None)
        monkeypatch.setattr(engorda_tables, "write_synthetic_table",
                            lambda spark, df, out_path: None)
        monkeypatch.setattr(engorda_tables, "bind_shared_key_children",
                            lambda synthetic, cs: synthetic)
        monkeypatch.setattr(engorda_tables, "null_orphan_fks", lambda synthetic, cs: synthetic)
        monkeypatch.setattr(engorda_tables, "compute_pk_maxes",
                            lambda spark, config, comp_specs, floor=0, band=0, n_rows=None:
                                floors.append(floor) or pk_maxes)

        def fake_run(tables, comp_specs, **kwargs):
            seen.update(kwargs)
            return {t: FakeDF() for t in comp_specs}

        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)
        engorda_tables.engorda(spark=object(), config=self._config(),
                               specs={"A": {"pk_cols": ["ID"]}}, scale_factor=1.0,
                               seed=42, continue_on_error=False, pk_offset=pk_offset)
        return seen, floors

    def test_forwards_true_pk_maxes_to_synthesis(self, monkeypatch):
        seen, _ = self._run_capturing(monkeypatch, pk_offset=None, pk_maxes={"A": 999})
        assert seen["pk_max_by_table"] == {"A": 999}

    def test_pk_offset_passed_as_floor(self, monkeypatch):
        _, floors = self._run_capturing(monkeypatch, pk_offset=10**13, pk_maxes={"A": 10**13})
        assert floors == [10**13]


class TestComputePkMaxes:
    CONFIG = {"DATAGEN_RAW_BASE_URI": "oci://raw@ns", "DATAGEN_RAW_PREFIX": ""}

    @pytest.fixture(autouse=True)
    def _no_clamp(self, monkeypatch):
        # default: unlimited PK domain (no clamp). Clamp test overrides this.
        monkeypatch.setattr(engorda_tables, "_pk_capacity", lambda s, p, c: None)

    def test_clamps_band_to_pk_domain(self, monkeypatch):
        specs = {"M": {"pk_cols": ["NUM_ID_MODALIDADE_LIQUIDACAO"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: 26)
        monkeypatch.setattr(engorda_tables, "_pk_capacity", lambda s, p, c: 999)  # Decimal(3,0)
        # band would push start to 1_000_026; clamp to capacity - n_rows
        out = engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs,
                                              band=1_000_000, n_rows={"M": 10})
        assert out == {"M": 989}            # 999 - 10, so 989 + 10 <= 999
        assert out["M"] >= 26               # never below true_max

    def test_skips_static_floors_and_uses_last_pk(self, monkeypatch):
        specs = {
            "A": {"pk_cols": ["ID"]},                    # true max 100
            "REF": {"pk_cols": ["C"], "static": True},   # skipped (static)
            "B": {"pk_cols": ["X", "ID"]},               # composite -> last col; max 5 -> floor
        }
        seen_cols = {}
        maxes = {"oci://raw@ns/A": 100, "oci://raw@ns/B": 5}

        def fake_max(spark, path, pk_col):
            seen_cols[path] = pk_col
            return maxes[path]

        monkeypatch.setattr(engorda_tables, "_read_pk_max", fake_max)
        out = engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs, floor=1000)
        assert out == {"A": 1000, "B": 1000}            # max(true_max, floor); REF omitted
        assert seen_cols["oci://raw@ns/B"] == "ID"      # last PK column

    def test_no_floor_uses_true_max(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: 8_000_000_000)
        assert engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs) == {"A": 8_000_000_000}

    def test_safety_band_added_above_true_max(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: 8_000_000_000)
        out = engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs, band=1_000_000)
        assert out == {"A": 8_001_000_000}  # true_max + band

    def test_floor_wins_over_band_when_higher(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: 100)
        out = engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs,
                                              floor=10**13, band=1_000_000)
        assert out == {"A": 10**13}  # max(true_max + band, floor)

    def test_omits_unreadable_max(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: None)
        assert engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs) == {}


class TestWriteSyntheticTable:
    def test_deletes_only_table_prefix_then_appends(self, monkeypatch):
        deleted = []
        appended = {}

        class FakeWriter:
            def __init__(self, df): self.df = df
            def mode(self, m):
                self.df.mode_arg = m
                return self
            def parquet(self, path): appended[path] = self.df.mode_arg

        class FakeDF:
            @property
            def write(self): return FakeWriter(self)

        df = FakeDF()
        # bypass column sanitization and the Hadoop FS plumbing
        monkeypatch.setattr(engorda_tables, "_sanitize_columns_for_save",
                            lambda d, name: d)
        monkeypatch.setattr(engorda_tables, "_delete_path",
                            lambda spark, path: deleted.append(path))

        engorda_tables.write_synthetic_table(object(), df, "oci://syn@ns/synthetic/CONDICAO_IF")

        # delete is scoped to exactly this table's prefix, never the parent
        assert deleted == ["oci://syn@ns/synthetic/CONDICAO_IF"]
        assert appended == {"oci://syn@ns/synthetic/CONDICAO_IF": "append"}


pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder.appName("engorda-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestContiguousRowId:
    def test_ids_are_contiguous_and_unique_across_partitions(self, spark):
        df = spark.range(0, 1000).repartition(7).withColumnRenamed("id", "val")
        out = engorda_tables._with_contiguous_row_id(df, "rid")
        rids = sorted(r["rid"] for r in out.select("rid").collect())
        assert rids == list(range(1000))  # 0..N-1, no gaps, no duplicates

    def test_id_matches_within_partition_order(self, spark):
        # Within each source partition, rid order must follow row order; offsets
        # must make the global set contiguous regardless of partition sizes.
        df = spark.range(0, 50).repartition(4).withColumnRenamed("id", "val")
        rows = engorda_tables._with_contiguous_row_id(df, "rid").select("val", "rid").collect()
        rid_by_val = {r["val"]: r["rid"] for r in rows}
        assert len(rid_by_val) == 50
        assert sorted(rid_by_val.values()) == list(range(50))

    def test_no_single_partition_window_in_plan(self, spark):
        # Guards the fix: the offset prefix-sum must not use a no-partitionBy
        # Window, which Spark executes as SinglePartition (serial, stalls at scale).
        df = spark.range(0, 100).repartition(5).withColumnRenamed("id", "val")
        out = engorda_tables._with_contiguous_row_id(df, "rid")
        plan = out._jdf.queryExecution().executedPlan().toString()
        assert "SinglePartition" not in plan

    def test_empty_input(self, spark):
        df = spark.range(0, 0).withColumnRenamed("id", "val")
        out = engorda_tables._with_contiguous_row_id(df, "rid")
        assert out.select("rid").collect() == []


class TestEngordaIntegration:
    def test_round_trip_preserves_keys_and_scales(self, spark, tmp_path):
        raw = tmp_path / "raw"
        syn = tmp_path / "syn"

        customers = spark.createDataFrame(
            [(i, f"name{i}") for i in range(1, 11)], ["CUSTOMER_ID", "NAME"]
        )
        orders = spark.createDataFrame(
            [(i, (i % 10) + 1, i * 1.5) for i in range(1, 101)],
            ["ORDER_ID", "CUSTOMER_ID", "AMOUNT"],
        )
        customers.write.parquet(str(raw / "CUSTOMERS"))
        orders.write.parquet(str(raw / "ORDERS"))

        config = {
            "DATAGEN_RAW_BASE_URI": str(raw), "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(syn), "DATAGEN_SYNTHETIC_PREFIX": "",
        }
        specs = {
            "CUSTOMERS": {"pk_cols": ["CUSTOMER_ID"]},
            "ORDERS": {"pk_cols": ["ORDER_ID"],
                       "foreign_keys": [{"columns": ["CUSTOMER_ID"],
                                          "parent_table": "CUSTOMERS"}]},
        }

        engorda_tables.engorda(spark, config, specs, scale_factor=3.0, seed=1,
                               continue_on_error=False)

        out_customers = spark.read.parquet(str(syn / "CUSTOMERS"))
        out_orders = spark.read.parquet(str(syn / "ORDERS"))

        # CUSTOMERS is an FK parent: floored at source count (10), scaled up by 3 -> 30.
        assert out_customers.count() == 30
        # ORDERS scaled 100 -> 300.
        assert out_orders.count() == 300
        # PK uniqueness.
        assert out_orders.select("ORDER_ID").distinct().count() == 300
        assert out_customers.select("CUSTOMER_ID").distinct().count() == 30
        # FK integrity: every synthetic ORDERS.CUSTOMER_ID exists in synthetic CUSTOMERS.
        orphans = out_orders.join(out_customers, "CUSTOMER_ID", "left_anti").count()
        assert orphans == 0
