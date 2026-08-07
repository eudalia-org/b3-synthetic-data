import json
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameWriter

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import repair_operacao_ctx_msg_p2 as repair  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("repair-operacao-ctx-msg-p2-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _source(spark, rows=None):
    rows = rows or [
        (1, Decimal("10.000"), "change-a", 1),
        (2, Decimal("10.000"), "change-b", 2),
        (3, None, "already-null", 3),
        (4, Decimal("20.500"), "unmatched", 4),
    ]
    return spark.createDataFrame(
        rows,
        f"{repair.PK_COLUMN} long, {repair.TARGET_COLUMN} decimal(38,9), "
        "PAYLOAD string, OTHER int",
    )


def _faltantes(spark, rows):
    return spark.createDataFrame(rows, "TABELA string, COLUNA string, VALOR string")


def _options(base, faltantes, specs, *, run_id="test-run", dry_run=False):
    return {
        "synthetic_base": str(base),
        "prefix": repair.DEFAULT_PREFIX,
        "faltantes": str(faltantes),
        "specs": str(specs),
        "run_id": run_id,
        "dry_run": dry_run,
    }


def _write_inputs(spark, tmp_path, source, faltantes):
    base = tmp_path / "synthetic"
    final = base / repair.DEFAULT_PREFIX / repair.TABLE
    missing = tmp_path / "faltantes"
    specs = tmp_path / "specs.json"
    source.write.mode("append").parquet(str(final))
    faltantes.write.mode("append").parquet(str(missing))
    specs.write_text(json.dumps(_valid_specs()), encoding="utf-8")
    return base, final, missing, specs


def _valid_specs():
    return repair.normalize_specs(
        {
            "CETIP.OPERACAO": {
                "pk_cols": ["NUM_ID_OPERACAO"],
                "foreign_keys": [
                    {
                        "columns": ["NUM_ID_CTX_MSG_P2"],
                        "parent_table": "CETIP.CONTEXTO_MENSAGEM",
                        "parent_columns": ["NUM_ID_CTX_MSG"],
                    }
                ],
                "not_null_cols": ["NUM_ID_OPERACAO"],
            }
        }
    )


def test_normalization_accepts_schema_qualified_table_and_dedupes_decimals(spark):
    faltantes = _faltantes(
        spark,
        [
            (" operacao ", " num_id_ctx_msg_p2 ", "10.000"),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", "10"),
            ("CETIP.OPERACAO", "NUM_ID_CTX_MSG_P2", "11"),
            ("OPERACAO", "OUTRA", "12"),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", "20.500"),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", " "),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", None),
        ],
    )
    values = {
        row[repair.NORMALIZED_KEY_COLUMN]
        for row in repair.relevant_keys(faltantes).collect()
    }
    assert values == {"10", "11", "20.500"}


def test_selective_repair_preserves_schema_order_pk_and_other_values(spark):
    source = _source(spark)
    faltantes = _faltantes(
        spark,
        [
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", "10"),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", "10.000"),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", "999.0"),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", None),
        ],
    )
    keys = repair.relevant_keys(faltantes)
    result = repair.repair_dataframe(source, keys)
    metrics = repair.validate_integrity(source, result, keys, 4, label="repaired")
    rows = result.orderBy(repair.PK_COLUMN).collect()

    assert result.schema == source.schema
    assert result.columns == source.columns
    assert [row[repair.PK_COLUMN] for row in rows] == [1, 2, 3, 4]
    assert [row[repair.TARGET_COLUMN] for row in rows] == [
        None,
        None,
        None,
        Decimal("20.500000000"),
    ]
    assert [(row.PAYLOAD, row.OTHER) for row in rows] == [
        ("change-a", 1),
        ("change-b", 2),
        ("already-null", 3),
        ("unmatched", 4),
    ]
    assert metrics["matched_rows"] == 2
    assert metrics["unmatched_change_violations"] == 0


def test_64k_keys_never_collect_or_iterate_on_driver(spark, monkeypatch):
    keys_as_faltantes = spark.range(64_000).select(
        F.lit("OPERACAO").alias("TABELA"),
        F.lit("NUM_ID_CTX_MSG_P2").alias("COLUNA"),
        F.col("id").cast("string").alias("VALOR"),
    )
    source = spark.createDataFrame(
        [(1, 0, "first", 1), (2, 63_999, "last", 2), (3, 64_000, "keep", 3)],
        f"{repair.PK_COLUMN} long, {repair.TARGET_COLUMN} long, PAYLOAD string, OTHER int",
    )
    keys = repair.relevant_keys(keys_as_faltantes)

    with monkeypatch.context() as guarded:
        guarded.setattr(
            DataFrame,
            "collect",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("repair keys/rows must stay distributed")
            ),
        )
        guarded.setattr(
            DataFrame,
            "toLocalIterator",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("repair keys/rows must stay distributed")
            ),
        )
        result = repair.repair_dataframe(source, keys)
        metrics = repair.validate_integrity(source, result, keys, 3, label="repaired")
        assert result.count() == 3
        assert keys.count() == 64_000
        assert metrics["matched_rows"] == 2


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ([(1, "10")], "missing required"),
        ([(None, "10", "x")], "null value"),
        ([(1, "10", "x"), (1, "20", "y")], "not unique"),
    ],
)
def test_invalid_source_schema_or_pk_fails(spark, source, message):
    if len(source[0]) == 2:
        frame = spark.createDataFrame(source, f"{repair.PK_COLUMN} long, VALUE string")
    else:
        frame = spark.createDataFrame(
            source,
            f"{repair.PK_COLUMN} long, {repair.TARGET_COLUMN} string, PAYLOAD string",
        )
    with pytest.raises(ValueError, match=message):
        repair.validate_source(frame)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda specs: specs.pop("OPERACAO"), "missing table"),
        (lambda specs: specs["OPERACAO"].update(pk_cols=["WRONG"]), "pk_cols"),
        (lambda specs: specs["OPERACAO"].update(foreign_keys=[]), "child foreign key"),
        (
            lambda specs: specs["OPERACAO"].update(
                not_null_cols=[repair.TARGET_COLUMN]
            ),
            "NOT NULL",
        ),
    ],
)
def test_invalid_specs_fail(mutate, message):
    specs = _valid_specs()
    mutate(specs)
    with pytest.raises(ValueError, match=message):
        repair.validate_specs(specs)


def test_no_relevant_keys_fails(spark, tmp_path):
    source = _source(spark)
    faltantes = _faltantes(spark, [("OPERACAO", "OTHER", "10")])
    base, _, missing, specs = _write_inputs(spark, tmp_path, source, faltantes)
    with pytest.raises(repair.RepairFailure, match="no nonblank keys"):
        repair.run_repair(spark, _options(base, missing, specs, dry_run=True))


def test_run_repair_requires_specs_before_transformation(spark, tmp_path, monkeypatch):
    source = _source(spark)
    faltantes = _faltantes(spark, [("OPERACAO", "NUM_ID_CTX_MSG_P2", "10")])
    base, _, missing, specs = _write_inputs(spark, tmp_path, source, faltantes)
    options = _options(base, missing, specs, dry_run=True)
    options["specs"] = None
    monkeypatch.setattr(
        repair,
        "repair_dataframe",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not transform")),
    )
    with pytest.raises(repair.RepairFailure, match="DATAGEN_SPECS_URI") as failure:
        repair.run_repair(spark, options)
    assert failure.value.report.get("specs_validated") is not True


def test_relevant_keys_without_source_match_are_noop_and_never_write(
    spark, tmp_path, monkeypatch
):
    source = _source(spark)
    faltantes = _faltantes(spark, [("OPERACAO", "NUM_ID_CTX_MSG_P2", "999")])
    base, _, missing, specs = _write_inputs(spark, tmp_path, source, faltantes)
    monkeypatch.setattr(
        repair,
        "delete_existing_staging",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not delete")),
    )
    monkeypatch.setattr(
        repair,
        "publish_staging",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not publish")),
    )
    report = repair.run_repair(spark, _options(base, missing, specs))
    paths = repair.build_paths(str(base), repair.DEFAULT_PREFIX, "test-run")

    assert report["status"] == "no-op"
    assert report["matched_rows"] == 0
    assert not Path(paths["staging"]).exists()


def test_dry_run_performs_all_in_memory_gates_without_filesystem_mutation(
    spark, tmp_path, monkeypatch
):
    source = _source(spark)
    faltantes = _faltantes(spark, [("OPERACAO", "NUM_ID_CTX_MSG_P2", "10")])
    base, _, missing, specs = _write_inputs(spark, tmp_path, source, faltantes)

    with monkeypatch.context() as guarded:
        guarded.setattr(
            DataFrameWriter,
            "parquet",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("dry run must not write")
            ),
        )
        guarded.setattr(
            repair,
            "path_exists",
            lambda *_args: (_ for _ in ()).throw(
                AssertionError("dry run must not mutate/check work paths")
            ),
        )
        report = repair.run_repair(
            spark, _options(base, missing, specs, dry_run=True)
        )

    assert report["status"] == "dry-run"
    assert report["matched_rows"] == 2
    assert report["exact_transform_violations"] == 0
    paths = repair.build_paths(str(base), repair.DEFAULT_PREFIX, "test-run")
    assert report["paths"]["staging"] == paths["staging"]
    assert not Path(paths["staging"]).exists()


def test_local_parquet_publish_integration(spark, tmp_path):
    source = _source(spark)
    faltantes = _faltantes(spark, [("OPERACAO", "NUM_ID_CTX_MSG_P2", "10.000")])
    base, final, missing, specs = _write_inputs(spark, tmp_path, source, faltantes)
    report = repair.run_repair(
        spark,
        _options(base, missing, specs, run_id="local-publish-integration"),
    )
    output = spark.read.parquet(str(final)).orderBy(repair.PK_COLUMN).collect()
    paths = repair.build_paths(
        str(base), repair.DEFAULT_PREFIX, "local-publish-integration"
    )

    assert report["status"] == "published"
    assert report["source_rows"] == report["repaired_rows"] == report["staged_rows"] == 4
    assert report["postpublish_validated"] is True
    assert report["backup_preserved"] is False
    assert [row[repair.TARGET_COLUMN] for row in output] == [
        None,
        None,
        None,
        Decimal("20.500000000"),
    ]
    assert not Path(paths["staging"]).exists()
    assert not Path(paths["backup"]).exists()


def test_postpublish_validation_failure_restores_full_previous_output(
    spark, tmp_path, monkeypatch
):
    source = _source(spark)
    faltantes = _faltantes(spark, [("OPERACAO", "NUM_ID_CTX_MSG_P2", "10")])
    base, final, missing, specs = _write_inputs(spark, tmp_path, source, faltantes)
    run_id = "post-validation-rollback"
    paths = repair.build_paths(str(base), repair.DEFAULT_PREFIX, run_id)
    original_validate = repair.validate_integrity

    def fail_final_validation(source_df, candidate, keys, source_rows, *, label):
        if label == "final":
            assert Path(paths["backup"]).exists()
            raise ValueError("injected final readback failure")
        return original_validate(source_df, candidate, keys, source_rows, label=label)

    monkeypatch.setattr(repair, "validate_integrity", fail_final_validation)
    with pytest.raises(repair.RepairFailure, match="previous destination restored") as failure:
        repair.run_repair(
            spark,
            _options(base, missing, specs, run_id=run_id),
        )

    restored = spark.read.parquet(str(final)).orderBy(repair.PK_COLUMN).collect()
    assert [row[repair.TARGET_COLUMN] for row in restored] == [
        Decimal("10.000000000"),
        Decimal("10.000000000"),
        None,
        Decimal("20.500000000"),
    ]
    assert failure.value.report["rollback_restored"] is True
    assert failure.value.report["postpublish_validated"] is False
    assert not Path(paths["backup"]).exists()


class FakeFileStatus:
    def __init__(self, path, length):
        self.path = path
        self.length = length

    def getPath(self):
        return self.path

    def getLen(self):
        return self.length


class FakeRemoteIterator:
    def __init__(self, statuses):
        self.statuses = iter(statuses)
        self.next_status = None

    def hasNext(self):
        if self.next_status is None:
            self.next_status = next(self.statuses, None)
        return self.next_status is not None

    def next(self):
        status = self.next_status
        self.next_status = None
        return status


class FakePublicationFs:
    def __init__(
        self,
        entries,
        failed_renames=(),
        failed_deletes=(),
        partial_renames=None,
        ambiguous_false_renames=(),
        ambiguous_throw_renames=(),
    ):
        self.entries = {
            path: value if isinstance(value, dict) else {"part.parquet": len(value)}
            for path, value in entries.items()
        }
        self.failed_renames = set(failed_renames)
        self.failed_deletes = set(failed_deletes)
        self.partial_renames = partial_renames or {}
        self.ambiguous_false_renames = set(ambiguous_false_renames)
        self.ambiguous_throw_renames = set(ambiguous_throw_renames)
        self.renames = []
        self.deletes = []

    def makeQualified(self, path):
        return str(path)

    def exists(self, path):
        return str(path) in self.entries

    def listFiles(self, root, recursive):
        assert recursive is True
        root = str(root)
        statuses = [
            FakeFileStatus(f"{root}/{relative}", length)
            for relative, length in self.entries[root].items()
        ]
        return FakeRemoteIterator(statuses)

    def rename(self, source, target):
        pair = (str(source), str(target))
        self.renames.append(pair)
        if pair in self.failed_renames or pair[0] not in self.entries:
            return False
        if pair in self.partial_renames:
            self.entries[pair[1]] = dict(self.partial_renames[pair])
            return True
        self.entries[pair[1]] = self.entries.pop(pair[0])
        if pair in self.ambiguous_throw_renames:
            raise RuntimeError("rename completed but API threw")
        if pair in self.ambiguous_false_renames:
            return False
        return True

    def delete(self, path, recursive):
        path = str(path)
        self.deletes.append((path, recursive))
        if path in self.failed_deletes or path not in self.entries:
            return False
        del self.entries[path]
        return True


def test_fake_promotion_success_retains_backup_until_final_validation():
    fs = FakePublicationFs(
        {"staging": {"new.parquet": 3}, "final": {"old.parquet": 3}}
    )
    backup = "final.__previous_run"
    result = repair.promote_staging_paths(
        fs, "staging", "final", backup
    )
    assert fs.entries == {
        "final": {"new.parquet": 3},
        backup: {"old.parquet": 3},
    }
    assert fs.renames == [
        ("final", backup),
        ("staging", "final"),
    ]
    assert fs.deletes == []
    assert result["backup_preserved"] is True


@pytest.mark.parametrize(
    "ambiguous_option",
    ["ambiguous_false_renames", "ambiguous_throw_renames"],
)
@pytest.mark.parametrize(
    "ambiguous_pair",
    [
        ("final", "final.__previous_run"),
        ("staging", "final"),
    ],
)
def test_full_ambiguous_publication_rename_is_accepted_by_manifest(
    ambiguous_option, ambiguous_pair
):
    backup = "final.__previous_run"
    fs = FakePublicationFs(
        {"staging": {"new.parquet": 7}, "final": {"old.parquet": 5}},
        **{ambiguous_option: {ambiguous_pair}},
    )
    result = repair.promote_staging_paths(fs, "staging", "final", backup)
    assert fs.entries["final"] == {"new.parquet": 7}
    assert fs.entries[backup] == {"old.parquet": 5}
    assert result["backup_preserved"] is True


def test_fake_promotion_failure_restores_verified_previous_final():
    backup = "final.__previous_run"
    fs = FakePublicationFs(
        {"staging": "new", "final": "old"}, failed_renames={("staging", "final")}
    )
    with pytest.raises(ValueError, match="restored and verified"):
        repair.promote_staging_paths(fs, "staging", "final", backup)
    assert fs.entries["final"] == {"part.parquet": 3}
    assert backup not in fs.entries


@pytest.mark.parametrize(
    "ambiguous_option",
    ["ambiguous_false_renames", "ambiguous_throw_renames"],
)
def test_full_ambiguous_restore_is_accepted_by_manifest(ambiguous_option):
    backup = "final.__previous_run"
    restore_pair = (backup, "final")
    fs = FakePublicationFs(
        {"staging": {"new.parquet": 7}, "final": {"old.parquet": 5}},
        failed_renames={("staging", "final")},
        **{ambiguous_option: {restore_pair}},
    )
    with pytest.raises(ValueError, match="restored and verified"):
        repair.promote_staging_paths(fs, "staging", "final", backup)
    assert fs.entries["final"] == {"old.parquet": 5}
    assert backup not in fs.entries


def test_fake_promotion_and_rollback_failure_preserves_critical_backup():
    backup = "final.__previous_manual_recovery"
    fs = FakePublicationFs(
        {"staging": "new", "final": "old"},
        failed_renames={("staging", "final"), (backup, "final")},
    )
    with pytest.raises(RuntimeError, match=rf"CRITICAL.*{backup}"):
        repair.promote_staging_paths(fs, "staging", "final", backup)
    assert fs.entries[backup] == {"part.parquet": 3}
    assert "final" not in fs.entries


def test_partial_promotion_with_missing_file_and_source_remaining_rolls_back():
    backup = "final.__previous_run"
    fs = FakePublicationFs(
        {
            "staging": {"part-1.parquet": 10, "part-2.parquet": 20},
            "final": {"old.parquet": 30},
        },
        partial_renames={("staging", "final"): {"part-1.parquet": 10}},
    )
    with pytest.raises(ValueError, match="restored and verified"):
        repair.promote_staging_paths(fs, "staging", "final", backup)
    assert fs.entries["final"] == {"old.parquet": 30}
    assert fs.entries["staging"] == {"part-1.parquet": 10, "part-2.parquet": 20}
    assert backup not in fs.entries


def test_cli_fallbacks_and_path_safety():
    args = repair.parse_arguments([])
    options = repair.resolve_options(
        args,
        {
            repair.SYNTHETIC_BASE_ENV: "/tmp/synthetic",
            repair.FALTANTES_ENV: "/tmp/faltantes",
            repair.SPECS_ENV: "/tmp/specs.json",
        },
    )
    assert options["prefix"] == "clones_instrumentos"
    assert options["specs"] == "/tmp/specs.json"

    with pytest.raises(ValueError, match="DATAGEN_SPECS_URI"):
        repair.resolve_options(
            args,
            {
                repair.SYNTHETIC_BASE_ENV: "/tmp/synthetic",
                repair.FALTANTES_ENV: "/tmp/faltantes",
            },
        )

    with pytest.raises(ValueError, match="root"):
        repair.resolve_options(
            SimpleNamespace(
                synthetic_base="/",
                prefix="clones",
                faltantes="/tmp/faltantes",
                specs="/tmp/specs.json",
                run_id="run",
                dry_run=False,
            )
        )
    with pytest.raises(ValueError, match="overlap"):
        repair.validate_work_paths(
            {"final": "/tmp/final", "staging": "/tmp/final/stage", "backup": "/tmp/bak"}
        )


def test_failure_stdout_retains_context_and_reached_metrics(monkeypatch, capsys):
    options = {
        "synthetic_base": "/tmp/synthetic",
        "prefix": repair.DEFAULT_PREFIX,
        "faltantes": "/tmp/faltantes",
        "specs": "/tmp/specs.json",
        "run_id": "failed-run",
        "dry_run": False,
    }
    report = repair.initial_report(options)
    report.update(
        status="failed",
        source_rows=10,
        relevant_keys=3,
        error_type="ValueError",
        error="injected",
    )

    class FakeSpark:
        def stop(self):
            pass

    monkeypatch.setattr(repair, "resolve_options", lambda _args: options)
    monkeypatch.setattr(repair, "create_spark_session", lambda: FakeSpark())
    monkeypatch.setattr(
        repair,
        "run_repair",
        lambda *_args: (_ for _ in ()).throw(
            repair.RepairFailure(report, ValueError("injected"))
        ),
    )
    assert repair.main([]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["run_id"] == "failed-run"
    assert payload["source_rows"] == 10
    assert payload["relevant_keys"] == 3
    assert payload["paths"] == report["paths"]


def test_missing_specs_failure_stdout_still_reports_all_paths(monkeypatch, capsys):
    monkeypatch.setenv(repair.SYNTHETIC_BASE_ENV, "/tmp/synthetic")
    monkeypatch.setenv(repair.FALTANTES_ENV, "/tmp/faltantes")
    monkeypatch.delenv(repair.SPECS_ENV, raising=False)
    monkeypatch.delenv(repair.PREFIX_ENV, raising=False)

    assert repair.main(["--run-id", "missing-specs"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["run_id"] == "missing-specs"
    assert payload["paths"] == {
        "source": "/tmp/synthetic/clones_instrumentos/OPERACAO",
        "final": "/tmp/synthetic/clones_instrumentos/OPERACAO",
        "staging": "/tmp/synthetic/clones_instrumentos/OPERACAO.__staging_missing-specs",
        "backup": "/tmp/synthetic/clones_instrumentos/OPERACAO.__previous_missing-specs",
        "faltantes": "/tmp/faltantes",
        "specs": None,
    }


def test_self_test(spark):
    report = repair.run_self_test(spark)
    assert report["status"] == "self-test-passed"
    assert report["matched_rows"] == 1
    assert report["spark_aqe"] == "false"
