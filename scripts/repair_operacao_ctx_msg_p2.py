"""Repair OPERACAO.NUM_ID_CTX_MSG_P2 in an existing synthetic clone.

The job nulls only non-null values listed in the faltantes Parquet dataset. It
is intentionally self-contained so it can be uploaded directly to OCI Data
Flow. Publication uses sibling staging and backup paths; object-store renames
are not atomic, so this job must have a single writer and may require manual
recovery from the reported backup path after a process crash.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE = "OPERACAO"
TARGET_COLUMN = "NUM_ID_CTX_MSG_P2"
PK_COLUMN = "NUM_ID_OPERACAO"
DEFAULT_PREFIX = "clones_instrumentos"

SYNTHETIC_BASE_ENV = "DATAGEN_SYNTHETIC_BASE_URI"
PREFIX_ENV = "DATAGEN_CLONE_PREFIX"
FALTANTES_ENV = "DATAGEN_FALTANTES_URI"
SPECS_ENV = "DATAGEN_SPECS_URI"

NORMALIZED_KEY_COLUMN = "__repair_ctx_msg_p2_key"
MATCH_MARKER_COLUMN = "__repair_ctx_msg_p2_match"
EXPECTED_PRESENT_COLUMN = "__repair_expected_present"
ACTUAL_PRESENT_COLUMN = "__repair_actual_present"

_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SPARK_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.adaptive.enabled": "false",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "30s",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class RepairFailure(RuntimeError):
    def __init__(self, report: dict[str, Any], cause: Exception):
        super().__init__(str(cause))
        self.report = report
        self.cause = cause


def create_spark_session(app_name: str = "RepairOperacaoCtxMsgP2") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in _SPARK_CONF.items():
        builder = builder.config(key, value)
    builder = builder.config(
        "spark.sql.shuffle.partitions", os.environ.get("DATAGEN_SHUFFLE_PARTITIONS", "200")
    )
    spark = builder.getOrCreate()
    configure_spark(spark)
    return spark


def configure_spark(spark: SparkSession) -> None:
    shuffle_partitions = int(os.environ.get("DATAGEN_SHUFFLE_PARTITIONS", "200"))
    if shuffle_partitions <= 0:
        raise ValueError("DATAGEN_SHUFFLE_PARTITIONS must be a positive integer")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))


def _default_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:12]}"


def parse_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Null listed missing references in {TABLE}.{TARGET_COLUMN}."
    )
    parser.add_argument("--synthetic-base")
    parser.add_argument("--prefix")
    parser.add_argument("--faltantes")
    parser.add_argument("--specs")
    parser.add_argument("--run-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def _is_root_path(path: str) -> bool:
    if path == "/":
        return True
    if path.startswith("dbfs:/"):
        return path.rstrip("/") == "dbfs:"
    parsed = urlsplit(path)
    if parsed.scheme:
        return parsed.path in ("", "/")
    return False


def validate_data_path(value: Any, label: str) -> str:
    path = str(value or "").strip()
    if not path:
        raise ValueError(f"{label} must not be empty")
    if _is_root_path(path):
        raise ValueError(f"{label} must not be a filesystem or object-store root: {path!r}")
    while len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    return path


def validate_prefix(value: Any) -> str:
    prefix = str(value or "").strip().strip("/")
    if not prefix:
        raise ValueError("--prefix/DATAGEN_CLONE_PREFIX must not be empty")
    if "://" in prefix or prefix.startswith("dbfs:"):
        raise ValueError("prefix must be relative to the synthetic base")
    parts = prefix.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError(f"unsafe prefix: {prefix!r}")
    return prefix


def resolve_options(
    args: argparse.Namespace, environ: Mapping[str, str] | None = None
) -> dict[str, Any]:
    env = os.environ if environ is None else environ
    synthetic_base = args.synthetic_base or env.get(SYNTHETIC_BASE_ENV)
    faltantes = args.faltantes or env.get(FALTANTES_ENV)
    specs = args.specs or env.get(SPECS_ENV)
    prefix = args.prefix if args.prefix is not None else env.get(PREFIX_ENV, DEFAULT_PREFIX)
    if not synthetic_base:
        raise ValueError(f"--synthetic-base or {SYNTHETIC_BASE_ENV} is required")
    if not faltantes:
        raise ValueError(f"--faltantes or {FALTANTES_ENV} is required")
    if not specs:
        raise ValueError(f"--specs or {SPECS_ENV} is required")
    run_id = str(args.run_id or _default_run_id()).strip()
    if not _RUN_ID_RE.fullmatch(run_id):
        raise ValueError(
            "run id must start with an alphanumeric and contain only "
            "letters, digits, dot, underscore, or hyphen"
        )
    return {
        "synthetic_base": validate_data_path(synthetic_base, "synthetic base"),
        "prefix": validate_prefix(prefix),
        "faltantes": validate_data_path(faltantes, "faltantes"),
        "specs": validate_data_path(specs, "specs"),
        "run_id": run_id,
        "dry_run": bool(args.dry_run),
    }


def build_paths(synthetic_base: str, prefix: str, run_id: str) -> dict[str, str]:
    final = f"{synthetic_base.rstrip('/')}/{prefix}/{TABLE}"
    paths = {
        "final": final,
        "staging": f"{final}.__staging_{run_id}",
        "backup": f"{final}.__previous_{run_id}",
    }
    validate_work_paths(paths)
    return paths


def _path_contains(parent: str, child: str) -> bool:
    return child.startswith(parent.rstrip("/") + "/")


def validate_work_paths(paths: Mapping[str, str]) -> None:
    normalized = {name: validate_data_path(path, name) for name, path in paths.items()}
    names = tuple(normalized)
    for index, left_name in enumerate(names):
        for right_name in names[index + 1 :]:
            left = normalized[left_name]
            right = normalized[right_name]
            if left == right or _path_contains(left, right) or _path_contains(right, left):
                raise ValueError(
                    f"unsafe path overlap between {left_name}={left!r} and "
                    f"{right_name}={right!r}"
                )


def normalize_key(column):
    """Match the generator's numeric-key normalization exactly."""
    return F.regexp_replace(F.trim(column.cast("string")), r"\.0+$", "")


def _required_columns(df: DataFrame, required: Sequence[str], label: str) -> dict[str, str]:
    by_upper: dict[str, list[str]] = {}
    for column in df.columns:
        by_upper.setdefault(column.upper(), []).append(column)
    missing = [column for column in required if column not in by_upper]
    ambiguous = {
        key: values
        for key, values in by_upper.items()
        if len(values) > 1 and key in required
    }
    if missing or ambiguous:
        raise ValueError(
            f"{label} schema invalid; missing={missing}, ambiguous={ambiguous}, found={df.columns}"
        )
    return {column: by_upper[column][0] for column in required}


def relevant_keys(faltantes: DataFrame) -> DataFrame:
    columns = _required_columns(faltantes, ("TABELA", "COLUNA", "VALOR"), "faltantes")
    key = normalize_key(F.col(columns["VALOR"]))
    normalized_table = F.element_at(
        F.split(F.upper(F.trim(F.col(columns["TABELA"]))), r"\."), -1
    )
    return (
        faltantes.where(
            (normalized_table == F.lit(TABLE))
            & (F.upper(F.trim(F.col(columns["COLUNA"]))) == F.lit(TARGET_COLUMN))
        )
        .select(key.alias(NORMALIZED_KEY_COLUMN))
        .where(
            F.col(NORMALIZED_KEY_COLUMN).isNotNull()
            & (F.col(NORMALIZED_KEY_COLUMN) != F.lit(""))
        )
        .dropDuplicates()
    )


def _scalar_row(frame: DataFrame):
    """Return one aggregate/metadata row without a DataFrame collection action."""
    rows = frame.rdd.take(1)
    return rows[0] if rows else None


def validate_source(source: DataFrame) -> dict[str, int]:
    missing = [column for column in (PK_COLUMN, TARGET_COLUMN) if column not in source.columns]
    if missing:
        raise ValueError(f"{TABLE} source schema is missing required column(s): {missing}")
    collisions = sorted(
        set(source.columns)
        & {
            NORMALIZED_KEY_COLUMN,
            MATCH_MARKER_COLUMN,
            EXPECTED_PRESENT_COLUMN,
            ACTUAL_PRESENT_COLUMN,
        }
    )
    if collisions:
        raise ValueError(f"{TABLE} source has reserved temporary column(s): {collisions}")
    stats = _scalar_row(
        source.agg(
            F.count(F.lit(1)).alias("rows"),
            F.count(F.when(F.col(PK_COLUMN).isNull(), F.lit(1))).alias("null_pks"),
        )
    )
    rows = int(stats["rows"])
    null_pks = int(stats["null_pks"])
    if null_pks:
        raise ValueError(f"{TABLE}.{PK_COLUMN} contains {null_pks} null value(s)")
    duplicate_groups = (
        source.groupBy(PK_COLUMN).count().where(F.col("count") > 1).limit(1).count()
    )
    if duplicate_groups:
        raise ValueError(f"{TABLE}.{PK_COLUMN} is not unique")
    return {"source_rows": rows, "source_pk_nulls": null_pks}


def normalize_specs(specs: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for raw_table, raw_config in specs.items():
        table = str(raw_table).strip().upper().split(".", 1)[-1]
        if table in normalized:
            raise ValueError(f"spec table collision after schema stripping: {table}")
        if not isinstance(raw_config, Mapping):
            raise ValueError(f"spec for {raw_table!r} must be an object")
        config = copy.deepcopy(dict(raw_config))
        config["pk_cols"] = [
            str(column).strip().upper() for column in (config.get("pk_cols") or [])
        ]
        raw_fks = config.get("foreign_keys")
        if not isinstance(raw_fks, (list, tuple)):
            raw_fks = config.get("fks") or []
        fks = []
        for raw_fk in raw_fks:
            if not isinstance(raw_fk, Mapping):
                continue
            fk = dict(raw_fk)
            fk["columns"] = [
                str(column).strip().upper() for column in (fk.get("columns") or [])
            ]
            if fk.get("parent_table"):
                fk["parent_table"] = str(fk["parent_table"]).strip().upper().split(".", 1)[-1]
            fks.append(fk)
        config["foreign_keys"] = fks
        config["not_null_cols"] = [
            str(column).strip().upper()
            for column in (config.get("not_null_cols") or [])
            if isinstance(column, str)
        ]
        normalized[table] = config
    return normalized


def load_specs(spark: SparkSession, uri: str) -> dict[str, Any]:
    records = spark.sparkContext.wholeTextFiles(uri).take(2)
    if len(records) != 1:
        raise ValueError(f"expected exactly one specs object at {uri!r}, found {len(records)}")
    try:
        parsed = json.loads(records[0][1])
    except json.JSONDecodeError as exc:
        raise ValueError(f"specs at {uri!r} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not parsed:
        raise ValueError(f"specs at {uri!r} must be a non-empty object")
    return normalize_specs(parsed)


def validate_specs(specs: Mapping[str, Any]) -> None:
    config = specs.get(TABLE)
    if not isinstance(config, Mapping):
        raise ValueError(f"specs is missing table {TABLE}")
    if list(config.get("pk_cols") or []) != [PK_COLUMN]:
        raise ValueError(
            f"specs {TABLE}.pk_cols must be exactly [{PK_COLUMN!r}], "
            f"found {config.get('pk_cols')!r}"
        )
    child_fk = any(
        TARGET_COLUMN in (fk.get("columns") or []) and bool(fk.get("parent_table"))
        for fk in (config.get("foreign_keys") or [])
        if isinstance(fk, Mapping)
    )
    if not child_fk:
        raise ValueError(f"specs {TABLE}.{TARGET_COLUMN} is not a child foreign key")
    if TARGET_COLUMN in set(config.get("not_null_cols") or []):
        raise ValueError(f"specs {TABLE}.{TARGET_COLUMN} is NOT NULL")


def _annotate_matches(source: DataFrame, keys: DataFrame) -> DataFrame:
    markers = keys.withColumn(MATCH_MARKER_COLUMN, F.lit(True))
    return (
        source.withColumn(NORMALIZED_KEY_COLUMN, normalize_key(F.col(TARGET_COLUMN)))
        .join(markers, on=NORMALIZED_KEY_COLUMN, how="left")
    )


def repair_dataframe(source: DataFrame, keys: DataFrame) -> DataFrame:
    target_type = source.schema[TARGET_COLUMN].dataType
    joined = _annotate_matches(source, keys)
    return joined.select(
        *[
            F.when(
                F.col(MATCH_MARKER_COLUMN).isNotNull() & F.col(column).isNotNull(),
                F.lit(None).cast(target_type),
            )
            .otherwise(F.col(column))
            .alias(column)
            if column == TARGET_COLUMN
            else F.col(column)
            for column in source.columns
        ]
    )


def _all_columns_equal(left_alias: str, right_alias: str, columns: Sequence[str]):
    predicate = F.lit(True)
    for column in columns:
        predicate = predicate & F.col(f"{left_alias}.`{column}`").eqNullSafe(
            F.col(f"{right_alias}.`{column}`")
        )
    return predicate


def _comparison_violations(expected: DataFrame, actual: DataFrame) -> int:
    expected_marked = expected.withColumn(EXPECTED_PRESENT_COLUMN, F.lit(True)).alias("expected")
    actual_marked = actual.withColumn(ACTUAL_PRESENT_COLUMN, F.lit(True)).alias("actual")
    joined = expected_marked.join(
        actual_marked,
        F.col(f"expected.`{PK_COLUMN}`") == F.col(f"actual.`{PK_COLUMN}`"),
        "full",
    )
    valid = (
        F.col(f"expected.`{EXPECTED_PRESENT_COLUMN}`").isNotNull()
        & F.col(f"actual.`{ACTUAL_PRESENT_COLUMN}`").isNotNull()
        & _all_columns_equal("expected", "actual", expected.columns)
    )
    return joined.where(~valid).limit(1).count()


def validate_integrity(
    source: DataFrame,
    candidate: DataFrame,
    keys: DataFrame,
    source_rows: int,
    *,
    label: str,
) -> dict[str, int]:
    if candidate.schema != source.schema or candidate.columns != source.columns:
        raise ValueError(f"{label} schema/order differs from source schema/order")
    candidate_stats = validate_source(candidate)
    candidate_rows = candidate_stats["source_rows"]
    if candidate_rows != source_rows:
        raise ValueError(
            f"{label} row count differs from source: {candidate_rows} != {source_rows}"
        )

    annotated = _annotate_matches(source, keys)
    matched_pks = annotated.where(
        F.col(MATCH_MARKER_COLUMN).isNotNull() & F.col(TARGET_COLUMN).isNotNull()
    ).select(PK_COLUMN)
    unmatched_pks = annotated.where(
        F.col(MATCH_MARKER_COLUMN).isNull() | F.col(TARGET_COLUMN).isNull()
    ).select(PK_COLUMN)
    matched_rows = matched_pks.count()

    expected = repair_dataframe(source, keys)
    exact_violations = _comparison_violations(expected, candidate)
    if exact_violations:
        raise ValueError(f"{label} failed exact distributed row transformation verification")

    matched_nonnull = (
        candidate.join(matched_pks, on=PK_COLUMN, how="left_semi")
        .where(F.col(TARGET_COLUMN).isNotNull())
        .limit(1)
        .count()
    )
    if matched_nonnull:
        raise ValueError(f"{label} retains non-null target values on matched rows")

    source_unmatched = source.join(unmatched_pks, on=PK_COLUMN, how="left_semi")
    actual_unmatched = candidate.join(unmatched_pks, on=PK_COLUMN, how="left_semi")
    unchanged_violations = _comparison_violations(source_unmatched, actual_unmatched)
    if unchanged_violations:
        raise ValueError(f"{label} changed one or more unmatched rows")

    return {
        f"{label}_rows": candidate_rows,
        "matched_rows": matched_rows,
        "unmatched_rows": source_rows - matched_rows,
        "exact_transform_violations": exact_violations,
        "matched_nonnull_rows": matched_nonnull,
        "unmatched_change_violations": unchanged_violations,
    }


def _hadoop_path(spark: SparkSession, path: str):
    jvm = spark.sparkContext._jvm
    return jvm.org.apache.hadoop.fs.Path(path)


def _filesystem(spark: SparkSession, path: str):
    hpath = _hadoop_path(spark, path)
    return hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hpath


def path_exists(spark: SparkSession, path: str) -> bool:
    fs, hpath = _filesystem(spark, path)
    return bool(fs.exists(hpath))


def recursive_file_manifest(fs, root) -> tuple[tuple[str, int], ...]:
    """Return deterministic recursive file metadata relative to ``root``."""
    root_text = str(fs.makeQualified(root)).rstrip("/")
    prefix = root_text + "/"
    iterator = fs.listFiles(root, True)
    files: list[tuple[str, int]] = []
    while iterator.hasNext():
        status = iterator.next()
        path = str(status.getPath())
        if not path.startswith(prefix):
            raise RuntimeError(f"file {path!r} is outside manifest root {root_text!r}")
        files.append((path[len(prefix) :], int(status.getLen())))
    return tuple(sorted(files))


def _manifest_if_present(fs, path) -> tuple[tuple[str, int], ...] | None:
    return recursive_file_manifest(fs, path) if fs.exists(path) else None


def _delete_and_verify(fs, path, *, error: str) -> None:
    try:
        deleted = bool(fs.delete(path, True))
        remains = bool(fs.exists(path))
    except Exception:
        deleted, remains = False, True
    if not deleted or remains:
        raise RuntimeError(error)


def delete_existing_staging(spark: SparkSession, staging_path: str) -> bool:
    fs, staging = _filesystem(spark, staging_path)
    if not fs.exists(staging):
        return False
    _delete_and_verify(
        fs,
        staging,
        error=f"failed to delete unique stale staging path {staging_path}",
    )
    return True


def _restore_backup_paths(fs, backup, final, expected_manifest) -> None:
    backup_text, final_text = str(backup), str(final)
    backup_manifest = _manifest_if_present(fs, backup)
    if backup_manifest != expected_manifest:
        raise RuntimeError(
            "CRITICAL: recovery backup changed before rollback; "
            f"preserve and inspect {backup_text}"
        )
    if fs.exists(final):
        _delete_and_verify(
            fs,
            final,
            error=(
                "CRITICAL: partial destination could not be removed; "
                f"manual recovery backup: {backup_text}"
            ),
        )
    try:
        fs.rename(backup, final)
    except Exception:
        pass
    backup_absent = not bool(fs.exists(backup))
    final_exists = bool(fs.exists(final))
    restored_manifest = _manifest_if_present(fs, final)
    if (
        not backup_absent
        or not final_exists
        or restored_manifest != expected_manifest
    ):
        raise RuntimeError(
            "CRITICAL: rollback could not restore the exact previous destination; "
            f"recover backup manually from {backup_text} to {final_text}"
        )


def promote_staging_paths(fs, staging, final, backup) -> dict[str, Any]:
    """Promote staging while retaining a manifest-verified previous destination."""
    staging_text, final_text, backup_text = map(str, (staging, final, backup))
    if not fs.exists(staging):
        raise ValueError(f"staging is absent before publication: {staging_text}")
    if fs.exists(backup):
        raise RuntimeError(f"refusing to replace existing recovery backup: {backup_text}")

    staging_manifest = recursive_file_manifest(fs, staging)
    had_previous = bool(fs.exists(final))
    previous_manifest = recursive_file_manifest(fs, final) if had_previous else None
    if had_previous:
        logger.warning(
            "Single-writer, non-atomic publication: after a crash restore %s to %s",
            backup_text,
            final_text,
        )
        try:
            fs.rename(final, backup)
        except Exception:
            pass
        final_absent = not bool(fs.exists(final))
        backup_manifest = _manifest_if_present(fs, backup)
        if not final_absent or backup_manifest != previous_manifest:
            raise RuntimeError(
                "CRITICAL: previous destination was not moved completely to its backup; "
                f"preserve and inspect recovery backup: {backup_text}"
            )

    try:
        fs.rename(staging, final)
    except Exception:
        pass
    staging_absent = not bool(fs.exists(staging))
    final_manifest = _manifest_if_present(fs, final)
    if not staging_absent or final_manifest != staging_manifest:
        if not had_previous:
            raise RuntimeError(
                "CRITICAL: staging promotion was incomplete and there was no previous "
                f"destination; inspect staging={staging_text} and final={final_text}"
            )
        _restore_backup_paths(fs, backup, final, previous_manifest)
        raise ValueError(
            "failed to promote staging completely; previous destination restored and verified"
        )

    return {
        "had_previous": had_previous,
        "backup_preserved": had_previous,
        "backup_path": backup_text if had_previous else None,
        "previous_manifest": previous_manifest,
        "staging_manifest": staging_manifest,
    }


def publish_staging(
    spark: SparkSession, staging_path: str, final_path: str, backup_path: str
) -> dict[str, Any]:
    fs, final = _filesystem(spark, final_path)
    staging = _hadoop_path(spark, staging_path)
    backup = _hadoop_path(spark, backup_path)
    return promote_staging_paths(fs, staging, final, backup)


def delete_verified_backup(spark: SparkSession, backup_path: str) -> None:
    fs, backup = _filesystem(spark, backup_path)
    if not fs.exists(backup):
        raise RuntimeError(f"verified backup disappeared before cleanup: {backup_path}")
    _delete_and_verify(
        fs,
        backup,
        error=f"final output is valid but backup cleanup failed; preserved at {backup_path}",
    )


def rollback_after_validation_failure(
    spark: SparkSession,
    *,
    final_path: str,
    backup_path: str,
    previous_manifest: tuple[tuple[str, int], ...],
    source_schema,
    source_rows: int,
) -> None:
    fs, final = _filesystem(spark, final_path)
    backup = _hadoop_path(spark, backup_path)
    _restore_backup_paths(fs, backup, final, previous_manifest)
    restored = spark.read.parquet(final_path)
    restored_metrics = validate_source(restored)
    if restored.schema != source_schema or restored_metrics["source_rows"] != source_rows:
        raise RuntimeError(
            "CRITICAL: rollback manifest matched but restored source integrity failed; "
            f"inspect {final_path} and recovery path {backup_path}"
        )


def initial_report(options: Mapping[str, Any]) -> dict[str, Any]:
    paths = build_paths(options["synthetic_base"], options["prefix"], options["run_id"])
    return {
        "status": "starting",
        "run_id": options["run_id"],
        "dry_run": bool(options["dry_run"]),
        "table": TABLE,
        "target_column": TARGET_COLUMN,
        "pk_column": PK_COLUMN,
        "paths": {
            "source": paths["final"],
            "final": paths["final"],
            "staging": paths["staging"],
            "backup": paths["backup"],
            "faltantes": options["faltantes"],
            "specs": options.get("specs"),
        },
        "warning": "single-writer publication; object-store rename is not atomic",
    }


def unresolved_failure_report(
    args: argparse.Namespace, environ: Mapping[str, str] | None = None
) -> dict[str, Any]:
    env = os.environ if environ is None else environ
    synthetic_base = args.synthetic_base or env.get(SYNTHETIC_BASE_ENV)
    prefix = args.prefix if args.prefix is not None else env.get(PREFIX_ENV, DEFAULT_PREFIX)
    faltantes = args.faltantes or env.get(FALTANTES_ENV)
    specs = args.specs or env.get(SPECS_ENV)
    run_id = args.run_id
    final = None
    if synthetic_base and prefix:
        final = f"{str(synthetic_base).rstrip('/')}/{str(prefix).strip('/')}/{TABLE}"
    return {
        "status": "starting",
        "run_id": run_id,
        "dry_run": bool(args.dry_run),
        "table": TABLE,
        "target_column": TARGET_COLUMN,
        "pk_column": PK_COLUMN,
        "paths": {
            "source": final,
            "final": final,
            "staging": f"{final}.__staging_{run_id}" if final else None,
            "backup": f"{final}.__previous_{run_id}" if final else None,
            "faltantes": faltantes,
            "specs": specs,
        },
        "warning": "single-writer publication; object-store rename is not atomic",
    }


def run_repair(spark: SparkSession, options: Mapping[str, Any]) -> dict[str, Any]:
    configure_spark(spark)
    report = initial_report(options)
    paths = build_paths(options["synthetic_base"], options["prefix"], options["run_id"])
    keys: DataFrame | None = None
    try:
        specs_uri = options.get("specs")
        if not specs_uri:
            raise ValueError(f"--specs or {SPECS_ENV} is required")
        validate_specs(load_specs(spark, specs_uri))
        report["specs_validated"] = True

        source = spark.read.parquet(paths["final"])
        source_metrics = validate_source(source)
        source_rows = source_metrics["source_rows"]
        source_schema = source.schema
        report.update(source_metrics)

        faltantes = spark.read.parquet(options["faltantes"])
        keys = relevant_keys(faltantes).persist()
        key_count = keys.count()
        report["relevant_keys"] = key_count
        if key_count == 0:
            raise ValueError(
                f"faltantes contains no nonblank keys for exact pair {TABLE}.{TARGET_COLUMN}"
            )

        repaired = repair_dataframe(source, keys)
        metrics = validate_integrity(
            source, repaired, keys, source_rows, label="repaired"
        )
        report.update(metrics)
        if metrics["matched_rows"] == 0:
            report["status"] = "no-op"
            report["paths"]["staging"] = None
            report["paths"]["backup"] = None
            return report

        if options["dry_run"]:
            report["status"] = "dry-run"
            return report

        if path_exists(spark, paths["backup"]):
            raise RuntimeError(
                f"recovery backup already exists; preserve and inspect it: {paths['backup']}"
            )
        report["stale_staging_deleted"] = delete_existing_staging(spark, paths["staging"])
        repaired.write.mode("append").parquet(paths["staging"])
        staged = spark.read.parquet(paths["staging"])
        staged_metrics = validate_integrity(
            source, staged, keys, source_rows, label="staged"
        )
        report.update(staged_metrics)
        logger.info(
            "%s",
            json.dumps(
                {
                    "event": "prepublish_validated",
                    "run_id": options["run_id"],
                    "source_rows": source_rows,
                    "staged_rows": staged_metrics["staged_rows"],
                    "matched_rows": staged_metrics["matched_rows"],
                    "staging": paths["staging"],
                    "final": paths["final"],
                    "backup": paths["backup"],
                },
                sort_keys=True,
            ),
        )
        promotion = publish_staging(
            spark, paths["staging"], paths["final"], paths["backup"]
        )
        previous_manifest = promotion.pop("previous_manifest")
        staging_manifest = promotion.pop("staging_manifest")
        report.update(promotion)
        report["previous_manifest_files"] = len(previous_manifest or ())
        report["published_manifest_files"] = len(staging_manifest)
        if not promotion["had_previous"] or previous_manifest is None:
            raise RuntimeError(
                "source destination disappeared before publication; no verified backup exists"
            )

        try:
            backup_source = spark.read.parquet(paths["backup"])
            published_final = spark.read.parquet(paths["final"])
            final_metrics = validate_integrity(
                backup_source,
                published_final,
                keys,
                source_rows,
                label="final",
            )
            report.update(final_metrics)
            report["postpublish_validated"] = True
        except Exception as validation_error:
            report["postpublish_validated"] = False
            try:
                rollback_after_validation_failure(
                    spark,
                    final_path=paths["final"],
                    backup_path=paths["backup"],
                    previous_manifest=previous_manifest,
                    source_schema=source_schema,
                    source_rows=source_rows,
                )
                report["rollback_restored"] = True
                report["backup_preserved"] = False
                report["backup_path"] = None
            except Exception as recovery_error:
                report["rollback_restored"] = False
                try:
                    report["backup_preserved"] = path_exists(spark, paths["backup"])
                except Exception:
                    report["backup_preserved"] = None
                report["backup_path"] = paths["backup"]
                raise RuntimeError(
                    "CRITICAL: post-publication validation and recovery failed; "
                    f"preserve backup {paths['backup']}: {recovery_error}"
                ) from recovery_error
            raise ValueError(
                "post-publication validation failed; previous destination restored "
                f"and verified: {validation_error}"
            ) from validation_error

        try:
            delete_verified_backup(spark, paths["backup"])
        except Exception:
            report["backup_preserved"] = True
            report["backup_path"] = paths["backup"]
            raise
        report["backup_preserved"] = False
        report["backup_path"] = None
        report["status"] = "published"
        return report
    except RepairFailure:
        raise
    except Exception as exc:
        report["status"] = "failed"
        report["error_type"] = type(exc).__name__
        report["error"] = str(exc)
        raise RepairFailure(report, exc) from exc
    finally:
        if keys is not None:
            keys.unpersist()


def run_self_test(spark: SparkSession) -> dict[str, Any]:
    from decimal import Decimal

    source = spark.createDataFrame(
        [
            (1, Decimal("10.000"), "change"),
            (2, Decimal("20.500"), "keep"),
            (3, None, "already-null"),
        ],
        f"{PK_COLUMN} long, {TARGET_COLUMN} decimal(38,9), PAYLOAD string",
    )
    faltantes = spark.createDataFrame(
        [
            (" operacao ", " num_id_ctx_msg_p2 ", "10.000"),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", "10"),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2", "999.0"),
        ],
        "TABELA string, COLUNA string, VALOR string",
    )
    keys = relevant_keys(faltantes)
    source_metrics = validate_source(source)
    repaired = repair_dataframe(source, keys)
    metrics = validate_integrity(
        source, repaired, keys, source_metrics["source_rows"], label="repaired"
    )
    if keys.count() != 2 or metrics["matched_rows"] != 1:
        raise AssertionError("self-test key normalization or selective repair failed")
    return {
        "status": "self-test-passed",
        "source_rows": source_metrics["source_rows"],
        "relevant_keys": 2,
        "matched_rows": metrics["matched_rows"],
        "spark_aqe": spark.conf.get("spark.sql.adaptive.enabled"),
        "spark_timezone": spark.conf.get("spark.sql.session.timeZone"),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_arguments(argv)
    if not args.self_test and not args.run_id:
        args.run_id = _default_run_id()
    spark: SparkSession | None = None
    options: dict[str, Any] | None = None
    failure_context = None if args.self_test else unresolved_failure_report(args)
    try:
        options = None if args.self_test else resolve_options(args)
        if options is not None:
            failure_context = initial_report(options)
        spark = create_spark_session()
        if args.self_test:
            report = run_self_test(spark)
        else:
            report = run_repair(spark, options)
        print(json.dumps(report, sort_keys=True, default=str))
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI must emit a structured failure report
        logger.exception("repair failed: %s", exc)
        if isinstance(exc, RepairFailure):
            failure_report = exc.report
        else:
            failure_report = dict(failure_context or {})
            failure_report.update(
                {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
        print(json.dumps(failure_report, sort_keys=True, default=str))
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
