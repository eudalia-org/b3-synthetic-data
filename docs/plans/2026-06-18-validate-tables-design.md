# validate_tables.py — Offline DB-Constraint Validator (Design)

Date: 2026-06-18
Status: design / awaiting approval
Related: `engorda_tables.py`, `load_tables.py`, `specs.json`,
`scripts/extract_constraints.sql`, `scripts/build_specs_from_constraints.py`,
`docs/plans/2026-06-16-engorda-tables-design.md`

## Problem

The load step (`load_tables.py`) writes engorda's synthetic Parquet into the
real, FK-constrained CETIP Oracle tables. Loads have failed one database
constraint at a time (ORA-01400 not-null, ORA-02291 FK parent missing,
precision overflow, PK collisions). Each failure costs a full cluster run to
discover. We need to validate the synthetic Parquet against the **same checks
the database enforces**, *without* running a load — turning a slow,
one-error-at-a-time loop into a single offline pass that reports everything
wrong at once.

## Scope

Mirror the row-rejection checks Oracle performs on insert:

| Class | Validated | Source |
|---|---|---|
| PK: not-null + unique (incl. collision with existing rows) | ✅ | `specs.json` |
| FK: referential integrity | ✅ | `specs.json` |
| UNIQUE constraints | ✅ | `schema.json` |
| NOT NULL on plain columns | ✅ | `schema.json` |
| Datatype precision/scale/length overflow | ✅ | `schema.json` |
| CHECK constraints | ❌ out of scope | — |

CHECK constraints are excluded: they require parsing/translating Oracle
`search_condition` SQL into Spark predicates (high effort, deferred).

## Key correctness principle: the "raw ∪ synthetic" universe

Oracle validates each constraint against the table's state **after the load
commits** = existing real rows **+** the synthetic rows being inserted. The
validator reproduces this fully offline because the **raw Parquet** ingested by
`save_tables.py` is the snapshot of the real DB:

- **PK uniqueness** = synthetic PKs internally unique **and** disjoint from raw
  PKs (catches a synthetic key colliding with an existing real key; verifies,
  rather than assumes, engorda's `source_max + id` offset).
- **FK referential** = each non-null synthetic FK value's parent key exists in
  **(raw ∪ synthetic) parent PKs** — so FKs to static/code tables (whose keys
  live in raw/real but not in the synthetic output) are correctly accepted.

No database connection is required.

## Architecture

Three pieces, mirroring the existing constraint→specs tooling.

### 1. Metadata → `schema.json`

- **`scripts/extract_schema.sql`** — dumps, for the target schema (`CETIP`):
  - per-column metadata from `all_tab_columns`: `data_type`,
    `data_precision`, `data_scale`, `char_length`, `nullable`;
  - UNIQUE (`U`) constraint columns from `all_constraints` /
    `all_cons_columns`, with position for composite pairing.
  Exported to CSV(s) the same way as `extract_constraints.sql`.
- **`scripts/build_schema_from_dump.py`** — pure/testable, mirrors
  `build_specs_from_constraints.py`. Produces:

  ```json
  {
    "TABLE": {
      "columns": {
        "NUM_IF":  {"type": "NUMBER",   "precision": 38, "scale": 0, "nullable": false},
        "COD_X":   {"type": "VARCHAR2", "length": 20, "nullable": true}
      },
      "unique": [["COL_A", "COL_B"]]
    }
  }
  ```

`schema.json` carries column domains + UNIQUE only. PK/FK stay in `specs.json`.
Each manifest is single-purpose; the validator reads both.

Domains come from `schema.json` — the **real Oracle column** precision/scale —
*not* the Parquet's stored type. This matters: unconstrained Oracle `NUMBER`
reads back over JDBC as `Decimal(38,9)`, so the Parquet's own type would
misreport the true domain. The validator always checks against the Oracle
metadata.

### 2. `validate_tables.py` — the app

Self-contained OCI Data Flow app, same skeleton as `engorda_tables.py`. Built
so the core is **importable and callable from an OCI Data Science notebook**
with a caller-supplied SparkSession — no logic locked behind `spark-submit` /
`__main__`.

Module boundaries:

- **`create_spark_session()`** — CLI path only; a notebook already has `spark`. (Named to match `engorda_tables.py`.)
- **`load_manifests(specs_uri, schema_uri) -> (specs, schema)`** — read +
  normalize (`OWNER.TABLE` → `TABLE`, reusing engorda's `normalize_specs`).
  Accepts dicts directly too, for notebook use.
- **`validate(spark, specs, schema, raw_base, synth_base, tables=None) -> Report`**
  — the core entrypoint. Takes a session + loaded manifests, returns a
  `Report`. No `sys.exit`, no Spark construction, no env reads inside it.
- **`render_summary(report) -> str`** / **`report_to_json(report) -> dict`** —
  formatting, Spark-independent.
- **`main()`** — CLI / Data Flow wrapper: parse args → read env →
  `create_spark_session()` → `load_manifests()` → `validate()` → write JSON
  report to `--report-uri` + print `render_summary()` →
  `sys.exit(1 if report.has_violations else 0)`.

Env vars: `DATAGEN_RAW_BASE_URI`, `DATAGEN_SYNTHETIC_BASE_URI`,
`DATAGEN_SPECS_URI`, `DATAGEN_SCHEMA_URI` (new). CLI: `--report-uri`,
`--specs`, `--schema`, `--tables` (subset; important for interactive
per-table notebook validation).

Notebook usage:

```python
from validate_tables import validate, load_manifests, render_summary
specs, schema = load_manifests(specs_uri, schema_uri)
report = validate(spark, specs, schema, raw_base, synth_base,
                  tables=["JUROS_FLUTUANTE"])
print(render_summary(report))
report.findings  # inspect interactively
```

### 3. The checks (approach A: column-pruned, independent)

Each check reads only the columns it needs — never full rows — so the pass is
memory-light without engorda's connected-component batching. Each is factored
as a function taking DataFrame(s) and returning `Finding`s.

| Check | Method |
|---|---|
| **NOT NULL** | per non-nullable column: `df.filter(col.isNull()).count()` |
| **Datatype domain** | Decimal: integer-part overflow only — `abs(col) >= 10**(precision − scale)`. VARCHAR: `length(col) > char_length`. Reuses engorda `_pk_capacity` math. **No scale-digit check** — Oracle *rounds* excess NUMBER scale on insert, it does not reject, so flagging it would be stricter than the DB (false positives). |
| **PK not-null + unique** | `groupBy(pk_cols).count()` filter `> 1`; **plus** anti-join synthetic PK vs raw PK (collision with existing rows) |
| **FK referential** | anti-join child's non-null FK columns vs **(raw ∪ synthetic) parent PK columns** |
| **UNIQUE** | `groupBy(cols).count()` filter `> 1`, ignoring all-null rows (matches Oracle's treatment of nulls in unique keys) |

Parent PK sets are read column-pruned; bare `distinct`/`max` on PK columns
benefit from `spark.sql.parquet.aggregatePushdown=true` (footer-only).

## Output & failure signaling

- **Full report, no fail-fast**: every table/check runs; one report lists all
  violations.
- **`Report`** dataclass: `findings: list[Finding]`, each
  `{table, check, column_or_constraint, violation_count, sample: [...],
  ok: bool}`; plus `has_violations` and `summary_counts`.
- **JSON report** (full, with samples of offending keys/values) written to
  `--report-uri` on object storage.
- **stdout summary** (`render_summary`): per-table ✅/❌ with counts, violations
  first — visible in Data Flow logs.
- **Exit code**: `sys.exit(1)` if any violations → acts as a hard pre-load
  gate; `0` otherwise.

## Testing

Same strategy as the engorda / build_specs tests
(`.venv/bin/python -m pytest`; `uv run` is broken via pyproject):

- **Pure unit tests** (no Spark): `build_schema_from_dump` (CSV → `schema.json`,
  composite UNIQUE pairing, nullability/precision parsing), manifest
  normalization, `render_summary`, `report_to_json`, the datatype-overflow
  math.
- **Check-logic tests**: each check is a function over DataFrames; tested with
  small in-memory DataFrames where a Spark session is available. The full
  Data Flow integration run stays a manual/skipped test (local JDK 17–21 gap,
  as with engorda).

## Out of scope / follow-ups

- CHECK constraint validation (search_condition translation).
- Wiring the validator as an automatic stage between engorda and load in the
  Data Flow pipeline (this design delivers the standalone gate first).
- Could later share a small common module with `engorda_tables.py`
  (`normalize_specs`, `_pk_capacity`); kept self-contained for now to match the
  one-file-per-app pattern.
