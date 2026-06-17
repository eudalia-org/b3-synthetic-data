# Engorda Session Handoff / Context

**Date:** 2026-06-17
**Purpose:** Complete context for `engorda_tables.py` so a new session can continue without re-deriving anything. Read this first.

---

## 1. The big picture

Three-stage synthetic-data pipeline, each stage its own **OCI Data Flow** application, with **Object Storage as the handoff** between stages:

1. **Ingest** — `save_tables.py` reads source Oracle tables → writes raw Parquet to `{RAW_BASE}/[{RAW_PREFIX}/]{TABLE}` (per-table dir, **no date** in path).
2. **Engorda (this work)** — `engorda_tables.py` reads raw Parquet → generates synthetic ("fattened") relational data preserving PK uniqueness + FK integrity → writes synthetic Parquet to `{SYNTHETIC_BASE}/[{SYNTHETIC_PREFIX}/]{TABLE}`.
3. **Load** — reads synthetic Parquet → loads into target OracleDB.

"Engorda" = Portuguese for "fatten" — grow tables beyond source size.

`engorda_tables.py` is **self-contained** (no `archive.zip`): the proven multi-table synthesizer from `transform/transform.py` was trimmed to its Parquet-only paths and **vendored** into the single file, under a thin entrypoint. Auth is left to the Data Flow environment (resource principal + native `oci://`), so no OCI-auth code is bundled.

## 2. Where everything is

- **Script:** `engorda_tables.py` (~2370 lines: vendored synthesizer + entrypoint).
- **Tests:** `tests/test_engorda_tables.py` (35 pure-unit + 1 skippable local-Spark integration).
- **Config:** `specs.json` (repo root) — the synthesis config; also belongs in Object Storage at `datagen/configs/specs.json`.
- **Design doc:** `docs/plans/2026-06-16-engorda-tables-design.md`
- **Implementation plan:** `docs/plans/2026-06-16-engorda-tables-implementation.md`
- **This handoff:** `docs/plans/2026-06-17-engorda-session-handoff.md`
- Branch: `main`, pushed to `origin` = `git@github.com:eudalia-org/data-generation.git` (repo moved from `eudal1a/`; remote URL already updated locally).

## 3. How engorda works

Entrypoint flow (`main` → `engorda`):
1. `get_engorda_env()` reads required env vars (below), normalizes (rstrip base, strip prefix).
2. `load_specs(spark, specs_uri)` reads the single specs.json object via `sparkContext.wholeTextFiles` (asserts exactly 1 record), `json.loads`, validates non-empty dict, then **`normalize_specs`** reduces dotted `OWNER.TABLE` keys + FK `parent_table` refs to bare `TABLE` (rejects collisions).
3. `connected_components(specs)` splits tables into FK-connected groups (union-find; edge only when parent is in specs).
4. For each component (processed **alphabetically**, one at a time, memory released between):
   - read each table's raw Parquet (`read_parquet`, applies `.limit(N)` if `--limit`),
   - `effective_n_rows(...)` computes per-table target,
   - `run_synthesis_from_tables(...)` synthesizes (validate_mode="full") **without writing**,
   - `write_synthetic_table(...)` writes each table (see §7),
   - `release(...)` + `spark.catalog.clearCache()`.
5. Per-component failures: `--continue-on-error` collects and exits non-zero at end; otherwise re-raises.

### CLI
- `--scale-factor FLOAT` (default 1.0) — global growth multiplier for non-static tables.
- `--limit INT` (default None = no limit) — read at most N rows **per raw table** before synthesizing (input sampling for fast test runs; orphans some FKs, so validates plumbing/dtypes, not FK fidelity).
- `--seed INT` (default 42).
- `--continue-on-error`.
- `--specs PATH` — overrides `DATAGEN_SPECS_URI`.

### Env vars (Spark `driverEnv.*`; set `executorEnv.*` too for predictability)
Required: `DATAGEN_RAW_BASE_URI`, `DATAGEN_SYNTHETIC_BASE_URI`, `DATAGEN_SPECS_URI`.
Optional: `DATAGEN_RAW_PREFIX`, `DATAGEN_SYNTHETIC_PREFIX`.
**OCI URI form is `oci://<bucket>@<namespace>`** (bucket first) — a common mistake is swapping them.

### Volume rules (`effective_n_rows`)
Per table, in order: empty source → 0; **static → keep source count 1:1 (any `n_rows` override ignored, warns)**; `n_rows` override (non-static) wins over factor; else `round(count × scale_factor)`. Then **parent floor**: a non-static FK-parent table is bumped to `max(target, source_count)` because the synthesizer bootstraps parents with `keep_all_source_rows=True` (requires target ≥ source).

## 4. The data being processed (real numbers)

From the ingestion size sheet: **~3.49 billion rows, ~117 GB Parquet, 47 tables.** Dominant fact tables:

| Table | Rows | Disk GB |
|---|---:|---:|
| CONDICAO_IF | 665 M | 10.3 |
| EVENTO | 488 M | 11.2 |
| ENTIDADE | 318 M | 5.4 |
| INSTRUMENTO_FINANCEIRO | 297 M | 21.6 |
| TITULO | 282 M | 8.0 |
| CREDITO | 264 M | 16.0 |
| RELACAO | 205 M | 4.3 |
| RESGATE | 184 M | 3.2 |
| LANCAMENTO | 39 M | 14.4 |

The other ~30 tables are small `TIPO_*`/`NAT_*`/`PARAMETRIZACAO_*`/code tables.

## 5. specs.json (current, committed)

47 tables. **22 reference/code tables marked `"static": true`** so they're copied 1:1 (no PK regeneration). FKs all have explicit `parent_table` + `parent_columns`.

**Why static matters:** `TIPO_DEBITO`'s PK `COD_TIPO_DEBITO` is `Decimal(2,0)` (max 99). With default `append_after_max_pk`, engorda tried to mint PKs up to 135 → `OverflowError`. Reference/code tables must not be fattened; static fixes the whole overflow class and keeps FK codes valid.

Static set: TIPO_DEBITO, TIPO_POSICAO_CARTEIRA, TIPO_IF, TIPO_OPER_OBJETO_SERV, TIPO_OPER_PTA_CARTEIRA, TIPO_DADO_OPERACAO, NAT_ECO_TIPO_IF, NAT_ECON_TP_OPER_PONTA, NATUREZA_ECONOMICA, MODALIDADE_LIQUIDACAO, MOTIVO_SITUACAO_IF, SITUACAO_CONTA, FORMA_PAGAMENTO, PAPEL_PARTICIPANTE, OBJETO_SERVICO, OPCAO_RECOMPRA, CERTIFICACAO_CETIP, PARAMETRIZACAO_REGIME_MERCADO, PARAMETRIZACAO_TIPO_REGIME, PARAMETRO_CONFIG, TCTPFEATURE_TOGGLE, TCTPHABILITA_OPERACAO_SERVICO.

**Watch for:** other non-static tables whose PK type can't hold their range will hit the same overflow — static them, or set `append_after_max_pk=False` for that table.

### Connected-component analysis (computed from specs.json)
**37 components; only 4 multi-table; 33 singletons.** The FK graph does **not** create a mega-component. Largest components by source rows:
- `CONDICAO_IF` alone — 665 M (singleton)
- `EVENTO` alone — 488 M (singleton)
- CARTEIRA cluster — ~352 M across `CARTEIRA_COMITENTE`, `CARTEIRA_PARTICIPANTE`, `ESPECIFICACAO_COMITENTE`, `OPERACAO` (+ static lookups)
- `INSTRUMENTO_FINANCEIRO` (+ static MOTIVO_SITUACAO_IF) — 297 M

**Peak memory ≈ one ~665 M-row table at a time**, not the whole 117 GB. The memory-bounded design pays off.

## 6. Cluster sizing (decided)

Hard constraint: **max 4 executors**, but **no per-executor OCPU cap**, shape **VM.Standard.E5.Flex**. So scale **up**, not out.

**Stage 1 — limited smoke (validate correctness + dtypes, cheap):**
```
--limit 1000000 --scale-factor 1.0 --continue-on-error
Driver:    E5.Flex  8 OCPU / 64 GB
Executors: E5.Flex  8 OCPU / 64 GB × 4
spark.sql.shuffle.partitions = 128
```

**Stage 2 — full volume, no growth first:**
```
--scale-factor 1.0 --continue-on-error
Driver:    E5.Flex  16 OCPU / 128 GB
Executors: E5.Flex  32 OCPU / 256 GB × 4   (~128 OCPU, ~1 TB executor RAM)
spark.sql.shuffle.partitions = 1024
```
Only turn on growth (`--scale-factor 2.0`+) after Stage 2 validates at full volume; a 3× factor on 117 GB → ~350 GB output + shuffle.

**Known wall-clock gate:** the synthesizer assigns PK row-ids via a non-partitioned `Window.orderBy()` in `_with_contiguous_row_id` → **single task per table**. CONDICAO_IF (665 M) and EVENTO (488 M) each run that stage on one core; vertical scaling cuts spill and speeds joins but not that sort. If it dominates, the fix is code-side (partition-aware id), not cluster-side.

## 7. The OCI overwrite bug + fix (most recent work)

**Symptom:** Stage-1 smoke wrote synthetic Parquet but **only `USUARIO` survived** (alphabetically last), no errors, all `n_rows > 0`.

**Root cause:** Spark `df.write.mode("overwrite")` on the OCI HDFS connector deletes the **shared parent prefix** (`…/synthetic/`) before each write, so every component wiped the previous components' output; only the last-processed table remained.

**Fix (commit `eff13ba`):** engorda no longer passes `save_path` to the synthesizer. It synthesizes, then writes each table with `write_synthetic_table(spark, df, out_path)`:
1. `_delete_path(spark, out_path)` — deletes **exactly that table's prefix** via the Hadoop `FileSystem` API (`Path.getFileSystem(conf).delete(path, True)`), never the parent.
2. `df.write.mode("append").parquet(out_path)`.

**Write semantics:** per run, per table, **full table-level overwrite** — delete that table's prefix then append fresh data. Idempotent reruns, siblings untouched. Caveat: the delete→append is **not transactional**; don't run the load stage against a table while engorda is mid-write on it (fine for the sequential batch flow).

## 8. State of verification

- 35 unit tests pass; `ruff` clean; `import engorda_tables` OK.
- The local-Spark **integration test cannot run on this machine**: PySpark 4.1 needs **Java 17–21**, but only JDK 11 (too old) and JDK 25 (too new — `Subject.getSubject` removed) are installed. Not a code defect; on Data Flow (matching JDK/Spark) it runs. To run locally, install Temurin 21 and set `JAVA_HOME`.
- Run tests with the venv directly: `.venv/bin/python -m pytest tests/test_engorda_tables.py -q` (note: `uv run` is broken here — modified `pyproject.toml` references a missing local `eudalia` distribution). `ruff` is at `/opt/homebrew/bin/ruff`.

## 9. Immediate next steps (where we left off)

1. Re-put the script: `oci os object put --bucket-name <b> --file engorda_tables.py --name datagen/apps/scripts/engorda_tables.py --force`.
2. Clear stale output once: `oci os object bulk-delete --bucket-name <b> --prefix synthetic/`.
3. Re-put `specs.json` to `datagen/configs/specs.json` if not already current.
4. Rerun the Stage-1 smoke, then verify **all ~37 table prefixes** exist:
   `oci os object list --bucket-name <b> --prefix synthetic/ --all --fields name | grep -oE 'synthetic/[^/]+/' | sort -u`
   and check the `writing <TABLE> -> …` log lines.
5. **Confirm output dtypes vs Oracle target columns** (the `ORA-01722 invalid number` trap from a prior load session — synthetic key came out `StringType` for a numeric Oracle column; the string-PK path produces `SYN_…` values). PKs here are `NUM_*`/numeric, so should be fine if the raw Parquet preserved numeric types.
6. If the smoke is clean, move to Stage-2 full run.

## 10. Open follow-ups / risks

- **Row-id single-task bottleneck** (§6) — may need a code fix for the 600 M-row tables if Stage 2 is too slow.
- **Other PK-overflow tables** beyond TIPO_DEBITO may surface on non-static tables.
- **Load idempotency** is a separate concern in the load stage (append-only there, per prior sessions).
- **Dependabot:** 25 vulns on default branch (pinned deps in `requirements.txt`) — unrelated to engorda.

## 11. Memory / prior sessions

No memory zettels existed at the start of this work (memory dir was empty). 9 prior sessions are indexed in `sessions-index.json` but their transcript `.jsonl` files are **not on disk** — only summaries are readable. Relevant prior topics: transform design, load.py, the `ORA-01722` load type-mismatch, Data Flow archive packaging, ADB TLS/TNS connection setup. A `MEMORY.md` pointer to this handoff has been added so future sessions discover it.
