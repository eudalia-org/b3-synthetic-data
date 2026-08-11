# CDB Simplificado — Validation Debug Loop: Handoff / Session State (2026-07-25)

Everything needed to continue the clone-validation debug loop. Written at session
compaction; the previous context covered 2026-07-18 → 2026-07-25.

## LOADED INTO QAB — 2026-07-26 01:15

19/19 tables, 13,889,830 rows in 617 s, run_id `clones500kx2_20260725`, manifest at
`oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/clones_instrumentos/_load_manifests/clones500kx2_20260725`
(rollback via rollback_load.py). All PK dup-guards clean (0 existing keys in every
offset band). Notes: ran with --skip-validation (target constraints
disabled/relaxed — also means QAB does NOT enforce FKs at insert); the loader
NULLED the self-ref FK columns NUM_IF_ORIGEM/NUM_IF_PERTENCE on
INSTRUMENTO_FINANCEIRO inserts — a known divergence from the validated parquet
(both columns are FK-clean in the batch but arrive NULL in QAB).
NEXT: NoMe batch on top — the true acceptance (watch ClassCast, ORA-01400
COD_MOTIVO, SEM MODALIDADE; Cat 6 combos were only best-effort-checked).

## GATE PASSED — 2026-07-25 (late night)

`296 checks | 0 ERROR | 0 WARN` on the regenerated 500k×2 batch, after: AQE forced
off in all four jobs (Spark 3.5.0 join row-loss, commit 1947be1) → complete
--universe all enumeration → regeneration with the full faltantes → baseline v5 from
the batch's own MAPA → gate. The debug loop is CLOSED. Next: QAB append
(datagen.load_tables — verify rollback manifest first), then the NoMe batch on top
(the true acceptance; watch ClassCast / ORA-01400 / SEM MODALIDADE). Everything
below is the history of how we got here.

## Where the loop stands RIGHT NOW (superseded — see above)

**The reactive faltantes loop does NOT converge — stop iterating it.** Two
Oracle-stable runs (2026-07-24 22:30 and 2026-07-25 11:44, all union notes present,
0 WARN) each ended `3 ERROR`, and the orphan keys are FRESH each time
(708 → 646 → 481 faltantes rows; sample values disjoint between runs). Everything
else passes: shapes (7a 0.5%, 7b TVD 0.013, 7c 2.9–3.0%, 7d clean), polymorphism,
NOT NULL, dates, domain. The ONLY remaining defect class is raw→QAB drift on
COMITENTE.NUM_ID_ENTIDADE and CONTA.NUM_CONTA (via CARTEIRA_COMITENTE +
ESPECIFICACAO_COMITENTE).

Root cause, confirmed in code (2026-07-25):

1. **Every regeneration is a fresh draw.** `seleciona_instrumentos` samples with
   `valido.orderBy(F.rand(seed)).limit(n)` (`tests/engorda_instrumentos.py:1220`).
   Pruning the domain via faltantes changes row order, so even with the same seed the
   batch shifts wholesale — and pruned IFs are *replaced* to keep N. Each new batch
   carries the domain's ~constant drift rate → a new ~400–600-key faltantes every run.
2. **Known-bad keys are forgotten.** `emit_faltantes` writes `mode("overwrite")` to the
   same URI (`scripts/validate_cdb_simplificado.py:826`), so each run's prune list
   replaces (not extends) the previous one; IFs referencing older bad keys re-enter
   the eligible domain.

Discovery rate per iteration ≈ the batch's share of the 17.9M-IF domain, so the loop
would need hundreds of iterations. Exits, in order of preference:

- **A (fast gate, tiny generator change): freeze the batch.** The generator already has
  an explicit-list mode (`--num-ifs`, validated at `engorda_instrumentos.py:1199-1216`,
  aborts naming any pruned IF), but it only takes a comma list — impractical for
  thousands of sources. Teammate adds a file variant (`--num-ifs-parquet`, mirroring
  `--faltantes-parquet`). Workflow: take run B's `MAPA_CLONE_NUM_IF.NUM_IF_ORIG`,
  subtract the IFs whose raw CARTEIRA_COMITENTE rows hit the 481 bad keys (that table
  carries NUM_IF + both bad columns), regenerate with that frozen list → every surviving
  IF's keys were already union-verified this run → clean by construction, one iteration.
  (Batch shrinks by ~300–400 sources; acceptable for test data.)
- **B (durable — IMPLEMENTED 2026-07-25, chosen path): enumerate the FULL drift set
  once.** `scripts/enumerate_faltantes.py` (self-contained, `--self-test` green):
  JDBC-reads QAB's COMITENTE.NUM_ID_ENTIDADE and CONTA.NUM_CONTA key columns
  (partitioned single-column reads), anti-joins the distinct keys referenced by raw
  CARTEIRA_COMITENTE across the FILTRO_BASE domain, writes the complete faltantes
  parquet (replaces its two (TABELA, COLUNA) pairs, preserves any other pairs at
  --output). Run it on the VALIDATOR Data Flow app (needs ojdbc + private endpoint +
  DATAGEN_SOURCE_* env). Then ANY sample is clean and the loop disappears. Re-run
  right before each regeneration (drift grows with time). ALWAYS `--universe all`
  (now the default): the FILTRO_BASE 'domain' reproduction PROVED INCOMPLETE — the
  generator's product query has an EVENTO closure it lacks, so the 2026-07-25 evening
  comparison saw 329 missing keys (domain) vs 1,224 (all), incl. a whole band of
  NUM_CONTA above QAB's max 95,325,659 (raw is newer than QAB). Same-day deployment
  gotchas: the generation app's file-uri is `scripts/engorda_tables.py` — misleading
  NAME, its CONTENT is engorda_instrumentos.py (verify with object roundtrips, never
  by filename); and runs dying at ~2 min with no logs + null lifecycle-details were
  Data Flow DNS flakiness to Object Storage (UnknownHostException → circuit breaker)
  — rerun, smaller shapes helped, `spark.task.maxFailures=10` rides out the 30 s
  breaker window.
- **C (hygiene — IMPLEMENTED 2026-07-25): accumulate faltantes.** `emit_faltantes`
  now appends only-new keys to the existing parquet (never collects existing keys to
  the driver, so a large enumerated file at the same path is fine). Alone it does NOT
  fix convergence, but known-bad keys can no longer leave the prune list.

There is no teammate anymore (2026-07-25): Mateus owns `tests/engorda_instrumentos.py`
too. Agreed order to prod: enumerate (B) → regenerate → gate → load current scale →
NoMe acceptance → production-size run (fresh enumeration first). Option A (freeze the
batch) dropped — unnecessary once B exists.

Oracle has dropped twice in two days (2026-07-23 and 24) — check for a maintenance-window
pattern with the QAB owner before scheduling long runs.

## The closed-loop workflow (the thing to remember)

```
validate_cdb_simplificado.py --emit-faltantes X   -> orphan keys (TABELA/COLUNA/VALOR)
engorda_instrumentos.py --faltantes-parquet X     -> regenerate, pruning affected IFs
validate again                                     -> clean, or fresh faltantes
```

## Standard run configurations (Data Flow)

Bucket/namespace: `oci-st-blc-engordai-qab-n@gr97zovfhcmu`. Reports under
`reports/cdb-shapes/`.

**Validator app** (has ojdbc jar + private endpoint + `DATAGEN_SOURCE_*` env;
`DATAGEN_SYNTHETIC_PREFIX=clones_instrumentos`):

```
--shape-baseline oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/reports/cdb-shapes/profile_raw_clone_domain_v2.json
--report-path    oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/reports/cdb-shapes/validate_clones_instrumentos.json
--emit-faltantes oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/reports/cdb-shapes/faltantes_qab.parquet
--validate-against union
--sample-size 20
```

`--no-oracle` runs Cats 1/2/5/7 only (shape gate works offline; Cats 3/4/6 skipped).

**Profiler app** (no JDBC, no private endpoint). Baseline for a clone run = profile raw
restricted to exactly the sampled sources:

```
--base-uri oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/onprem-export-full
--universe-keys <clones-path>/MAPA_CLONE_NUM_IF --universe-keys-column NUM_IF_ORIG
--label raw_clone_domain --report-path <...>.json --sample-size 20
```

Other profiler modes: `--universe domain` (team FILTRO_BASE, 17,949,933 IFs),
`--apply-filtros-fonte` (row-level engorda-input image), `--self-test` (no data needed).
NEVER `--compare-with` across metric generations (13-metric vs 18-metric baselines have
disjoint signatures).

## Findings ledger (chronological, all resolved or pending)

| Finding | Status | Fix |
|---|---|---|
| Old engorda: 0/100k valid shapes (EVENTO smeared, subtype mix inverted, 1:2:1 destroyed) | CLOSED | Replaced by cluster cloning (`tests/engorda_instrumentos.py`) — `docs/cdb-shapes-findings.md` |
| 314 dangling CONDICAO_IF (clone set lacked AMORTIZACAO/PARTICIPACAO_LUCROS/RESET/DESDOBRAMENTO) | CLOSED (verified 2026-07-24 no-oracle run) | Subtype tables added to clone set |
| 12 OPERACAO → TRANSFERENCIA_ARQUIVO orphans | CLOSED (verified 2026-07-24) | Columns nulled (nullable) |
| 26 NUM_IF_ORIGEM "orphans" | CLOSED — validator false positive | Cat 3 inverted (commit 6e59dda) |
| COMITENTE/CONTA ids absent from QAB (source/target drift) | **OPEN — reactive loop non-convergent** (708 → 646 → 481 keys across three runs, fresh keys each time; per-run pruning + resampling = whack-a-mole, see "Where the loop stands") | Freeze the batch (`--num-ifs` file variant) or enumerate the full drift set once; accumulate faltantes either way |

## Key invariants & numbers (production evidence, 67.2M active CDBs)

- Reference registration write-set (`docs/cetip.out`, matches `docs/query.sql`):
  `1 IF : 1 TITULO : 1 CREDITO : 2 CONDICAO_IF (1 RESGATE + 1 JUROS_FLUTUANTE) :
  2 EVENTO (1×tipo83 + 1×tipo85) : 1 OPERACAO : 2 DADO_OPERACAO : 1 LANCAMENTO :
  1 DEPOSITO_AUTOMATICO_IF : 1 CARTEIRA_COMITENTE : 1 CARTEIRA_PARTICIPANTE`.
- Hard invariants: `DADO_OPERACAO = 2×OPERACAO`, `LANCAMENTO = OPERACAO` (~99%, Cat 7c
  tolerance 5%); `RESGATE ≤ 1` per IF (0 exceptions in 67.2M — Cat 7d); every domain IF
  has evento tipo 85, ~96% also tipo 83; `QJFL == QC03` and `QJFI == QC02` exactly
  (polymorphism holds 100% in prod).
- Team domain (FILTRO_BASE): IF-level exists(active SEM TABELA resgate) + non-escalonado
  titulo = **17,949,933 IFs** (profiler `--universe domain` reproduces it; note their
  EVENTO CTE does NOT filter DAT_EXCLUSAO, profiler counts active-only).
- Filter audits (unfiltered 67.2M): SEM TABELA = 54% of resgates; escalonamento drops
  5.2M titles; 17% of universe held by NON-simplificado comitentes (no flag filter exists).

## Tooling map (all on main)

- `scripts/profile_cdb_shapes.py` — shape profiler, 18 metrics, self-contained,
  `--self-test`. Key commits: 0e203f3, cc2d3fd (`--apply-filtros-fonte`), 242ddf7
  (subtypes + EVENTO_TIPO83/85 + `--universe domain` + `--universe-keys`).
- `scripts/validate_cdb_simplificado.py` — 7-category gate. Cat 7 = shape conformance
  (7a unseen vs baseline, 7b TVD ≤0.15, 7c op ratio, 7d resgate multiplicity; metric list
  parsed FROM the baseline JSON). Cat 3 inverted: anti-join synthetic then residual
  IN-lists into Oracle (batch 1000, `--max-residual-keys` default 1M) — no parent
  downloads; Oracle outage mid-run degrades to per-FK WARNs, never aborts.
  `--emit-faltantes` writes union-verified orphans only. `--report-path`/baselines accept
  oci:// URIs. Tables cached after read. Commits: f36edc3, 6e59dda, c4fe24a, PR #2
  (be225f5: 75d9407 + 018810f).
- `scripts/enumerate_faltantes.py` — one-time full drift enumeration (see "Where the
  loop stands"); self-contained, `--self-test`, runs on the validator Data Flow app.
- `tests/engorda_instrumentos.py` (now Mateus's too — no teammate, cluster cloning) — samples FILTRO_BASE
  domain, clones whole clusters, writes `MAPA_CLONE_NUM_IF` (NUM_IF_ORIG, K, NUM_IF_NOVO);
  prunes domain via `--faltantes-arg`/`--faltantes-parquet` (TABELA/COLUNA/VALOR; tables
  without NUM_IF are skipped with a warning but covered transitively); nulls
  NUM_ID_TRANSF_ARQ_P1/P2.
- Docs: `docs/cdb-shapes-findings.md` (synthetic vs prod analysis),
  `docs/cdb-simplificado-filtering-proposal.md` (IF-level filtering proposal),
  `docs/cetip.out` + `docs/query.sql` (evidence base).
- **Uncommitted, deliberately**: `docs/plans/2026-07-19-engorda-clusters-design.md`
  (full grill-me design for a from-scratch `engorda_clusters.py`: offset-cloning,
  ownership boundary from specs static flags, per-cluster date rebase + 90d spread,
  seeded bootstrap + [0.5,2] per-cluster scale factor, JSON product configs; phase 2 =
  comitente clusters, blocked on PESSOA_FISICA/CONTA_COMITENTE extraction; PARTICIPANTE
  never cloned). Superseded in practice by the teammate's engorda_instrumentos.py for
  the current cycle, but is the reference design for productizing/expanding to other IFs.

## Gotchas that cost time before (don't relearn them)

- **THE BIG ONE (found 2026-07-25 night, commit 1947be1): Spark 3.5.0 on Data Flow +
  AQE + cached DataFrames silently LOSES JOIN ROWS (SPARK-45282, fixed 3.5.1).**
  Proven via enumerate_faltantes --explain-keys: the 67M-row universe semi-join
  returned 0 for NUM_IFs a direct filter on the SAME cached DF found, all predicates
  True. Small broadcast joins unaffected; big sort-merge joins over cache lose the
  newest-id slice. This produced the "phantom drift keys" (627/541 fresh orphans per
  gate despite complete enumeration). ALL four jobs now force
  spark.sql.adaptive.enabled=false after session creation; every validation/baseline
  produced BEFORE this fix is suspect and must be re-run. Long-term: move the Data
  Flow apps to Spark >= 3.5.1 and the workaround can go.

- QAB Oracle drops intermittently (listener down / connection refused) — two outages in
  two days; a mid-run outage produces a misleading `0 ERROR ... OK` with WARN-degraded
  Cat 3.
- The 23 IF-cluster tables have ZERO unique constraints/indexes beyond PKs (verified in
  QAB) — offset keys are collision-free; business codes duplicate across clones (accepted).
- pluginless facts: `IND_COMITENTE_SIMPLIFICADO` is a physical COMITENTE column;
  EVENTO carries BOTH NUM_IF and NUM_CONDICAO_IF FKs.
- Local Spark on this Mac: `JAVA_HOME=$(/usr/libexec/java_home -v 11)` +
  `PYSPARK_PYTHON=$PWD/.venv/bin/python` (system Java 25 / Python 3.14 break it).
- Re-ingesting raw from Oracle: decided AGAINST for now — resets the drift clock without
  removing the mechanism, kills all baselines and clone runs, needs hours of Oracle
  uptime. Reconsider only at a natural refresh cycle, ideally extracting FROM QAB (the
  load target) so raw is self-consistent by construction.

## After the gate passes

1. Load: `python -m datagen.load_tables` semantics (append, PK guard, rollback manifest;
   `rollback_load.py` deletes above per-table max — clone BASE keys are in the 6.0e9 range).
2. NoMe daily/operational batch on top of QAB — the true acceptance (watch for the
   original failure classes: ClassCast, ORA-01400 COD_MOTIVO, SEM MODALIDADE — Cat 6
   combo check is only best-effort).
3. Follow-ups parked: filter-scope questions to the business (COM TABELA resgates,
   escalonados, missing simplificado filter — see filtering proposal); Cat 6 needs the
   real tipo_operacao×modalidade×serviço mapping table added to COMBO_TABLE_PATTERNS;
   phase-2 comitente cloning per the design doc.
