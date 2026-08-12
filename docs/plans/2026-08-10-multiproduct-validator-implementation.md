# Multi-Product Synthetic Validator Implementation Plan

> **For agentic workers:** implement task-by-task with tests first. Do not enable a product-specific check unless its profile contains evidence-backed values; unresolved rules must emit an explicit unsupported/WARN finding, never inherit CDB defaults silently.

**Goal:** Convert `scripts/validate_products.py` and `scripts/profile_cdb_shapes.py` into reliable validation tooling for `cdb_simplificado`, full `cdb`, and `rdb`, while preserving the self-contained single-file OCI Data Flow deployment model.

**VDI constraints:** The implementation environment has no Git repository or Git executable. Do not run or request `git status`, `git diff`, commits, branches, tags, pushes, or pull requests. Edit and test the copied files directly. Track touched paths manually.

**Required final output:** Return only a delivery manifest listing changed and new files. Do not include commit instructions, diffs, summaries, or unchanged files:

```text
Changed files:
- scripts/example.py

New files:
- tests/test_example.py
```

**Architecture:** Keep a small `ValidationProfile` interface inside the validator and thread the selected profile only through product-sensitive checks. Generic Oracle/schema, polymorphism, FK, capacity, NOT NULL, and date implementations stay behind their current interfaces. Product identity is validated before semantic checks, domain membership is evaluated per `NUM_IF` with `EXISTS` semantics, and reports identify unsupported coverage instead of representing skipped checks as passes.

**Safety model:**

- `cdb_simplificado`: strict mode after parity with the current known-good gate.
- `cdb`: strict structural and domain mode, without simplificado-only row, shape, type-mix, or registration assumptions.
- `rdb`: strict structural mode initially. Strict lookup, shape, and registration mode remains blocked until the target evidence in Task 8 is collected.
- A missing `--product`, empty selected-product root universe, mixed/wrong root types, or incompatible shape baseline is an error, not a skip.

**Estimated effort:** 24-34 engineering hours, plus OCI/QAB runtime for canaries and the NoMe acceptance batch.

## Non-Goals

- Preserving the legacy validator filename or adding a compatibility wrapper; callers must migrate to the generic filename.
- Importing `datagen/engorda_tables.py` or another project module from the validator/profiler; both jobs remain independently deployable single files.
- Inventing RDB values for `COD_IF` format, platform code, modalidade, account, registration constants, or shape/type mixes.
- Treating empirical shape distributions as application invariants.

## File Structure

- **Modify:** `scripts/validate_products.py` - product profiles, input resolution, identity preflight, IF-level domain, product-aware Categories 6-8, PK/map checks, report v2.
- **Modify:** `scripts/profile_cdb_shapes.py` - explicit product selection, profile-aware universe, baseline schema v2, source-map provenance.
- **Modify:** `tests/test_validate_cdb_lookup_combos.py` - CDB/RDB profile-aware lookup fixtures.
- **Modify:** `tests/test_validate_cdb_log_invariants.py` - product-aware Cat 8 fixtures.
- **Create:** `tests/test_validate_products.py` - profiles, paths, identity, domain, PK/map, and report tests.
- **Create:** `tests/test_validate_product_shapes.py` - profile-aware shape and baseline compatibility tests.
- **Modify:** operational docs that show validator/profiler commands after the interfaces stabilize.

## Verification Commands

Use the repository's PySpark environment and Java 11:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 11) \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
  "$PWD/.venv/bin/python" -m pytest \
  tests/test_validate_products.py \
  tests/test_validate_product_shapes.py \
  tests/test_validate_cdb_lookup_combos.py \
  tests/test_validate_cdb_log_invariants.py \
  tests/test_validate_cdb_capacity.py -q
```

If the project virtualenv is elsewhere, preserve Java 11 and run the same test modules with that interpreter.

---

## Task 1: Freeze Existing Behavior and Add the Product Interface

**Files:**

- Modify: `scripts/validate_products.py`
- Create: `tests/test_validate_products.py`

**Estimate:** 3-4 hours.

- [ ] Add characterization tests for current CDB-simplificado profile constants, current report exit policy, and the known generic checks.
- [ ] Add a frozen `ValidationProfile` with explicit fields; no profile inheritance or `deepcopy`:

```python
@dataclass(frozen=True)
class ValidationProfile:
    name: str
    num_tipo_if: int
    default_clone_prefix: str
    simplified_domain: bool
    object_service_id: int
    object_service_code: Optional[str]
    cod_if_pattern: Optional[str]
    sic_enabled: bool
    platform_check_enabled: bool
    account_check_enabled: bool
    sem_modalidade_ids: Optional[Tuple[int, ...]]
    hard_shape_rules: Tuple[str, ...]
    registration_constants: Optional[Dict[str, Dict[str, object]]]
    required_capabilities: Tuple[str, ...]
    supported_capabilities: Tuple[str, ...]
    evidence_version: int
```

- [ ] Define `VALIDATION_PROFILES` for `cdb_simplificado`, `cdb`, and `rdb` from the evidence report.
- [ ] Set proven RDB values only: `num_tipo_if=50`, `object_service_id=45`, output prefix. Leave unresolved fields `None`/disabled; do not copy CDB values.
- [ ] Define a capability ledger (`identity`, `domain`, `platform`, `account`, `modalidade`, `cod_if_format`, `shape`, `registration_profile`, etc.). Every profile declares what strict semantic validation requires and what current evidence supports.
- [ ] Generate one unsupported finding for every `required_capabilities - supported_capabilities`; this set mechanically forces `PARTIAL` and cannot disappear because a check function was disabled.
- [ ] Add required `--product {cdb_simplificado,cdb,rdb}` and select the profile before creating Spark or reading data.
- [ ] Add `--input-base`; resolve input in this order: explicit `--input-base`, `DATAGEN_SYNTHETIC_BASE_URI + DATAGEN_CLONE_PREFIX`, then base + profile default prefix.
- [ ] Remove `DATAGEN_SYNTHETIC_PREFIX` from the new interface. If temporary migration support is required for the deployed CDB app, accept it only for `cdb_simplificado`, emit a deprecation warning, and delete it after the Data Flow arguments are updated.
- [ ] Add `check_product_identity(tables, profile, sample)` before Category 1. It must error on missing root table/columns, no active roots, wrong root type, or mixed product types.
- [ ] Preserve `--tables` only as a diagnostic mode. Any explicit table subset is recorded as partial coverage and can never produce strict `PASS`.
- [ ] Thread `profile` only into product-sensitive functions; generic check signatures remain unchanged.

**Acceptance:**

- Missing `--product` exits before Spark starts.
- An RDB dataset passed as `--product cdb` fails identity rather than producing an empty-universe pass.
- Every profile field is explicit and unit-tested; unresolved RDB values are visible as unresolved, not inherited.
- RDB's unresolved capability set is asserted exactly in tests and forces `PARTIAL` even when every executed check passes.
- Existing CDB-simplificado generic checks produce unchanged findings on frozen fixtures.

---

## Task 2: Replace Row-Level Domain Rules with IF-Level Eligibility

**Files:**

- Modify: `scripts/validate_products.py`
- Test: `tests/test_validate_products.py`

**Estimate:** 3-4 hours.

- [ ] Delete global `DOMAIN_RULES` and introduce `build_eligible_num_ifs(tables, profile)`.
- [ ] Build the eligible root set using left-semi joins/`EXISTS` semantics matching the product SQL:
  - active root of the profile's `NUM_TIPO_IF`;
  - a `TITULO` row;
  - an active `CONDICAO_IF` joined to an active `RESGATE` row;
  - an active non-resgate `CONDICAO_IF` (`COD_TIPO_CONDICAO_IF <> 20`);
  - a `DEPOSITO_AUTOMATICO_IF` row;
  - an operation cluster containing `OPERACAO`, `DADO_OPERACAO`, and `LANCAMENTO`;
  - a specification cluster containing `ESPECIFICACAO` and `ESPECIFICACAO_COMITENTE`.
- [ ] For `cdb_simplificado`, additionally require an eligible non-escalonado title and an active `RESGATE='SEM TABELA'` through `CONDICAO_IF`.
- [ ] For full CDB and RDB, still require an active resgate path but allow any escalation and `COD_COND_RESGATE` value.
- [ ] Compare every active output root to the eligible set. Do not require every copied `TITULO`, `RESGATE`, or historical closure row to match the qualifying predicate.
- [ ] Emit separate findings for missing required tables/columns versus ineligible instruments.
- [ ] Keep `CARTEIRA_* QTD > 0` out of strict shared rules until primary evidence establishes whether it is product-wide; represent it as an optional profile rule if retained.

**Tests:**

- [ ] Simplificado accepts an IF with one qualifying resgate plus an additional nonmatching closure row.
- [ ] Simplificado rejects an IF with no qualifying `SEM TABELA`/non-escalonado path.
- [ ] Full CDB and RDB accept escalonamento and non-`SEM TABELA` values.
- [ ] Full CDB and RDB reject an IF with no active resgate path.
- [ ] All products reject missing non-resgate condition, deposit, operation cluster, or specification cluster.
- [ ] A wrong-type root never disappears from the check universe.

**Acceptance:** Category 2 models selection eligibility per instrument and cannot reject valid complete closure rows.

---

## Task 3: Complete the Universal Structural Gate

**Files:**

- Modify: `scripts/validate_products.py`
- Test: `tests/test_validate_products.py`

**Estimate:** 3-4 hours.

- [ ] Verify every `TipoCondicaoIFDO` code against `CondicaoIFDO.hbm.xml` in the application repository and add only proven type-to-physical-table mappings to `SUBTYPE_BY_TIPO`; record the application `file:line` evidence beside each non-obvious mapping.
- [ ] Add an `EXPECTED_CONDICAO_TYPE_CODES` inventory from `TipoCondicaoIFDO.java` and a test requiring every expected code to have a reviewed physical mapping (including trigger types such as `TRIGGER_SPR`) before full-product rollout.
- [ ] Make subtype auditing bidirectional: every active condition has exactly one matching subtype; every subtype row has a parent; every observed type is configured; every configured table present in output is checked.
- [ ] Add `check_primary_keys(tables, meta, sample)` for every synthetic Oracle table: required PK columns exist, values are non-null, and the complete PK tuple is unique.
- [ ] In strict mode, require auto-discovery of the complete output inventory. If `--tables` restricts input, mark PK/FK/table coverage partial before checks execute.
- [ ] Treat missing PK metadata or missing PK columns as ERROR when Oracle metadata is enabled. Under `--no-oracle`, emit one explicit unsupported finding instead of silently skipping PK validation.
- [ ] Recognize `MAPA_CLONE_NUM_IF` as an artifact rather than an Oracle table and validate:
  - `(NUM_IF_ORIG, K)` uniqueness;
  - `NUM_IF_NOVO` uniqueness;
  - one map row for every synthetic root and no map row pointing outside the root output.
- [ ] Ensure generic FK, NOT NULL, capacity, date, and polymorphism checks receive no product-specific branching.

**Acceptance:** every product must pass identity, PK, map, subtype, FK, NOT NULL, capacity, and date checks before semantic validation can be called strict.

---

## Task 4: Parameterize Category 6 Lookup Validation

**Files:**

- Modify: `scripts/validate_products.py`
- Modify: `tests/test_validate_cdb_lookup_combos.py`

**Estimate:** 3-4 hours.

- [ ] Pass `profile` to `check_lookup_combos`, `check_lookup_combo_frames`, and `check_required_lookup_frames`.
- [ ] Replace hard-coded type `49` and object service `44` in SIC/TOS queries and DataFrame checks with profile fields.
- [ ] Replace CDB-specific finding messages and identifiers where needed with product-neutral language; preserve stable check IDs when semantics have not changed.
- [ ] Execute object-service platform, account, modalidade, and operation-type checks only when the profile contains evidence-backed values.
- [ ] When a selected product lacks a required value, emit an explicit `unsupported`/WARN finding naming the missing evidence. Never PASS or use a CDB value.
- [ ] Add RDB fixtures proving service `45` passes the structural TOS/SIC check and service `44` fails.
- [ ] Keep RDB platform code, identification/operation-type literals, account literals, and modalidade IDs unresolved until Task 8 evidence closes them.

**Acceptance:** Cat 6 never queries or validates RDB as `(NUM_TIPO_IF=49, object_service=44)`, and unresolved RDB semantics are visible in the report.

---

## Task 5: Generalize Shape Profiling and Introduce Baseline Schema v2

**Files:**

- Modify: `scripts/profile_cdb_shapes.py`
- Modify: `scripts/validate_products.py`
- Create: `tests/test_validate_product_shapes.py`

**Estimate:** 4-6 hours.

- [ ] Add required `--product` and explicit local profiles to the profiler.
- [ ] Parameterize the root type and product domain; remove hard-coded `CDB_TIPO_IF=49` from universe construction.
- [ ] Emit baseline schema v2 with:

```json
{
  "schema_version": 2,
  "product": "cdb",
  "num_tipo_if": 49,
  "domain_version": 1,
  "metric_version": 2,
  "metrics": [],
  "source_key_count": 0,
  "source_key_fingerprint": "...",
  "map_mode": "exact-source-keys",
  "spark_version": "...",
  "aqe_enabled": false,
  "shapes": []
}
```

- [ ] Compute source-key provenance when `--universe-keys` points to `MAPA_CLONE_NUM_IF`; fingerprint distinct original keys deterministically.
- [ ] Make the validator compare baseline product, type, domain version, metric version, map mode, key count, and fingerprint before shape actions.
- [ ] Reject incompatible/cross-product baselines as ERROR. Do not parse metrics from an untrusted first shape signature.
- [ ] Permit legacy untagged baselines only for a temporary explicit `cdb_simplificado` compatibility mode, with WARN and no strict full-CDB/RDB status.
- [ ] Move hard shape rules into profile policy:
  - simplificado: retain current empirical rules at their current severities;
  - full CDB: do not enforce resgate maximum, exact condition/event mixes, or simplificado registration shape;
  - RDB: no strict shape/type-mix rules until a type-50 baseline is produced and reviewed.
- [ ] Keep aggregate distribution comparison secondary to map/source provenance; skipped metrics or incomplete map coverage prevent strict status.

**Acceptance:** an RDB baseline cannot be generated or consumed with type 49, and a simplificado baseline cannot validate full CDB/RDB.

---

## Task 6: Parameterize Category 8 Business-Key and Registration Checks

**Files:**

- Modify: `scripts/validate_products.py`
- Modify: `tests/test_validate_cdb_log_invariants.py`

**Estimate:** 2-3 hours.

- [ ] Replace `_cat8_active_cdb` with `_active_product_roots(tables, profile, required)`.
- [ ] Keep proven generic checks enabled for all products: COD_IF uniqueness, root/operation COD_IF equality, COD_OPERACAO uniqueness, and meu-numero tuple uniqueness.
- [ ] Read COD_IF regex from the profile. If absent and registration-profile validation is requested, emit unsupported/WARN rather than accepting the generator's assumed RDB regex.
- [ ] Keep `REGISTRATION_CONSTANTS`, condition type mix, and event type mix enabled only for `cdb_simplificado`.
- [ ] Keep COD_OPERACAO format and DADO_OPERACAO `(502,503)` as empirical checks; use WARN unless product-specific evidence promotes them.
- [ ] Include the selected product in all messages and fixtures.

**Acceptance:** Cat 8 sees active type-50 RDB roots, never returns a vacuous green from an empty type-49 filter, and does not enforce CDB registration constants on RDB/full CDB.

---

## Task 7: Report Coverage Honestly and Wire the End-to-End CLI

**Files:**

- Modify: `scripts/validate_products.py`
- Test: `tests/test_validate_products.py`

**Estimate:** 2-3 hours.

- [ ] Add report schema v2 with selected product, type, profile/evidence version, resolved input path, discovered table inventory, baseline identity, and runtime Spark/AQE information.
- [ ] Distinguish `PASS`, `FAIL`, and `PARTIAL`:
  - `FAIL`: any finding at or above `--fail-severity` or any non-skippable identity/contract error;
  - `PARTIAL`: no failing finding, but one or more required checks are unsupported/unresolved;
  - `PASS`: all checks required by the selected strict profile executed and passed.
- [ ] Compute required coverage from the profile capability ledger, not from whichever check functions happened to run.
- [ ] Do not serialize skipped/unsupported checks as `passed: true`.
- [ ] Change `--skip-check` planning so skipped prefixes are excluded before expensive Spark actions and recorded in report coverage. A strict run with manually skipped required checks is `PARTIAL`.
- [ ] Print a product-neutral report heading and concise coverage summary.
- [ ] Add an orchestration test that runs all check groups against one tiny positive fixture per product and verifies exit status/report metadata.
- [ ] Keep AQE disabled on Spark 3.5.0 as currently documented.

**Acceptance:** no RDB run can print `OK` merely because type-49 universes are empty or unsupported checks were filtered from the findings list.

---

## Task 8: Close RDB Evidence Before Strict Semantic Promotion

**Files:**

- No validator code until evidence is captured.
- Update the profile/tests after each value is proven.
- Save query outputs or a research note under `docs/` without credentials or sensitive row data.

**Estimate:** 2-4 hours of engineering work plus target-query/runtime availability.

- [ ] Query `V_OBJETOS_SERVICO` for object service `45` to prove the RDB service code and `IND_PLATAFORMA_BAIXA`.
- [ ] Query `V_PARAMETRO_SIC` for `NUM_TIPO_IF=50` and prove the object-service/TOS mappings.
- [ ] Query `TIPO_OPER_OBJETO_SERV` joined with `TIPO_OPERACAO` for service `45` to prove identification and operation-type requirements.
- [ ] Capture a real RDB registration or allocator output to prove the complete COD_IF format. Do not promote `^RDB...$` from assumption before this.
- [ ] Confirm account eligibility and sem-modalidade behavior for RDB through application control flow, target configuration, or an SME decision.
- [ ] Produce and review an exact-source-key type-50 baseline before enabling any RDB shape/type-mix rule.
- [ ] Record every promoted value with source, date, environment, and confidence in the profile comment/tests.

**Acceptance:** RDB becomes strict-semantic only when no required profile field remains unresolved and its canary has no unsupported findings.

---

## Task 9: Data Flow and QAB Rollout

**Files:**

- Modify relevant command examples in `docs/cdb-simplificado-ingestion-analysis.md`, `docs/cdb-simplificado-filtering-proposal.md`, and `docs/plans/2026-07-25-cdb-validation-handoff.md` only where they describe current operational commands.

**Estimate:** 2 hours of engineering work plus job runtime.

- [ ] Deploy validator and profiler together as self-contained single files; product domain builders remain embedded and require no auxiliary SQL/config packaging.
- [ ] Update every Data Flow invocation to pass explicit `--product` and the correct clone path/prefix.
- [ ] Run old/new CDB-simplificado validators in shadow against the same frozen known-good batch. Explain every finding or exit-status difference.
- [ ] Run 100-instrument then 10,000-instrument isolated-prefix canaries for full CDB. Require strict structural/domain PASS and a compatible v2 baseline.
- [ ] Run the same canaries for RDB in PARTIAL mode until Task 8 closes; require identity type 50, service 45 structural checks, and universal structural PASS.
- [ ] After semantic promotion, load an isolated RDB canary into QAB with a rollback manifest and run the NoMe operational batch.
- [ ] Treat ClassCast, ORA-01400/01438/12899, service/modalidade errors, or missing batch records as release failures even when the offline report passes.

**Acceptance Gates:**

| Gate | Requirement |
|---|---|
| G0 | Unit/integration suites cover profiles, path resolution, identity, IF-level domain, all-table PKs, map coverage, subtypes, lookups, baselines, and Cat 8. |
| G1 | New CDB-simplificado gate matches the old gate on a frozen batch, except reviewed fixes to row-level domain and coverage reporting. |
| G2 | Full CDB passes without simplificado-only domain/shape/registration findings. |
| G3 | RDB has zero unsupported required checks and passes type-50/service-45 target checks. |
| G4 | QAB append and NoMe batch complete without the known failure classes. |

## Delivery Tracking

- [ ] Maintain a manual list of every changed or newly created path while implementing.
- [ ] Do not depend on Git to discover touched files.
- [ ] At completion, verify each listed path exists and classify it as changed or new relative to the copied input files.
- [ ] Return only the changed/new file manifest in the exact format defined at the top of this plan.

## Definition of Done

- All three products require explicit selection and resolve the intended output tree.
- Generic structural validation is strict for all products.
- Full CDB is never subjected to simplificado-only row/shape/registration rules.
- RDB uses type 50 and service 45 everywhere those concepts apply.
- Unknown RDB rules are visible and block `PASS`; no CDB assumption fills them.
- Baselines are versioned, product-bound, and source-map-bound.
- CDB simplificado parity, full-CDB canaries, RDB evidence gates, QAB load, and NoMe batch acceptance are documented with immutable reports.
