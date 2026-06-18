# Repo Clean-up — `datagen/` package + cruft/dead-file purge (Design)

Date: 2026-06-18
Status: design / approved
Related: `etl.py` (discontinued), `transform/`, `save_tables.py`,
`engorda_tables.py`, `load_tables.py`, `validate_tables.py`, `scripts/`, `tests/`

## Problem

The repo root has accumulated three kinds of clutter:

1. **A flat root** holding ~8 Python modules mixing the live pipeline, a
   discontinued monolith, and one-off dev scripts.
2. **Binary/visual cruft** sitting in the working tree (screenshots, a notebook,
   a 118 MB `archive.zip`) — all untracked/gitignored, so noise but not in
   history.
3. **Dead code** left over from an abandoned pipeline.

The live pipeline is now **`save_tables` → `engorda_tables` → `load_tables`**
(with `validate_tables` as an offline validator). `etl.py` — the hand-inlined
OCI Data Flow monolith — and `transform/` (the old v3 synthesizer + its
LLM-driven `spec_build.py`) are being discontinued. `engorda_tables.py`
superseded the `transform.py` synthesis logic; the live `specs.json` is built by
`scripts/build_specs_from_constraints.py`, **not** `transform/spec_build.py`.

## Key facts established during analysis

- `etl.py` is self-contained (line ~255: *"Inlined from transform.py so OCI Data
  Flow only needs etl.py."*). The live modules do **not** import each other —
  they are standalone. So moving them causes no import cascade; only `scripts/`
  and `tests/` importers shift.
- `transform/` (`transform.py`, `spec_build.py`) is referenced **only** by
  `etl.py`. Untracked. `spec_build.py` builds an in-memory `specs_config` for the
  old v3 synthesizer, not `specs.json`.
- `oracle_read_smoke.py` — orphaned dev smoke test; 0 README mentions; nothing
  imports it.
- `secrets.py` — imported nowhere; relies on `oci`, whose dependency is already
  being dropped in the uncommitted `pyproject.toml`/`requirements.txt` edit.
- None of the binary cruft is git-tracked; the 5 `.jar` drivers are **required**
  runtime files per README and stay.

## Scope

### 1. Reorganize layout → `datagen/` package

Create `datagen/` with `datagen/__init__.py` and move the four live modules in:

| From (root) | To |
|---|---|
| `save_tables.py` | `datagen/save_tables.py` |
| `engorda_tables.py` | `datagen/engorda_tables.py` |
| `load_tables.py` | `datagen/load_tables.py` |
| `validate_tables.py` | `datagen/validate_tables.py` |

Use `git mv` to preserve history. Entrypoints are invoked as
`python -m datagen.<module>` (each module keeps its `__main__`).

Follow-on edits:

- **scripts** → **no import changes needed.** Verified: `scripts/*.py` mention
  `validate_tables`/`load_tables` only in docstrings/comments, not imports.
- **tests** → only **4 files** import the moved modules; update each:
  - `tests/test_save_tables.py:3` `import save_tables` → `from datagen import save_tables`
  - `tests/test_engorda_tables.py:6` `import engorda_tables` → `from datagen import engorda_tables`
  - `tests/test_load_tables.py:6` `import load_tables` → `from datagen import load_tables`
  - `tests/test_validate_tables.py:7` `import validate_tables as vt` → `from datagen import validate_tables as vt`
- **README** → replace all `python <module>.py …` usages (≈9 occurrences for
  save/load/validate) with `python -m datagen.<module> …`; remove the `etl.py`
  usage section (lines ~14-17) and the OCI Vault / `secrets.py` line (~line 9).
- **pyproject.toml** → declare the `datagen` package so it's importable/installable.

### 1a. Repoint the deploy workflow

`.github/workflows/deploy-eudalia-datagen-scripts-to-s3.yml` currently triggers on
push to `etl.py` and runs `aws s3 cp etl.py s3://…/scripts/etl.py`. Repoint it to
the new package:

- Trigger `paths:` → `datagen/**` (instead of `etl.py`).
- Upload step → `aws s3 sync datagen/ s3://${S3_BUCKET}/scripts/datagen/`
  (replacing the single `etl.py` copy).
- Update the workflow `name`/echo text away from "ETL Script".

### 2. Remove the discontinued pipeline

- `git rm etl.py`
- `rm -r transform/` (untracked: `transform.py`, `spec_build.py`)

### 3. Remove dead files

- `git rm oracle_read_smoke.py`
- `git rm secrets.py` (and drop README's OCI Vault secrets reference)

### 4. Purge cruft from the working tree (untracked/gitignored — plain `rm`)

Remove: `v1-initial.png`, `v2-100.png`, `v2-fit.png`, `v2-zoom.png`, `v2.png`,
`v3.png`, `v4.png`, `datagen_arch.jpeg`, `SintetizacaoAnonima.ipynb`,
`archive.zip`.

**Keep**: the 5 `.jar` drivers (`ojdbc8.jar`, `oraclepki.jar`, `osdt_cert.jar`,
`osdt_core.jar`, `ucp.jar`) and `version.txt`.

## Out of scope (intentionally untouched)

- Committing the 5 untracked `docs/plans/*.md` files.
- `.playwright-mcp/` directory.
- Doc-location convention (`docs/plans/` stays as-is).
- The uncommitted `pyproject`/`requirements` `oci`-removal edit beyond what the
  `secrets.py`/package changes require for consistency.

## Verification

- `pytest` passes after the moves.
- `python -m datagen.save_tables --help`, `… engorda_tables --help`,
  `… load_tables --help`, `… validate_tables --help` all run.
- `git grep -n "import etl\|from etl\|oracle_read_smoke\|^import secrets\|transform\."`
  returns nothing in live code.
- Root no longer lists the removed binaries.
- The deploy workflow references `datagen/`, not `etl.py`
  (`grep -n etl .github/workflows/*.yml` is empty).

## Risks

- A module run directly (`python datagen/x.py`) still works since modules have no
  sibling imports; `python -m datagen.x` is the documented form.
- Missing an importer reference → caught by `pytest` + the `git grep` check.
