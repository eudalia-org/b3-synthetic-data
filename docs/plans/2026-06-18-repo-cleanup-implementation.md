# Repo Clean-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the live pipeline modules into a `datagen/` package, delete the discontinued `etl.py`/`transform/` path and dead files, repoint the deploy workflow, and purge binary/visual cruft from the working tree.

**Architecture:** The live pipeline is `save_tables → engorda_tables → load_tables` (+ `validate_tables`). These four modules are standalone (no inter-module imports), so they move into `datagen/` with only test-import and pyproject changes. `etl.py` (discontinued OCI monolith) and `transform/` (its only consumer) are removed. Cruft is untracked/gitignored, so purging is a plain `rm`.

**Tech Stack:** Python 3.11, PySpark, uv, pytest, GitHub Actions.

**Design doc:** `docs/plans/2026-06-18-repo-cleanup-design.md`

---

### Task 1: Create `datagen/` package and move the four live modules

**Files:**
- Create: `datagen/__init__.py`
- Move (git mv): `save_tables.py`, `engorda_tables.py`, `load_tables.py`, `validate_tables.py` → `datagen/`
- Modify: `pyproject.toml`
- Modify tests: `tests/test_save_tables.py:3`, `tests/test_engorda_tables.py:6`, `tests/test_load_tables.py:6`, `tests/test_validate_tables.py:7`

- [ ] **Step 1: Create the package dir with an empty init**

```bash
mkdir -p datagen
: > datagen/__init__.py
```

- [ ] **Step 2: Move the four modules with history preserved**

```bash
git mv save_tables.py datagen/save_tables.py
git mv engorda_tables.py datagen/engorda_tables.py
git mv load_tables.py datagen/load_tables.py
git mv validate_tables.py datagen/validate_tables.py
```

- [ ] **Step 3: Update the four test imports**

Edit each test to import from the package:
- `tests/test_save_tables.py:3` → `from datagen import save_tables`
- `tests/test_engorda_tables.py:6` → `from datagen import engorda_tables`
- `tests/test_load_tables.py:6` → `from datagen import load_tables`
- `tests/test_validate_tables.py:7` → `from datagen import validate_tables as vt`

Check each test file for a `sys.path` insert near the import (the `# noqa: E402`
on the validate import hints at one). If a test prepends the repo root to
`sys.path`, leave it — running from root keeps `datagen` importable. If a test
prepends a path so the *bare* module is importable, the `from datagen import …`
form still works from root; no path edit needed.

- [ ] **Step 4: Make `datagen` importable in tests via pytest pythonpath**

This repo has no `[build-system]` table — it runs as an application with the repo
root on `sys.path` (that's why bare `import save_tables` works today, but only
when pytest happens to run from root). Make that explicit and robust so
`from datagen import …` resolves regardless of how pytest is invoked. Add to
`pyproject.toml`:

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
```

Do NOT add a `[tool.setuptools]` package table — there is no setuptools build
backend here, so it would be inert/misleading. The `__init__.py` + pythonpath is
all that's needed for both `from datagen import …` and `python -m datagen.<mod>`.

Note: `tests/test_validate_tables.py:6` has its own `sys.path.insert(0, <root>)`.
Leave it — it remains correct and harmless alongside the pythonpath setting.

- [ ] **Step 5: Run the full test suite — expect all green**

Run: `pytest -q`
Expected: PASS (same count as before the move). With `pythonpath = ["."]` set,
this works from any CWD; if you see `ModuleNotFoundError: datagen`, confirm the
pytest config landed in `pyproject.toml`.

- [ ] **Step 6: Smoke each entrypoint as a module**

Run: `for m in save_tables engorda_tables load_tables validate_tables; do python -m datagen.$m --help >/dev/null && echo "$m OK"; done`
Expected: four `… OK` lines (argparse `--help` exits 0).

- [ ] **Step 7: Commit**

```bash
git add datagen pyproject.toml tests/test_save_tables.py tests/test_engorda_tables.py tests/test_load_tables.py tests/test_validate_tables.py
git status --short   # confirm the four git mv renames are staged as renames
git commit -m "refactor: move live pipeline modules into datagen/ package

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Remove the discontinued pipeline and dead files

**Files:**
- Delete (tracked): `etl.py`, `oracle_read_smoke.py`, `secrets.py`
- Delete (untracked): `transform/transform.py`, `transform/spec_build.py`

- [ ] **Step 1: Confirm nothing live imports the doomed modules**

Run: `git grep -nE "(^|[^.])import (etl|secrets|oracle_read_smoke)\b|from (etl|transform|secrets) import|import (transform|spec_build)\b" -- '*.py' ':!etl.py' ':!transform/'`
Expected: no output. (If anything prints, stop and resolve before deleting.)

- [ ] **Step 2: Remove tracked dead files**

```bash
git rm etl.py oracle_read_smoke.py secrets.py
```

- [ ] **Step 3: Remove the untracked transform/ dir**

```bash
rm -rf transform/
```

- [ ] **Step 4: Re-run tests to confirm nothing depended on them**

Run: `pytest -q`
Expected: PASS, unchanged count.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: remove discontinued etl.py + transform/ and dead secrets/smoke files

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Update README to the new layout

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Remove the etl.py usage section and OCI secrets line**

Delete the `## Usage` block that documents `python etl.py --config …` and the
sentence "The single `etl.py` entrypoint …" (README ~lines 11-17). Delete the
setup line "Configure OCI Vault secrets (see secrets.py …)" (~line 9).

- [ ] **Step 2: Remove the entire stale `## OCI Data Flow Deployment` section**

README ~lines 319-end document the discontinued path: `### Build Archive`,
`### Upload Archive` (`archive.zip`, purged in Task 5), `### Upload ETL Script`
(`etl.py`, deleted in Task 2), `### Run on Data Flow`. The live deploy is now AWS
S3 via the GitHub workflow (Task 4), so delete this whole section. If useful, add
a one-line pointer instead, e.g. *"Deployment: `datagen/` is synced to S3 by
`.github/workflows/deploy-eudalia-datagen-scripts-to-s3.yml` on push to `main`."*

- [ ] **Step 3: Rewrite remaining command examples to module form**

Replace every `python <module>.py …` for the moved modules with
`python -m datagen.<module> …` (verified counts: 3 save, 3 load, 3 validate;
`engorda_tables` has none):
- lines ~66, 72, 81: `python save_tables.py …` → `python -m datagen.save_tables …`
- lines ~147-149: `python load_tables.py …` → `python -m datagen.load_tables …`
- lines ~222-224: `python validate_tables.py …` → `python -m datagen.validate_tables …`

- [ ] **Step 4: Verify no stale references remain in README**

Run: `grep -nE "etl\.py|secrets\.py|archive\.zip|python (save_tables|load_tables|validate_tables|engorda_tables)\.py" README.md`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: update README for datagen/ package + drop etl/secrets

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Repoint the deploy workflow to `datagen/`

**Files:**
- Modify: `.github/workflows/deploy-eudalia-datagen-scripts-to-s3.yml`

- [ ] **Step 1: Update trigger, upload, and labels**

- `name:` → `Upload DataGen Pipeline to S3`
- `on.push.paths:` `- etl.py` → `- datagen/**`
- Upload step body: replace the two lines

```bash
          aws s3 cp etl.py "s3://${S3_BUCKET}/scripts/etl.py"
          echo "Uploaded etl.py to s3://${S3_BUCKET}/scripts/etl.py"
```

  with

```bash
          aws s3 sync datagen/ "s3://${S3_BUCKET}/scripts/datagen/" --delete --exclude "__pycache__/*"
          echo "Synced datagen/ to s3://${S3_BUCKET}/scripts/datagen/"
```

- Update the step `name:` from "Upload script to S3" if it references etl.

- [ ] **Step 2: Verify no etl reference remains in the workflow**

Run: `grep -n "etl" .github/workflows/deploy-eudalia-datagen-scripts-to-s3.yml`
Expected: no output.

- [ ] **Step 3: Lint the YAML is still valid**

Run: `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/deploy-eudalia-datagen-scripts-to-s3.yml')); print('yaml ok')"`
Expected: `yaml ok`

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/deploy-eudalia-datagen-scripts-to-s3.yml
git commit -m "ci: deploy datagen/ package to S3 instead of etl.py

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Purge binary/visual cruft from the working tree

**Files (all untracked/gitignored — no commit):**
- Delete: `v1-initial.png`, `v2-100.png`, `v2-fit.png`, `v2-zoom.png`, `v2.png`, `v3.png`, `v4.png`, `datagen_arch.jpeg`, `SintetizacaoAnonima.ipynb`, `archive.zip`

- [ ] **Step 1: Confirm none are tracked (safety)**

Run: `git ls-files -- '*.png' '*.jpeg' '*.ipynb' archive.zip`
Expected: no output (nothing tracked → safe to rm).

- [ ] **Step 2: Remove the cruft**

```bash
rm -f v1-initial.png v2-100.png v2-fit.png v2-zoom.png v2.png v3.png v4.png \
      datagen_arch.jpeg SintetizacaoAnonima.ipynb archive.zip
```

- [ ] **Step 3: Confirm the .jar drivers and version.txt are still present**

Run: `ls ojdbc8.jar oraclepki.jar osdt_cert.jar osdt_core.jar ucp.jar version.txt`
Expected: all six listed (these are intentionally kept).

---

### Task 6: Final verification sweep

- [ ] **Step 1: Full test suite green**

Run: `pytest -q`
Expected: PASS.

- [ ] **Step 2: No dangling references to removed code anywhere live**

Run: `git grep -nE "import etl|from etl|oracle_read_smoke|^import secrets\b|from transform|import transform\b|spec_build" -- '*.py' '*.yml' README.md`
Expected: no output.

- [ ] **Step 3: Entrypoints still run as modules**

Run: `for m in save_tables engorda_tables load_tables validate_tables; do python -m datagen.$m --help >/dev/null && echo "$m OK"; done`
Expected: four `… OK` lines.

- [ ] **Step 4: Root is clean**

Run: `git status --porcelain && ls *.png *.jpeg *.ipynb archive.zip 2>/dev/null`
Expected: the `ls` finds nothing (cruft gone); `git status` shows only the
expected, intended deltas.
