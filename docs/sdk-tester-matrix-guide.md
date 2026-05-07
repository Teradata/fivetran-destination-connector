# SDK Tester Matrix — User Guide

## What it is

A single-command runner for the Fivetran SDK destination-connector-tester against the Teradata destination connector. Replaces the manual 12-step ritual of running each `(input × config)` combo by hand and spot-checking tables in BTEQ.

The matrix covers every combination that matters for a release sign-off:

| Input file                                 | TMODE=ANSI | TMODE=TERA | use.fastload=true |
| ------------------------------------------ | ---------- | ---------- | ----------------- |
| `input.json`                               | ✓          | ✓          | ✓ (LOB-stripped)  |
| `schema_migrations_input_ddl.json`         | ✓          | ✓          | ✓                 |
| `schema_migrations_input_dml.json`         | ✓          | ✓          | ✓                 |
| `schema_migrations_input_sync_modes.json`  | ✓          | ✓          | ✓                 |

12 combos by default. The connector JAR stays running across all of them — TMODE and `use.fastload` are read per-request from the gRPC `configurationMap`, so no JVM restart is needed.

## Why use it

- One command, ~28 minutes wall clock, vs. half a day of manual work.
- Catches **silent failures** like IDE-26074, where the connector reports success but tables are empty — manual spot-checks miss these.
- Reproducible: same expectations every run, no human variance.
- Post-mortem-friendly: every BTEQ script + tester log is archived per combo.

## Prerequisites

| Tool            | Why                                              | Where to get it                                     |
| --------------- | ------------------------------------------------ | --------------------------------------------------- |
| Python 3.7+     | Orchestrator (stdlib only — no `pip install`)    | [python.org](https://www.python.org)                |
| BTEQ            | Validation queries against Teradata              | Teradata Tools and Utilities (TTU)                  |
| Docker          | Runs the SDK tester image                        | Docker Desktop (Windows) / docker-ce (Linux)        |
| Connector JAR   | Service under test (port 50052)                  | `gradle jar` → `build/libs/TeradataDestination.jar` |

Environment variables (mirror what `gradle test` already uses):

```
TERADATA_HOST       Teradata host (IP or DNS, optionally :port)
TERADATA_USER       Teradata username
TERADATA_PASSWORD   Teradata password
TERADATA_DATABASE   Target database (used by BTEQ for cleanup + assertions; also forwarded to the connector)
```

## Quick Start

1. **Build the JAR**:
   ```
   gradle jar
   ```

2. **Start the connector** in one terminal — it stays running across all 12 combos:
   ```
   java -jar build/libs/TeradataDestination.jar
   ```

3. **Set env vars** in a second terminal:
   ```
   export TERADATA_HOST=<your-teradata-host>
   export TERADATA_USER=<your-username>
   export TERADATA_PASSWORD=<your-password>
   export TERADATA_DATABASE=<your-database>
   ```

4. **Run the matrix**:
   ```
   python scripts/sdk_tester_matrix/run.py
   ```

5. **Read the report** at the end. Exit code 0 = ship it. Exit code 1 = read the failures section + open the named log.

## How a run works

For each combo, in order:

1. **Probe** `127.0.0.1:50052` — hard-fails fast if the JAR isn't up (exit 2).
2. **Drop** the 10 known `tester_*` tables (idempotent — "doesn't exist" is silent).
3. **Write `configuration.json`** to the docker mount source with the combo's `tmode` + `use.fastload` + connection settings. The tester reads this file at runtime and forwards its contents as the gRPC `configurationMap`. An archive copy is kept under `<generated_dir>/configurations/<combo_id>.json`.
4. **Patch the input JSON**:
   - Auto-fix `[B@xxx` byte[].toString() junk (reuses `scripts/patch_tester_inputs.py`).
   - For `input.json × FastLoad` only: strip BLOB/JSON/XML columns (workaround for FastLoad LOB pre-flight gap).
   - Write the patched copy to the mount source so docker can see it via the bind mount.
5. **Run** the SDK tester docker container, streaming logs to stderr and to `<generated_dir>/tester-logs/<combo_id>.log`.
6. **Validate** by running the combo's expectation file (`expectations/<input>.json` or `expectations/<input>.fastload.json`) as one BTEQ script. Each assertion is wrapped as `SELECT COUNT(*) FROM (<your_sql>) bad_` — pass iff count is 0.
7. **Record** the combo result.

After all combos finish: console matrix table (stdout), full `results.json` for post-mortem.

## CLI Reference

```
python scripts/sdk_tester_matrix/run.py [OPTIONS]
```

| Flag                   | Effect                                                              |
| ---------------------- | ------------------------------------------------------------------- |
| `--only <csv>`         | Run a subset (combo IDs are `<input_name>__<config_id>`, comma-separated). E.g. `--only input__FastLoad,dml__TERA`. |
| `--fail-fast`          | Stop on the first combo failure. Default: keep going for a complete view. |
| `--dry-run`            | Print the planned combos and exit. Useful for sanity-checking config changes. |
| `--purge-generated`    | Delete `<generated_dir>` at end of a fully-passing run.            |
| `--config <path>`      | Use a custom config file. Default: `scripts/sdk_tester_matrix/config.json`. |

### Exit codes

| Code | Meaning                                                                |
| ---- | ---------------------------------------------------------------------- |
| 0    | Every combo passed (tester rc=0 + every assertion's bad-rows count is 0) |
| 1    | At least one combo failed validation or the tester                     |
| 2    | Orchestrator fault: bad config, missing env var, missing CLI tool, port unreachable, BTEQ logon failure |

## Reading the report

Streamed during the run (to stderr):

```
[1/12] input × ANSI       … tester rc=0 in 39s, validating … 5/5 assertions pass
[2/12] input × TERA       … tester rc=0 in 41s, validating … 5/5 assertions pass
[3/12] input × FastLoad   … tester rc=0 in 47s, validating … 4/5 assertions pass
```

Final summary (stdout):

```
================================================================================
 SDK Tester Matrix — 2026-05-06 10:45:06 — image sdk-tester:2.26.0410.001
================================================================================
 #   Input              Config     Tester Validate Duration Log
 --- ------------------ ---------- ------ -------- -------- ----------------
 1   input              ANSI       PASS   PASS     01:59    input__ANSI.log
 ...
 --- ------------------ ---------- ------ -------- -------- ----------------
 PASS: 12/12   FAIL: 0/12   total 28:22
```

If anything fails, the failures section names the combo and the assertion that bombed:

```
Failures:
 #3  input/FastLoad    no soft-deleted rows in base table (got 1)
```

Then open `<generated_dir>/bteq/<combo_id>__assert.log` to see which `MATRIXASSERT_NNN` row had a count > 0.

## Generated artifacts

```
<generated_dir>/
├── configurations/<combo_id>.json    # archive of configuration.json used
├── inputs/<combo_id>.json            # patched input handed to docker
├── tester-logs/<combo_id>.log        # captured tester stdout+stderr
├── bteq/<combo_id>__drop.sql/log     # cleanup script + output
├── bteq/<combo_id>__assert.sql/log   # validation script + output
└── results.json                      # final structured report
```

These are kept by default (post-mortem useful). Use `--purge-generated` to delete on a fully-passing run.

## Adding a new test case

The matrix is data-driven. To add a 5th input:

1. **Drop the input JSON** in `scripts/sdk_tester_matrix/inputs/`. It's automatically picked up.
2. **Add an expectation file** in `scripts/sdk_tester_matrix/expectations/<your_input>.json`:
   ```json
   {
     "tables_required": ["tester_my_table"],
     "lob_columns_to_strip": [],
     "assertions": [
       {
         "name": "tester_my_table has expected row count",
         "sql": "SELECT 1 AS x FROM (SELECT COUNT(*) AS c FROM tester_my_table) t WHERE t.c <> 5"
       }
     ]
   }
   ```
3. **Register it** in `scripts/sdk_tester_matrix/config.json` under `matrix.inputs`:
   ```json
   {
     "name": "my_test",
     "file": "my_input.json",
     "expectations": "my_input.json"
   }
   ```
4. **Re-run**: `python scripts/sdk_tester_matrix/run.py`. The matrix is now 5×3 = 15 combos.

If the new test creates additional `tester_*` tables, append their names to `TESTER_TABLES_TO_DROP` in `run.py` (module constant near the top) so they get cleaned up between combos.

## Editing expectations

The expectation contract is **bad-rows count must be 0**:

```json
{
  "name": "amount column updated correctly for all rows",
  "sql": "SELECT 1 AS x FROM tester_transaction WHERE amount_renamed <> 202.57 OR amount_renamed IS NULL"
}
```

The orchestrator wraps each `sql` as `SELECT COUNT(*) FROM (<your_sql>) bad_`. Author the SQL so it surfaces "bad" rows — anything that **should not be true**. Pass iff the wrapped query returns 0.

**Contract notes:**

- Every projection column needs an explicit name. Use `SELECT 1 AS x FROM …`, `SELECT id FROM …`, etc. Never bare `SELECT 1 FROM …` — Teradata error 3706: "All expressions in a derived table must have an explicit name".
- Quote reserved/special column names. Teradata's `desc` is reserved → use `"desc"`.
- For combos with a separate FastLoad variant (currently only `input`), set `fastload_expectations` in config and write a `<input>.fastload.json` reflecting the trimmed schema.

## Troubleshooting

| Symptom                                                                 | Likely cause / fix                                                                          |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `ERROR: connector not reachable on 127.0.0.1:50052`                     | The JAR isn't running. Start it in another terminal.                                        |
| `ERROR: 'bteq' not on PATH`                                             | Install Teradata Tools and Utilities.                                                        |
| `ERROR: 'docker' not on PATH`                                           | Install Docker Desktop / docker-ce.                                                          |
| `ERROR: env var $TERADATA_PASSWORD not set`                             | Export the env vars first (see Quick Start).                                                 |
| Combo passes the tester but fails validation                            | Open `<generated_dir>/bteq/<combo_id>__assert.log`. Look for `MATRIXASSERT_NNN <count>` rows; any count > 0 names a bad assertion. |
| Combo fails with `Error 3614 / 2652`                                    | Previous run left temp tables in *Loading* state. Drop them in BTEQ or restart the JAR (clears Teradata sessions). |
| All FastLoad combos fail with empty target tables                       | If you see this, check whether IDE-26074 fix is in your build. Pre-fix builds silently dropped FastLoad data when configured DB ≠ JDBC user home. |
| `Error 5628 Column X not found` in assertions                           | The schema migration step didn't apply (likely because the upstream FastLoad write failed silently — see above). |

## Bumping the docker tester version

Edit `config.json` → `docker.tag` (currently `2.26.0410.001`). No code change.

## FAQ

**Q: Why does the JAR need to stay running across all 12 combos?**
TMODE and `use.fastload` are read per-request from the gRPC `configurationMap`, not at JVM startup. So one JAR handles the whole matrix without restarts.

**Q: Where does `configuration.json` live?**
The orchestrator writes it to the docker bind-mount source (`docker.mount_source` in config — default `C:\Fivetran` on Windows). Each combo overwrites it. Archived copies live under `<generated_dir>/configurations/`.

**Q: Why are inputs vendored in the repo instead of downloaded each run?**
Reproducibility. Upstream changes to canonical SDK inputs would silently break the matrix. The vendored copies are the contract; bumping them is a deliberate commit.

**Q: How do I re-run only the failing combos?**
Use `--only <combo-id-csv>`. The combo IDs are in the report. E.g. `--only input__FastLoad,dml__FastLoad,sync_modes__FastLoad`.

**Q: Can I run this in CI?**
Yes — exit codes are CI-friendly (0 / 1 / 2). The run is hermetic *given* a Teradata host, env vars, and a running JAR. The orchestrator will need network access to the Docker registry and the Teradata host.
