# SDK Tester Matrix

Single-click runner for the Fivetran SDK destination-connector-tester against
this connector. Replaces the manual 12-step ritual of running each
`(input × config)` combo by hand and spot-checking tables in BTEQ.

## Matrix

| Input file                                 | TMODE=ANSI | TMODE=TERA | use.fastload=true |
| ------------------------------------------ | ---------- | ---------- | ----------------- |
| `input.json`                               | ✓          | ✓          | ✓ (LOB-stripped)  |
| `schema_migrations_input_ddl.json`         | ✓          | ✓          | ✓                 |
| `schema_migrations_input_dml.json`         | ✓          | ✓          | ✓                 |
| `schema_migrations_input_sync_modes.json`  | ✓          | ✓          | ✓                 |

12 combos by default. The connector JAR stays running across all of them —
TMODE and `use.fastload` are read per-request from the gRPC `configurationMap`,
so no JVM restart is needed.

## Prerequisites

1. **Python 3.7+** (no third-party packages — stdlib only).
2. **`bteq`** on PATH (Teradata Tools and Utilities).
3. **`docker`** on PATH (Docker Desktop on Windows, docker-ce on Linux).
4. **Connector JAR running** on port 50052, in another terminal:
   ```
   java -jar build/libs/TeradataDestination.jar
   ```
5. **Env vars** for the Teradata connection (nothing sensitive in `config.json`).
   These match the names `IntegrationTestBase` already uses, so the same
   environment that runs `gradle test` runs this:
   ```
   TERADATA_HOST       Teradata host (IP or DNS, optionally :port)
   TERADATA_USER       Teradata username
   TERADATA_PASSWORD   Teradata password
   TERADATA_DATABASE   target database (BTEQ uses it for cleanup + assertions;
                       also passed to the connector so tables land in the same DB)
   ```
6. **The 4 canonical input JSONs** placed at `C:\Fivetran\` (or whichever
   `paths.canonical_inputs_dir` points to in `config.json`). Source:
   <https://github.com/fivetran/fivetran_partner_sdk/tree/main/tools/destination-connector-tester/input-files>

## Usage

```bash
# Run the full 12-combo matrix
python scripts/sdk_tester_matrix/run.py

# Run a subset (combo IDs are <input_name>__<config_id>)
python scripts/sdk_tester_matrix/run.py --only input__FastLoad,dml__TERA

# Stop on the first failure (default: keep going to give a complete view)
python scripts/sdk_tester_matrix/run.py --fail-fast

# Print the planned combos and exit (useful for sanity-checking config)
python scripts/sdk_tester_matrix/run.py --dry-run

# Delete the generated/ dir on a fully-passing run
python scripts/sdk_tester_matrix/run.py --purge-generated

# Custom config
python scripts/sdk_tester_matrix/run.py --config /path/to/my-config.json
```

### Exit codes

| Code | Meaning                                                                |
| ---- | ---------------------------------------------------------------------- |
| 0    | Every combo passed (tester rc=0 + every assertion's bad-rows count is 0) |
| 1    | At least one combo failed validation or the tester                     |
| 2    | Orchestrator fault: bad config, missing env var, missing CLI tool, connector port unreachable, or BTEQ logon failure |

## What a run does

For each combo (in order):

1. **Probe** `127.0.0.1:50052` — hard-fails fast if the JAR isn't up.
2. **Drop** the 10 known `tester_*` tables (idempotent; "doesn't exist" is silent).
3. **Write** `<canonical_inputs_dir>/configuration.json` with this combo's
   `tmode`, `use.fastload`, and the connection settings from env vars. The
   tester reads this file and forwards its contents as the gRPC
   `configurationMap`. An archive copy is kept under
   `<generated_dir>/configurations/<combo_id>.json`.
4. **Patch the input JSON**:
   - Auto-fix any upstream `[B@xxx` byte[].toString() junk
     (reuses `scripts/patch_tester_inputs.py`).
   - For `input.json × FastLoad` only: strip BLOB/JSON/XML columns
     (workaround for the parked Jira on FastLoad pre-flight LOB validation).
   - Write the patched copy to `<canonical_inputs_dir>/<combo_id>.json` so
     the docker bind mount can see it.
5. **Run** the SDK tester docker container, streaming logs to stderr and to
   `<generated_dir>/tester-logs/<combo_id>.log`.
6. **Validate** by running the combo's expectation file (`expectations/<input>.json`
   or `expectations/<input>.fastload.json`) as one BTEQ script. Each assertion
   is wrapped as `SELECT COUNT(*) FROM (<your_sql>) AS bad` — pass iff count
   is 0. The orchestrator parses tagged result rows.
7. **Record** the combo result.

After all combos finish, prints a console matrix and writes
`<generated_dir>/results.json`.

## Generated artifacts

```
<generated_dir>/
├── configurations/<combo_id>.json    # archive of configuration.json used
├── inputs/<combo_id>.json            # patched input handed to docker
├── tester-logs/<combo_id>.log        # captured tester stdout+stderr
├── bteq/<combo_id>__drop.sql/log     # cleanup
├── bteq/<combo_id>__assert.sql/log   # validation
└── results.json                      # final report
```

These are kept by default (post-mortem useful). Use `--purge-generated` to
delete on a fully-passing run.

## Editing expectations

Each `expectations/<input>.json` is a list of assertions. The uniform
contract is **bad-rows count must be 0**:

```json
{
  "name": "tester_transaction has 6 rows",
  "sql": "SELECT 1 FROM (SELECT COUNT(*) AS c FROM tester_transaction) t WHERE t.c <> 6"
}
```

The orchestrator wraps each `sql` as `SELECT COUNT(*) FROM (<sql>) bad_`,
so the SQL just has to surface "bad" rows — anything that should not be
true. Add new assertions by appending to the list.

For combos with a separate FastLoad variant (currently only `input`), the
orchestrator picks `fastload_expectations` from `config.json` when the
combo's `fastload=true`. Set `lob_columns_to_strip` in the variant to have
the orchestrator strip those columns from the input JSON before docker.

## Troubleshooting

- **`ERROR: connector not reachable on 127.0.0.1:50052`** — the JAR isn't
  running. Start it in another terminal.
- **`ERROR: 'bteq' not on PATH`** — install Teradata Tools and Utilities.
- **`ERROR: 'docker' not on PATH`** — install Docker Desktop / docker-ce.
- **`ERROR: env var $TERADATA_PASSWORD not set`** — export the env vars first.
- **Combo passes the tester but fails validation** — open
  `<generated_dir>/bteq/<combo_id>__assert.log`. Look for the
  `MATRIXASSERT_NNN <count>` rows; any count > 0 names a bad assertion.
- **Combo fails with `Error 3614 / 2652`** — the previous tester run left
  temp tables in *Loading* state. Drop them manually in BTEQ or restart
  the connector JAR (clears the Teradata sessions).

## Bumping the docker tester version

Edit `config.json` → `docker.tag` (currently `2.26.0410.001`). No code change.
