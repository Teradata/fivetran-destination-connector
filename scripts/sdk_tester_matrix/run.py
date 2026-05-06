#!/usr/bin/env python3
"""
SDK Tester Matrix - single-click runner for the Fivetran SDK
destination-connector-tester against our locally-running Teradata destination.

Runs all combos of {input.json, schema_migrations_input_ddl.json,
schema_migrations_input_dml.json, schema_migrations_input_sync_modes.json}
x {TMODE=ANSI, TMODE=TERA, use.fastload=true} (12 combos by default), with:
  * pre-run cleanup (drops the known tester_* tables)
  * per-combo configuration.json generation (the tester reads this and forwards
    its contents as the gRPC configurationMap)
  * input JSON patching (auto-fixes upstream `[B@xxx` byte[].toString() junk;
    optionally strips LOB columns for the FastLoad input.json combo)
  * docker tester invocation with streaming logs
  * BTEQ-driven validation of expected end-state
  * console + JSON report

Prereqs
-------
  * `bteq` and `docker` on PATH
  * Connector JAR running on port 50052 (in another terminal)
  * Env vars: TERADATA_HOST, TERADATA_USER, TERADATA_PASSWORD, TERADATA_DATABASE
    (same names IntegrationTestBase already uses)
  * The 4 canonical input JSONs in C:\\Fivetran (or whichever
    paths.canonical_inputs_dir is set to in config.json)

Usage
-----
    python scripts/sdk_tester_matrix/run.py
    python scripts/sdk_tester_matrix/run.py --only input__FastLoad,dml__TERA
    python scripts/sdk_tester_matrix/run.py --fail-fast --purge-generated
    python scripts/sdk_tester_matrix/run.py --dry-run

Exit codes
----------
    0  - every combo passed (tester clean + every assertion 0 rows)
    1  - at least one combo failed validation or tester
    2  - orchestrator fault: bad config, missing env var, missing CLI tool,
         connector port unreachable, BTEQ logon failure
"""

import sys
if sys.version_info < (3, 7):
    sys.stderr.write("ERROR: Python 3.7+ required; got {}.{}.\n".format(
        sys.version_info.major, sys.version_info.minor))
    sys.exit(2)

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import time
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# tester_* tables created by any combo of the 4 SDK input JSONs. Dropped
# (idempotently) before each combo so every run starts from a clean slate.
TESTER_TABLES_TO_DROP = [
    "tester_transaction",
    "tester_campaign",
    "tester_composite_table",
    "tester_composite_table_alter_tmp",
    "tester_transaction_alter_tmp",
    "tester_transaction_drop",
    "tester_transaction_new",
    "tester_transaction_renamed",
    "tester_transaction_history",
    "tester_new_transaction_history",
]

# Teradata error 3807 = "Object … does not exist". Suppressed during the
# pre-run drop so a missing table is not a fatal error.
TD_ERR_TABLE_NOT_FOUND = 3807

# BTEQ exit codes we care about (per Teradata docs).
BTEQ_RC_OK = 0
BTEQ_RC_WARNING = 4
BTEQ_RC_DB_ERROR = 8
BTEQ_RC_CONNECTION = 12

# Tag prefix used in assertion SELECTs to anchor result parsing.
ASSERT_TAG_PREFIX = "MATRIXASSERT"


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(path):
    """Read config.json, resolve *_env keys to actual values from os.environ.
    Returns the resolved dict. Hard-fails (exit 2) on missing env vars or
    structurally invalid config, listing every problem at once.
    """
    if not path.is_file():
        sys.stderr.write("ERROR: config file not found: {}\n".format(path))
        sys.exit(2)

    try:
        cfg = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        sys.stderr.write("ERROR: config not valid JSON ({}): {}\n".format(path, e))
        sys.exit(2)

    problems = []
    for section in ("teradata", "connector", "docker", "paths", "matrix",
                    "configuration_template"):
        if section not in cfg:
            problems.append("missing top-level section: {}".format(section))

    td = cfg.get("teradata", {}) or {}
    resolved_td = {}
    for key, env_key in (("host", "host_env"), ("user", "user_env"),
                         ("password", "password_env"),
                         ("database", "database_env")):
        env_var = td.get(env_key)
        if not env_var:
            problems.append("teradata.{} not set in config".format(env_key))
            continue
        val = os.environ.get(env_var)
        if val is None or val.strip() == "":
            problems.append("env var ${} not set (referenced by teradata.{})"
                            .format(env_var, env_key))
            continue
        # Strip whitespace - guards against `set FOO= bar` accidentally
        # putting a leading space into the value (BTEQ .LOGON rejects it).
        resolved_td[key] = val.strip()
    cfg["teradata_resolved"] = resolved_td

    matrix = cfg.get("matrix", {}) or {}
    if not matrix.get("inputs") or not matrix.get("configs"):
        problems.append("matrix.inputs and matrix.configs both required")

    if problems:
        sys.stderr.write("ERROR: config invalid:\n")
        for p in problems:
            sys.stderr.write("  - {}\n".format(p))
        sys.exit(2)

    return cfg


# ---------------------------------------------------------------------------
# Matrix expansion
# ---------------------------------------------------------------------------

def expand_matrix(cfg, only=None):
    """Build the list of combo dicts. Order: input outermost, config inner.
    Combo id format: '<input_name>__<config_id>' (e.g. 'input__ANSI').
    """
    inputs = cfg["matrix"]["inputs"]
    configs = cfg["matrix"]["configs"]
    canonical_dir = Path(cfg["paths"]["canonical_inputs_dir"])
    expectations_dir = Path(cfg["paths"]["expectations_dir"])

    only_set = None
    if only:
        only_set = {s.strip() for s in only.split(",") if s.strip()}

    combos = []
    for inp in inputs:
        for conf in configs:
            cid = "{}__{}".format(inp["name"], conf["id"])
            if only_set and cid not in only_set:
                continue
            exp_key = ("fastload_expectations"
                       if conf.get("fastload") and inp.get("fastload_expectations")
                       else "expectations")
            combos.append({
                "id": cid,
                "input_name": inp["name"],
                "input_file": inp["file"],
                "input_path": canonical_dir / inp["file"],
                "tmode": conf["tmode"],
                "fastload": bool(conf.get("fastload", False)),
                "expectations_path": expectations_dir / inp[exp_key],
            })

    if only_set:
        unknown = only_set - {c["id"] for c in combos}
        if unknown:
            sys.stderr.write("ERROR: --only references unknown combos: {}\n"
                             .format(", ".join(sorted(unknown))))
            sys.exit(2)

    return combos


# ---------------------------------------------------------------------------
# Connector port probe
# ---------------------------------------------------------------------------

def probe_grpc_port(host, port, timeout=2.0):
    """Cheap TCP probe - returns True if a SYN/ACK comes back.
    Always probes 127.0.0.1 from the orchestrator's perspective; the
    docker container reaches the same gRPC server via host.docker.internal.
    """
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            return True
    except OSError:
        return False


def require_port_or_fail(cfg, label):
    port = cfg["connector"]["port"]
    if not probe_grpc_port(cfg["connector"]["host"], port):
        sys.stderr.write(
            "ERROR ({}): connector not reachable on 127.0.0.1:{}.\n"
            "  hint: start the connector in another terminal:\n"
            "        java -jar build/libs/TeradataDestination.jar\n"
            .format(label, port))
        sys.exit(2)


# ---------------------------------------------------------------------------
# BTEQ runner
# ---------------------------------------------------------------------------

def find_tool(name, hint):
    exe = shutil.which(name)
    if not exe:
        sys.stderr.write("ERROR: '{}' not on PATH.\n  hint: {}\n".format(name, hint))
        sys.exit(2)
    return exe


def bteq_run(cfg, sql_text, label, generated_dir):
    """Run BTEQ with `sql_text` piped in. Writes `<label>.sql` and `<label>.log`
    into generated_dir/bteq/. Returns (rc, stdout, stderr).
    """
    bteq_dir = generated_dir / "bteq"
    bteq_dir.mkdir(parents=True, exist_ok=True)
    sql_path = bteq_dir / "{}.sql".format(label)
    log_path = bteq_dir / "{}.log".format(label)
    sql_path.write_text(sql_text, encoding="utf-8")

    bteq = find_tool("bteq", "install Teradata Tools and Utilities")
    proc = subprocess.run([bteq], input=sql_text, capture_output=True, text=True)
    log_path.write_text(
        "$ bteq <<EOF\n{}\nEOF\n\n--- STDOUT ---\n{}\n--- STDERR ---\n{}\n--- RC {} ---\n"
        .format(sql_text, proc.stdout, proc.stderr, proc.returncode),
        encoding="utf-8")
    return proc.returncode, proc.stdout, proc.stderr


def bteq_logon_block(cfg):
    td = cfg["teradata_resolved"]
    return ".LOGON {}/{},{}\n".format(td["host"], td["user"], td["password"])


def drop_tester_tables(cfg, combo_id, generated_dir):
    """Run the standard 10-table drop. Suppresses 3807 ('table does not exist')
    so this is idempotent. Restores severity afterwards.
    """
    drops = "\n".join(
        "DROP TABLE \"{}\".\"{}\";".format(cfg["teradata_resolved"]["database"], t)
        for t in TESTER_TABLES_TO_DROP
    )
    sql = (
        ".SET WIDTH 500\n"
        ".SET FORMAT OFF\n"
        + bteq_logon_block(cfg)
        + ".SET ERRORLEVEL {} SEVERITY 0\n".format(TD_ERR_TABLE_NOT_FOUND)
        + drops + "\n"
        + ".SET ERRORLEVEL {} SEVERITY 8\n".format(TD_ERR_TABLE_NOT_FOUND)
        + ".LOGOFF\n.QUIT\n"
    )
    rc, out, err = bteq_run(cfg, sql, "{}__drop".format(combo_id), generated_dir)
    if rc == BTEQ_RC_CONNECTION:
        sys.stderr.write("ERROR: BTEQ logon failed during drop - see "
                         "{}/bteq/{}__drop.log\n".format(generated_dir, combo_id))
        sys.exit(2)
    # Anything other than 0 (clean) or warning (4) for the drop is an
    # orchestrator fault - we already suppressed "not exist".
    if rc not in (BTEQ_RC_OK, BTEQ_RC_WARNING):
        sys.stderr.write(
            "WARNING: drop block returned rc={} - proceeding anyway.\n".format(rc))


# ---------------------------------------------------------------------------
# configuration.json + input.json patching
# ---------------------------------------------------------------------------

def write_configuration_json(cfg, combo, generated_dir):
    """Write C:\\Fivetran\\configuration.json (the tester reads this) plus an
    archive copy under generated_dir/configurations/<combo_id>.json.
    """
    td = cfg["teradata_resolved"]
    contents = dict(cfg["configuration_template"])
    contents["host"] = td["host"]
    contents["user"] = td["user"]
    contents["td2password"] = td["password"]
    contents["database"] = td["database"]
    contents["tmode"] = combo["tmode"]
    contents["use.fastload"] = "true" if combo["fastload"] else "false"

    canonical_dir = Path(cfg["paths"]["canonical_inputs_dir"])
    live_path = canonical_dir / "configuration.json"
    live_path.write_text(json.dumps(contents, indent=4), encoding="utf-8")

    archive_dir = generated_dir / "configurations"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / "{}.json".format(combo["id"])
    archive_path.write_text(json.dumps(contents, indent=4), encoding="utf-8")
    return live_path


def import_patch_tester_inputs():
    """Lazy-import patch_tester_inputs from the parent scripts/ dir."""
    parent = str(Path(__file__).resolve().parent.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    import patch_tester_inputs  # noqa: E402
    return patch_tester_inputs


def _strip_lob_columns(node, lob_names):
    """Recursively walk the JSON and drop every key whose name matches
    a LOB column. Handles both:
      * column declarations:  "columns": {"binary_val": "BINARY", ...}
      * row payloads:         {"id": 1, "binary_val": "...", ...}
    The canonical Fivetran SDK input JSONs use a dict (name -> type) for
    column declarations, not a list of {name, type} entries, so the same
    "delete matching keys" rule covers both cases.
    """
    lob = set(lob_names)
    if isinstance(node, list):
        return [_strip_lob_columns(v, lob_names) for v in node]
    if isinstance(node, dict):
        return {k: _strip_lob_columns(v, lob_names)
                for k, v in node.items() if k not in lob}
    return node


def patch_input(combo, expectation, generated_dir):
    """Read the canonical input JSON, fix the upstream `[B@xxx` byte[].toString()
    junk via patch_tester_inputs, optionally strip LOB columns, and write to
    canonical_inputs_dir/<combo_id>.json so docker sees it via the bind mount.
    Returns the file name (relative to canonical_inputs_dir) for --input-file.
    """
    src = combo["input_path"]
    if not src.is_file():
        sys.stderr.write("ERROR: canonical input not found: {}\n".format(src))
        sys.exit(2)

    raw = json.loads(src.read_text(encoding="utf-8"))

    pti = import_patch_tester_inputs()
    counter = [0]
    patched = pti.patch_value(raw, counter)

    lob = expectation.get("lob_columns_to_strip") or []
    if lob and combo["fastload"]:
        patched = _strip_lob_columns(patched, lob)

    # Archive the patched copy under generated/inputs/, AND drop it next to
    # the canonical so docker's bind mount sees it.
    inputs_archive = generated_dir / "inputs"
    inputs_archive.mkdir(parents=True, exist_ok=True)
    archive_path = inputs_archive / "{}.json".format(combo["id"])
    archive_path.write_text(json.dumps(patched, indent=2), encoding="utf-8")

    canonical_dir = src.parent
    live_name = "{}.json".format(combo["id"])
    live_path = canonical_dir / live_name
    live_path.write_text(json.dumps(patched, indent=2), encoding="utf-8")

    return live_name, counter[0]


# ---------------------------------------------------------------------------
# Docker tester runner
# ---------------------------------------------------------------------------

def build_docker_argv(cfg, input_filename):
    d = cfg["docker"]
    argv = [
        "docker", "run",
        "--memory={}".format(d["memory"]),
        "-e", "JAVA_TOOL_OPTIONS={}".format(d["java_tool_options"]),
        "--mount", "type=bind,source={},target={}".format(d["mount_source"],
                                                          d["mount_target"]),
        "-a", "STDIN", "-a", "STDOUT", "-a", "STDERR",
        "-e", "WORKING_DIR={}".format(d["working_dir_env"]),
        "-e", "GRPC_HOSTNAME={}".format(cfg["connector"]["host"]),
        "--network={}".format(d["network"]),
        "--rm",
        "{}:{}".format(d["image"], d["tag"]),
        "--tester-type", "destination",
        "--port", str(cfg["connector"]["port"]),
        "--input-file", input_filename,
    ]
    argv.extend(d.get("extra_args", []))
    return argv


def run_tester(cfg, combo, input_filename, generated_dir):
    """Spawn docker, stream stdout/stderr to console + log file. Returns
    (exit_code, log_path, duration_s).
    """
    log_dir = generated_dir / "tester-logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "{}.log".format(combo["id"])
    argv = build_docker_argv(cfg, input_filename)

    sys.stderr.write(">>> docker: {}\n".format(" ".join(argv)))
    started = time.time()
    with log_path.open("w", encoding="utf-8") as logf:
        logf.write("$ " + " ".join(argv) + "\n\n")
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        try:
            for line in iter(proc.stdout.readline, ""):
                sys.stderr.write(line)
                logf.write(line)
        finally:
            proc.stdout.close()
        rc = proc.wait()
    return rc, log_path, time.time() - started


# ---------------------------------------------------------------------------
# Assertion compilation + parsing
# ---------------------------------------------------------------------------

def compile_assertions(cfg, expectation):
    """Build a BTEQ script that runs each assertion as its own SELECT
    statement. One bad assertion (compile error, missing column, etc.)
    therefore can't kill the others - BTEQ continues to the next
    statement and the matching ones still emit their MATRIXASSERT_NNN
    rows for the parser to scan.

    Each assertion's row pattern: 'MATRIXASSERT_NNN' tag followed by
    the bad-row count for that assertion.
    """
    db = cfg["teradata_resolved"]["database"]
    lines = [
        ".SET WIDTH 500",
        ".SET FORMAT OFF",
        ".SET FOLDLINE OFF",
        ".SET TITLEDASHES OFF",
        ".SET RTITLE OFF",
        bteq_logon_block(cfg).rstrip("\n"),
        "DATABASE \"{}\";".format(db),
    ]
    assertions = expectation.get("assertions", []) or []
    for i, a in enumerate(assertions):
        tag = "{}_{:03d}".format(ASSERT_TAG_PREFIX, i + 1)
        sql = a["sql"].strip().rstrip(";")
        lines.append(
            "SELECT '{}' AS tag_, "
            "(SELECT COUNT(*) FROM ({}) bad_) AS cnt_;".format(tag, sql)
        )
    lines.append(".LOGOFF")
    lines.append(".QUIT")
    return "\n".join(lines) + "\n"


_TAG_RE = re.compile(
    # Tolerant matcher: anywhere on a line, MATRIXASSERT_NNN followed by any
    # whitespace and then digits. BTEQ sometimes pads the literal column to
    # CHAR(N), wraps the count in spaces, etc. - all fine.
    ASSERT_TAG_PREFIX + r"_(\d{3})\s+(\d+)\b",
)


def parse_assertion_results(stdout, assertions):
    """Pull (idx, count) pairs from BTEQ stdout. Returns a list aligned with
    `assertions`: each entry {name, count, ok, found}.
    """
    found = {}
    for m in _TAG_RE.finditer(stdout):
        idx = int(m.group(1))
        cnt = int(m.group(2))
        found[idx] = cnt
    out = []
    for i, a in enumerate(assertions):
        idx = i + 1
        cnt = found.get(idx)
        out.append({
            "name": a["name"],
            "count": cnt,
            "ok": cnt == 0,
            "found": cnt is not None,
        })
    return out


# ---------------------------------------------------------------------------
# Per-combo orchestration
# ---------------------------------------------------------------------------

def run_combo(cfg, combo, generated_dir, idx, total):
    label = "[{}/{}] {} x {}".format(idx, total, combo["input_name"],
                                     "FastLoad" if combo["fastload"]
                                     else combo["tmode"])
    sys.stderr.write("\n=== {} ===\n".format(label))
    require_port_or_fail(cfg, label)

    expectation = json.loads(combo["expectations_path"].read_text(encoding="utf-8"))

    drop_tester_tables(cfg, combo["id"], generated_dir)
    write_configuration_json(cfg, combo, generated_dir)
    input_filename, junk_replaced = patch_input(combo, expectation, generated_dir)
    if junk_replaced:
        sys.stderr.write("    (patched {} byte[].toString() junk values)\n"
                         .format(junk_replaced))

    tester_rc, tester_log, tester_dur = run_tester(
        cfg, combo, input_filename, generated_dir)

    assertions = expectation.get("assertions", []) or []
    if assertions:
        sql = compile_assertions(cfg, expectation)
        rc, out, err = bteq_run(cfg, sql, "{}__assert".format(combo["id"]),
                                generated_dir)
        if rc == BTEQ_RC_CONNECTION:
            sys.stderr.write("ERROR: BTEQ logon failed during validation.\n")
            sys.exit(2)
        results = parse_assertion_results(out, assertions)
        validate_pass = all(r["ok"] and r["found"] for r in results)
        validate_rc = rc
        # If BTEQ ran but we found ZERO assertion rows, dump diagnostics.
        # Most likely: bad logon (e.g. whitespace in user), missing privs
        # on the database, or a SQL error before any UNION-ALL row emitted.
        n_found = sum(1 for r in results if r["found"])
        if assertions and n_found == 0:
            sys.stderr.write(
                "    !! validation parsed 0 of {} assertions - BTEQ rc={}\n"
                "    !! see {}/bteq/{}__assert.log\n"
                "    !! tail of BTEQ stdout:\n".format(
                    len(assertions), rc, generated_dir, combo["id"]))
            tail_lines = (out or "").splitlines()[-40:]
            for line in tail_lines:
                sys.stderr.write("        {}\n".format(line))
            if err and err.strip():
                sys.stderr.write("    !! BTEQ stderr:\n")
                for line in err.splitlines()[-20:]:
                    sys.stderr.write("        {}\n".format(line))
    else:
        results = []
        validate_pass = tester_rc == 0
        validate_rc = 0

    overall = (tester_rc == 0) and validate_pass

    sys.stderr.write("    tester rc={}  validate {}/{}\n".format(
        tester_rc,
        sum(1 for r in results if r["ok"] and r["found"]),
        len(results),
    ))

    return {
        "id": combo["id"],
        "input": combo["input_name"],
        "config": "FastLoad" if combo["fastload"] else combo["tmode"],
        "tmode": combo["tmode"],
        "fastload": combo["fastload"],
        "tester_rc": tester_rc,
        "tester_duration_s": round(tester_dur, 2),
        "tester_log": str(tester_log),
        "validate_rc": validate_rc,
        "assertions": results,
        "overall_pass": overall,
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_console_table(cfg, results, total_dur):
    print()
    print("=" * 80)
    print(" SDK Tester Matrix - {} - image {}:{}".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        cfg["docker"]["image"].rsplit("/", 1)[-1],
        cfg["docker"]["tag"],
    ))
    print("=" * 80)
    print(" {:<3} {:<18} {:<10} {:<6} {:<8} {:<8} {}".format(
        "#", "Input", "Config", "Tester", "Validate", "Duration", "Log"))
    print(" {:<3} {:<18} {:<10} {:<6} {:<8} {:<8} {}".format(
        "-" * 3, "-" * 18, "-" * 10, "-" * 6, "-" * 8, "-" * 8, "-" * 20))
    pass_n = fail_n = 0
    for i, r in enumerate(results, 1):
        tester_str = "PASS" if r["tester_rc"] == 0 else "FAIL({})".format(r["tester_rc"])
        if r["assertions"]:
            n_ok = sum(1 for a in r["assertions"] if a["ok"] and a["found"])
            n_total = len(r["assertions"])
            v_str = "PASS" if n_ok == n_total else "FAIL({}/{})".format(n_ok, n_total)
        else:
            v_str = "n/a"
        d = r["tester_duration_s"]
        dur_str = "{:02d}:{:02d}".format(int(d) // 60, int(d) % 60)
        log_short = Path(r["tester_log"]).name
        print(" {:<3} {:<18} {:<10} {:<6} {:<8} {:<8} {}".format(
            i, r["input"][:18], r["config"][:10], tester_str, v_str, dur_str, log_short))
        if r["overall_pass"]:
            pass_n += 1
        else:
            fail_n += 1
    print(" {:<3} {:<18} {:<10} {:<6} {:<8} {:<8} {}".format(
        "-" * 3, "-" * 18, "-" * 10, "-" * 6, "-" * 8, "-" * 8, "-" * 20))
    total_str = "{:02d}:{:02d}".format(int(total_dur) // 60, int(total_dur) % 60)
    print(" PASS: {}/{}   FAIL: {}/{}   total {}".format(
        pass_n, len(results), fail_n, len(results), total_str))

    failures = []
    for r in results:
        if r["tester_rc"] != 0:
            failures.append((r["id"], "tester rc={}".format(r["tester_rc"])))
        for a in r["assertions"]:
            if not a["found"]:
                failures.append((r["id"], "assertion not found: {}".format(a["name"])))
            elif not a["ok"]:
                failures.append((r["id"], "{} (got {})".format(a["name"], a["count"])))

    if failures:
        print()
        print("Failures:")
        for cid, msg in failures:
            print("  {:<25} {}".format(cid, msg))


def write_results_json(path, cfg, results):
    payload = {
        "image": "{}:{}".format(cfg["docker"]["image"], cfg["docker"]["tag"]),
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "combos": results,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Run the Fivetran SDK destination-connector-tester across "
                    "every (input x config) combo and validate the result.",
    )
    here = Path(__file__).resolve().parent
    p.add_argument("--config", default=str(here / "config.json"),
                   help="Path to config.json (default: %(default)s).")
    p.add_argument("--only",
                   help="Comma-separated list of combo IDs to run "
                        "(e.g. 'input__FastLoad,dml__TERA').")
    p.add_argument("--fail-fast", action="store_true",
                   help="Stop on the first combo that fails. Default: run all "
                        "12 to give a complete matrix view.")
    p.add_argument("--purge-generated", action="store_true",
                   help="Delete the generated directory after a fully-passing run.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the planned combos and the docker argv for each, "
                        "then exit without running anything.")
    return p.parse_args()


def main():
    args = parse_args()
    cfg_path = Path(args.config).resolve()
    cfg = load_config(cfg_path)

    # Resolve expectations_dir relative to repo root if it's a relative path.
    exp_dir = Path(cfg["paths"]["expectations_dir"])
    if not exp_dir.is_absolute():
        exp_dir = cfg_path.parent.parent.parent / exp_dir
    cfg["paths"]["expectations_dir"] = str(exp_dir)

    combos = expand_matrix(cfg, only=args.only)
    if not combos:
        sys.stderr.write("ERROR: no combos selected.\n")
        sys.exit(2)

    generated_dir = Path(cfg["paths"]["generated_dir"])
    generated_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        print("Dry run - would execute {} combo(s):".format(len(combos)))
        for i, c in enumerate(combos, 1):
            argv = build_docker_argv(cfg, "{}.json".format(c["id"]))
            print("  [{}] {}".format(i, c["id"]))
            print("      input    : {}".format(c["input_path"]))
            print("      tmode    : {}".format(c["tmode"]))
            print("      fastload : {}".format(c["fastload"]))
            print("      docker   : {}".format(" ".join(argv)))
        return 0

    find_tool("docker", "install Docker Desktop or docker-ce")
    find_tool("bteq", "install Teradata Tools and Utilities")
    require_port_or_fail(cfg, "startup")

    started = time.time()
    results = []
    for i, c in enumerate(combos, 1):
        try:
            r = run_combo(cfg, c, generated_dir, i, len(combos))
        except SystemExit:
            raise
        except Exception as e:
            sys.stderr.write("ERROR running {}: {}\n".format(c["id"], e))
            r = {
                "id": c["id"],
                "input": c["input_name"],
                "config": "FastLoad" if c["fastload"] else c["tmode"],
                "tmode": c["tmode"],
                "fastload": c["fastload"],
                "tester_rc": -1,
                "tester_duration_s": 0,
                "tester_log": "",
                "validate_rc": -1,
                "assertions": [],
                "overall_pass": False,
            }
        results.append(r)
        if args.fail_fast and not r["overall_pass"]:
            sys.stderr.write("--fail-fast: stopping after combo {}\n".format(c["id"]))
            break

    total_dur = time.time() - started
    print_console_table(cfg, results, total_dur)

    results_json = generated_dir / "results.json"
    write_results_json(results_json, cfg, results)
    print()
    print("Results JSON: {}".format(results_json))

    all_pass = all(r["overall_pass"] for r in results) and len(results) == len(combos)
    if all_pass and args.purge_generated:
        shutil.rmtree(generated_dir, ignore_errors=True)

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
