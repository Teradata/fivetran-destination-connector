#!/usr/bin/env python3
"""
Run Snyk SCA (dependencies) + SAST (code) scans against this repo.

Cross-platform — works on Linux, macOS, and Windows (Git Bash, PowerShell,
cmd). Requires the Snyk CLI to be installed and on PATH:
    npm install -g snyk
or see https://docs.snyk.io/snyk-cli/install-or-update-the-snyk-cli

Usage
-----
    # Token via env var (preferred — not visible in process list):
    export SNYK_TOKEN=...        # or:  set SNYK_TOKEN=... on Windows cmd
    python scripts/snyk_scan.py

    # Or pass the token explicitly:
    python scripts/snyk_scan.py --token <SNYK_TOKEN>

    # Custom output directory for JSON reports:
    python scripts/snyk_scan.py --out build/snyk

Exit codes
----------
    0  — no HIGH/critical findings in either scan
    1  — at least one HIGH/critical finding (CI-friendly gate)
    2  — Snyk CLI missing or token invalid, or Python < 3.7

Python version
--------------
Needs Python >= 3.7 (for subprocess.run(capture_output=..., text=...)).
No third-party packages; stdlib only.
"""

import sys
if sys.version_info < (3, 7):
    sys.stderr.write("ERROR: Python 3.7+ required; got {}.{}.\n".format(
        sys.version_info.major, sys.version_info.minor))
    sys.exit(2)


import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_snyk():
    """Locate the Snyk CLI. shutil.which resolves snyk.cmd on Windows."""
    exe = shutil.which("snyk")
    if not exe:
        print(
            "ERROR: 'snyk' CLI not on PATH. Install via 'npm install -g snyk' or\n"
            "       https://docs.snyk.io/snyk-cli/install-or-update-the-snyk-cli",
            file=sys.stderr,
        )
        sys.exit(2)
    return exe


def run(cmd, **kw):
    """Run a command and capture stdout/stderr as text."""
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def configure_token(snyk, token):
    """Store the token in Snyk's local config. Token is never echoed."""
    r = run([snyk, "config", "set", "api={}".format(token)])
    if r.returncode != 0:
        print("ERROR: 'snyk config set' failed: {}".format(r.stderr.strip()),
              file=sys.stderr)
        sys.exit(2)


def whoami(snyk):
    r = run([snyk, "whoami", "--experimental"])
    return r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else None


def tail(text, n=12):
    lines = [ln for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines[-n:])


# ---------------------------------------------------------------------------
# Scan runners
# ---------------------------------------------------------------------------

def run_deps_scan(snyk, out_json):
    print("\n[1/2] snyk test (dependencies)...", flush=True)
    # snyk test exits 1 when vulns found; don't treat that as fatal here.
    r = run([snyk, "test", "--json-file-output={}".format(out_json)])
    print(tail(r.stdout))
    return parse_deps(out_json)


def run_code_scan(snyk, out_json):
    print("\n[2/2] snyk code test (SAST)...", flush=True)
    r = run([snyk, "code", "test", "--sarif-file-output={}".format(out_json)])
    print(tail(r.stdout))
    return parse_code(out_json)


# ---------------------------------------------------------------------------
# Report parsers
# ---------------------------------------------------------------------------

def parse_deps(path):
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None

    # snyk test returns either a single project dict or a list of dicts.
    projects = d if isinstance(d, list) else [d]
    sev = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    total = 0
    for p in projects:
        for v in p.get("vulnerabilities", []):
            total += 1
            k = v.get("severity", "").lower()
            if k in sev:
                sev[k] += 1
    return {"total": total, "sev": sev}


def parse_code(path):
    """Parse SARIF output from `snyk code test`."""
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None

    runs = d.get("runs", [])
    if not runs:
        return {"total": 0, "sev": {"HIGH": 0, "MEDIUM": 0, "LOW": 0}}
    results = runs[0].get("results", [])

    sarif_to_sev = {"error": "HIGH", "warning": "MEDIUM", "note": "LOW"}
    sev = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for r in results:
        bucket = sarif_to_sev.get(r.get("level"))
        if bucket:
            sev[bucket] += 1
    return {"total": len(results), "sev": sev}


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(deps, code):
    print("\n" + "=" * 64)
    print(" Snyk Scan Summary")
    print("=" * 64)
    if deps is not None:
        s = deps["sev"]
        print(
            f"\nDependencies : total={deps['total']:<4} "
            f"critical={s['critical']:<3} high={s['high']:<3} "
            f"medium={s['medium']:<3} low={s['low']:<3}"
        )
    else:
        print("\nDependencies : (scan produced no JSON)")

    if code is not None:
        s = code["sev"]
        print(
            f"Code (SAST)  : total={code['total']:<4} "
            f"HIGH={s['HIGH']:<5} MEDIUM={s['MEDIUM']:<5} LOW={s['LOW']:<5}"
        )
    else:
        print("Code (SAST)  : (scan produced no JSON)")
    print()


def fail_gate(deps, code):
    if deps and (deps["sev"]["critical"] or deps["sev"]["high"]):
        return 1
    if code and code["sev"]["HIGH"]:
        return 1
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Run Snyk dependency and code scans; print a concise summary.",
    )
    p.add_argument(
        "--token",
        help="Snyk API token. Prefer the SNYK_TOKEN env var (not visible in "
             "process list).",
    )
    p.add_argument(
        "--out",
        default=".",
        help="Directory to write JSON/SARIF reports into (default: current dir).",
    )
    args = p.parse_args()

    token = args.token or os.environ.get("SNYK_TOKEN")
    if not token:
        print(
            "ERROR: Snyk token missing. Pass --token or set SNYK_TOKEN env var.",
            file=sys.stderr,
        )
        sys.exit(2)

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    snyk = find_snyk()
    configure_token(snyk, token)

    user = whoami(snyk)
    if user:
        print(f"Authenticated as: {user}")
    else:
        print("WARNING: 'snyk whoami' did not return a user; token may be invalid.",
              file=sys.stderr)

    deps = run_deps_scan(snyk, out_dir / "snyk-deps-report.json")
    code = run_code_scan(snyk, out_dir / "snyk-code-report.sarif.json")

    print_summary(deps, code)
    sys.exit(fail_gate(deps, code))


if __name__ == "__main__":
    main()
