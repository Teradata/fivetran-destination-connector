#!/usr/bin/env python3
"""
Patch Fivetran SDK destination-connector-tester input JSONs to replace junk
Java byte[].toString() values (e.g. "[B@7d4991ad") with valid base64.

Workaround for an upstream defect in
https://github.com/fivetran/fivetran_partner_sdk/blob/main/tools/destination-connector-tester/input-files/input.json
where some binary_val fields contain "[B@<hex>" — the literal output of
calling .toString() on a Java byte[] — instead of a base64-encoded payload.
Java's Base64.getDecoder() rightly rejects these with
"Illegal base64 character 5b" (0x5b is '[').

Replacements are deterministic by occurrence order: the first junk value
becomes base64 of "Row 1", the second "Row 2", etc. This matches the
convention used in PR #29's manual patches so the existing validation SQL
keeps decoding to the same expected byte sequences.

Usage
-----
    # Print patched content to stdout (single file):
    python scripts/patch_tester_inputs.py input.json

    # Rewrite the file in place:
    python scripts/patch_tester_inputs.py input.json --in-place

    # Patch every JSON in a folder, in place:
    python scripts/patch_tester_inputs.py /path/to/data/*.json --in-place

    # Just count without writing:
    python scripts/patch_tester_inputs.py input.json --dry-run

Exit codes
----------
    0  — completed (with or without replacements)
    1  — bad input (no files / file unreadable / multiple files without --in-place)

Python 3.7+. Stdlib only.
"""

import sys
if sys.version_info < (3, 7):
    sys.stderr.write("ERROR: Python 3.7+ required; got {}.{}.\n".format(
        sys.version_info.major, sys.version_info.minor))
    sys.exit(1)

import argparse
import base64
import json
import re
from pathlib import Path


# Matches Java Object.toString() output for arrays. Examples:
#   [B@7d4991ad           — byte[]
#   [I@deadbeef           — int[]
#   [[B@cafebabe          — byte[][]
#   [Ljava.lang.String;@1 — String[]
JAVA_ARRAY_TOSTRING = re.compile(r"^\[+(?:[BCDFIJSZ]|L[\w.$]+;)@[0-9a-f]+$")


def encoded_row(n):
    """Return base64('Row N') as an ASCII string."""
    return base64.b64encode("Row {}".format(n).encode("ascii")).decode("ascii")


def patch_value(value, counter):
    """Recursively walk a JSON-decoded structure and replace junk values."""
    if isinstance(value, dict):
        return {k: patch_value(v, counter) for k, v in value.items()}
    if isinstance(value, list):
        return [patch_value(v, counter) for v in value]
    if isinstance(value, str) and JAVA_ARRAY_TOSTRING.match(value):
        counter[0] += 1
        return encoded_row(counter[0])
    return value


def patch_file(path, in_place, dry_run, multi):
    """Patch one file. Returns the count of replacements made (or 0)."""
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as e:
        print("{}: cannot read ({})".format(path, e), file=sys.stderr)
        return 0
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print("{}: not valid JSON ({})".format(path, e), file=sys.stderr)
        return 0

    counter = [0]
    patched = patch_value(data, counter)

    if counter[0] == 0:
        print("{}: no Java-array-toString junk found".format(path),
              file=sys.stderr)
        return 0

    out = json.dumps(patched, indent=2)

    if dry_run:
        print("{}: would replace {} junk value(s)".format(path, counter[0]))
        return counter[0]

    if in_place:
        path.write_text(out, encoding="utf-8")
        print("{}: replaced {} junk value(s) (in-place)".format(path, counter[0]),
              file=sys.stderr)
    else:
        if multi:
            print("\n# === {} ===".format(path))
        sys.stdout.write(out)
        if not out.endswith("\n"):
            sys.stdout.write("\n")
    return counter[0]


def main():
    p = argparse.ArgumentParser(
        description="Patch Java byte[].toString() junk in Fivetran tester input JSONs."
    )
    p.add_argument("paths", nargs="+", help="JSON files to patch.")
    p.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the source file with the patched content (recommended).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the count of replacements without writing anything.",
    )
    args = p.parse_args()

    files = [Path(s) for s in args.paths]
    missing = [f for f in files if not f.is_file()]
    if missing:
        for f in missing:
            print("{}: not a file".format(f), file=sys.stderr)
        sys.exit(1)

    multi = len(files) > 1
    if multi and not args.in_place and not args.dry_run:
        print(
            "ERROR: --in-place required when patching multiple files "
            "(or use --dry-run).",
            file=sys.stderr,
        )
        sys.exit(1)

    total = 0
    for f in files:
        total += patch_file(f, args.in_place, args.dry_run, multi)

    print(
        "Done. {} junk value(s) replaced across {} file(s).".format(total, len(files)),
        file=sys.stderr,
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
