#!/usr/bin/env bash
# Run the Fivetran SDK destination-connector-tester against every JSON in the
# data folder, with per-JSON table reset and post-run validation.
#
# Env vars (required):
#   TERADATA_HOST, TERADATA_USER, TERADATA_PASSWORD
# Env vars (optional):
#   TERADATA_DATABASE  (default: junit)
#   TERADATA_LOGMECH   (default: TD2)
#   TERADATA_TMODE     (default: ANSI)
#   DATA_FOLDER        (default: /data/fivetran)    -- host path to input JSONs + configuration.json
#   DATA_FOLDER_WIN    (default: $DATA_FOLDER)      -- native Windows form for docker bind, only set on Windows
#   TESTER_VERSION     (default: 2.26.0410.001)
#
# Flags:
#   --matrix                run all 4 combinations: (TERA|ANSI) x (fastload false|true)
#   --tmode <TERA|ANSI>     patch configuration.json tmode for a single run
#   --fastload <true|false> patch configuration.json use.fastload for a single run
#   --json <file>           run only one JSON from DATA_FOLDER
#   --skip-build            skip gradlew jar
#   --keep-running          leave connector running after tests
#   --tester-version <ver>  override TESTER_VERSION
#
# Exit code: number of failed runs (0 = all pass)

set -u
trap 'stop_connector' EXIT

: "${TERADATA_HOST:?set TERADATA_HOST}"
: "${TERADATA_USER:?set TERADATA_USER}"
: "${TERADATA_PASSWORD:?set TERADATA_PASSWORD}"
: "${TERADATA_DATABASE:=junit}"
: "${TERADATA_LOGMECH:=TD2}"
: "${TERADATA_TMODE:=ANSI}"

DATA_FOLDER="${DATA_FOLDER:-/data/fivetran}"
DATA_FOLDER_WIN="${DATA_FOLDER_WIN:-$DATA_FOLDER}"
TESTER_VERSION="${TESTER_VERSION:-2.26.0410.001}"
TESTER_IMAGE_BASE="us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester"
JAR="build/libs/TeradataDestination.jar"
LOG_DIR="sdk-test-logs"
CONNECTOR_LOG="$LOG_DIR/connector.log"
VALIDATION_DIR="validation"

SKIP_BUILD=false
KEEP_RUNNING=false
SINGLE_JSON=""
MATRIX=false
OVERRIDE_TMODE=""
OVERRIDE_FASTLOAD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --matrix)          MATRIX=true; shift ;;
    --tmode)           OVERRIDE_TMODE="$2"; shift 2 ;;
    --fastload)        OVERRIDE_FASTLOAD="$2"; shift 2 ;;
    --json)            SINGLE_JSON="$2"; shift 2 ;;
    --skip-build)      SKIP_BUILD=true; shift ;;
    --keep-running)    KEEP_RUNNING=true; shift ;;
    --tester-version)  TESTER_VERSION="$2"; shift 2 ;;
    -h|--help)         sed -n '2,28p' "$0"; exit 0 ;;
    *)                 echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

TESTER_IMAGE="${TESTER_IMAGE_BASE}:${TESTER_VERSION}"
mkdir -p "$LOG_DIR"

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

pass=0
fail=0
declare -a results

say() { printf '\n=== %s ===\n' "$*"; }

run_bteq() {
  # Usage: run_bteq "<sql>"  -- runs under TERADATA_DATABASE, ignores 3807/3802
  local sql="$1"
  bteq <<EOF
.LOGON ${TERADATA_HOST}/${TERADATA_USER},${TERADATA_PASSWORD};
.SET ERRORLEVEL 3807 SEVERITY 0;
.SET ERRORLEVEL 3802 SEVERITY 0;
DATABASE "${TERADATA_DATABASE}";
${sql}
.LOGOFF;
.QUIT;
EOF
}

drop_known_tables() {
  run_bteq "$(cat <<'SQL'
DROP TABLE "transaction";
DROP TABLE "campaign";
DROP TABLE "composite_table";
DROP TABLE "transaction_drop";
DROP TABLE "transaction_new";
DROP TABLE "transaction_renamed";
DROP TABLE "transaction_history";
DROP TABLE "new_transaction_history";
SQL
)" > "$LOG_DIR/reset.log" 2>&1 || true
}

wait_for_port() {
  local host="$1" port="$2" tries=0
  until (echo >"/dev/tcp/$host/$port") 2>/dev/null; do
    ((tries++ >= 30)) && { echo "connector did not come up on $host:$port" >&2; return 1; }
    sleep 1
  done
}

start_connector() {
  if [[ ! -f "$JAR" ]]; then
    echo "JAR missing: $JAR. Re-run without --skip-build or build it first." >&2
    exit 1
  fi
  local tag="${1:-}"
  local logfile="$CONNECTOR_LOG"
  [[ -n "$tag" ]] && logfile="$LOG_DIR/connector-${tag}.log"
  say "Starting connector on port 50052 (log=$logfile)"
  : > "$logfile"
  nohup java -Ddebuglog=yes -jar "$JAR" > "$logfile" 2>&1 &
  CONN_PID=$!
  CONNECTOR_LOG="$logfile"
  echo "connector pid=$CONN_PID"
  wait_for_port 127.0.0.1 50052 || { kill "$CONN_PID" 2>/dev/null; exit 1; }
}

stop_connector() {
  $KEEP_RUNNING && { echo "(--keep-running) connector left alive at pid ${CONN_PID:-?}"; return; }
  [[ -n "${CONN_PID:-}" ]] && kill "$CONN_PID" 2>/dev/null || true
  # wait for port to free
  local tries=0
  while (echo >"/dev/tcp/127.0.0.1/50052") 2>/dev/null; do
    ((tries++ >= 10)) && break
    sleep 1
  done
  CONN_PID=""
}

patch_config() {
  # Usage: patch_config <tmode> <fastload>
  local tmode="$1" fl="$2"
  local cfg="$DATA_FOLDER/configuration.json"
  if [[ ! -f "$cfg" ]]; then
    echo "configuration.json missing at $cfg" >&2
    echo "copy validation/configuration.json.template -> $cfg first" >&2
    return 1
  fi
  python -c "
import json
p = r'$cfg'
c = json.load(open(p))
c['tmode'] = '$tmode'
c['use.fastload'] = '$fl'
json.dump(c, open(p, 'w'), indent=4)
" || { echo "python patch_config failed (need python on PATH)" >&2; return 1; }
  echo "  configuration.json -> tmode=$tmode  use.fastload=$fl"
}

run_tester() {
  local json="$1"
  local testlog="$2"

  say "Tester: $json"
  MSYS_NO_PATHCONV=1 docker run --rm \
    --memory=12g \
    -e "JAVA_TOOL_OPTIONS=-Xmx8g -Xms1g" \
    --mount "type=bind,source=${DATA_FOLDER_WIN},target=/data" \
    -e "WORKING_DIR=${DATA_FOLDER_WIN}" \
    -e "GRPC_HOSTNAME=host.docker.internal" \
    --network=host \
    "$TESTER_IMAGE" \
    --tester-type destination --port 50052 \
    --input-file "$json" --disable-operation-delay \
    2>&1 | tee "$testlog"
  return ${PIPESTATUS[0]}
}

check_logs_clean() {
  local testlog="$1"
  local connector_tail="$2"
  local bad=0

  if grep -qE 'ERROR|Exception|FAILED|SEVERE' "$testlog"; then
    echo "  [!] tester log contains ERROR/Exception — see $testlog"
    bad=1
  fi
  if grep -qE '"level":"SEVERE"|"level":"ERROR"|SQLException|\[Error [0-9]+\]' "$connector_tail"; then
    echo "  [!] connector log contains SEVERE/SQLException/Error nnnn — see $connector_tail"
    bad=1
  fi
  return $bad
}

run_validation() {
  local json="$1"
  local basename="${json%.json}"
  local sqlfile="$VALIDATION_DIR/validate-${basename}.sql"
  local vlog="$LOG_DIR/validate-${basename}.log"

  if [[ ! -f "$sqlfile" ]]; then
    echo "  (no validation SQL at $sqlfile — skipping deep check)"
    return 0
  fi

  run_bteq "$(cat "$sqlfile")" > "$vlog" 2>&1
  if grep -qE '^FAIL:|\*\*\* Failure' "$vlog"; then
    echo "  [!] validation assertions failed — see $vlog"
    grep -E '^FAIL:|PASS:|\*\*\* Failure' "$vlog" | head -30
    return 1
  fi
  local ok_count
  ok_count=$(grep -cE '^PASS:' "$vlog" || true)
  echo "  validation: $ok_count checks passed"
  return 0
}

run_one() {
  local json="$1"
  local basename="${json%.json}"
  local testlog="$LOG_DIR/tester-${basename}.log"
  local connector_tail="$LOG_DIR/connector-tail-${basename}.log"

  local conn_lines_before
  conn_lines_before=$(wc -l < "$CONNECTOR_LOG")

  say "RESET → $json"
  drop_known_tables

  local rc=0
  run_tester "$json" "$testlog" || rc=$?

  tail -n "+$((conn_lines_before + 1))" "$CONNECTOR_LOG" > "$connector_tail"

  local ok=0
  [[ $rc -ne 0 ]] && { echo "  [!] tester exited $rc"; ok=1; }
  check_logs_clean "$testlog" "$connector_tail" || ok=1
  run_validation "$json" || ok=1

  if [[ $ok -eq 0 ]]; then
    ((pass++)); results+=("PASS  $json")
  else
    ((fail++)); results+=("FAIL  $json")
  fi
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

if ! $SKIP_BUILD; then
  say "Build jar"
  ./gradlew.bat jar || { echo "build failed" >&2; exit 1; }
fi

[[ ! -d "$DATA_FOLDER" ]] && { echo "DATA_FOLDER not found: $DATA_FOLDER" >&2; exit 1; }

if [[ -n "$SINGLE_JSON" ]]; then
  jsons=("$SINGLE_JSON")
else
  mapfile -t jsons < <(cd "$DATA_FOLDER" && ls *.json 2>/dev/null | grep -v '^configuration\.json$' | sort)
fi
[[ ${#jsons[@]} -eq 0 ]] && { echo "no input JSON files found in $DATA_FOLDER" >&2; exit 1; }

run_combo() {
  local tmode="$1" fl="$2"
  local tag="${tmode}-fl${fl}"
  say "### COMBO  tmode=$tmode  use.fastload=$fl ###"

  patch_config "$tmode" "$fl" || return 1
  start_connector "$tag"

  # per-combo counters
  pass=0; fail=0; results=()

  for j in "${jsons[@]}"; do
    run_one "$j"
  done

  say "Combo summary  tmode=$tmode  use.fastload=$fl"
  printf '  %s\n' "${results[@]}"
  echo "  passed=$pass  failed=$fail"

  matrix_pass=$((matrix_pass + pass))
  matrix_fail=$((matrix_fail + fail))
  matrix_results+=("$(printf '  %-6s fl=%-5s  passed=%d failed=%d' "$tmode" "$fl" "$pass" "$fail")")

  stop_connector
}

matrix_pass=0
matrix_fail=0
declare -a matrix_results

if $MATRIX; then
  for combo in "TERA false" "TERA true" "ANSI false" "ANSI true"; do
    # shellcheck disable=SC2086
    run_combo $combo
  done
  say "MATRIX SUMMARY"
  printf '%s\n' "${matrix_results[@]}"
  echo "  total  passed=$matrix_pass  failed=$matrix_fail"
  exit $matrix_fail
fi

# Single run (optional tmode/fastload patch)
if [[ -n "$OVERRIDE_TMODE" || -n "$OVERRIDE_FASTLOAD" ]]; then
  local_tmode="${OVERRIDE_TMODE:-TERA}"
  local_fl="${OVERRIDE_FASTLOAD:-false}"
  run_combo "$local_tmode" "$local_fl"
  say "Total  passed=$matrix_pass  failed=$matrix_fail"
  exit $matrix_fail
fi

# Fall-through: use configuration.json as-is, single connector run
start_connector
pass=0; fail=0; results=()
for j in "${jsons[@]}"; do
  run_one "$j"
done
say "Summary"
printf '  %s\n' "${results[@]}"
echo "  passed=$pass  failed=$fail"
exit $fail
