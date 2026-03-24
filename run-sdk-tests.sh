#!/bin/bash
# Fivetran SDK Destination Tester - Full Test Runner
#
# Usage: ./run-sdk-tests.sh <sdk-tester-version>
# Example: ./run-sdk-tests.sh 1.0.0
#
# Prerequisites:
# - Docker running
# - Teradata instance accessible
# - Environment variables set: TERADATA_HOST, TERADATA_USER, TERADATA_PASSWORD, etc.
# - gcloud auth configured: gcloud auth configure-docker us-docker.pkg.dev

set -e

VERSION=${1:?"Usage: $0 <sdk-tester-version>"}
IMAGE="us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:${VERSION}"
DATA_FOLDER="$(cd "$(dirname "$0")" && pwd)/data-folder"
PORT=50052

echo "=== Building connector ==="
gradle jar

echo "=== Starting connector in background ==="
java -jar build/libs/TeradataDestination.jar &
CONNECTOR_PID=$!
sleep 5

cleanup() {
    echo "=== Stopping connector ==="
    kill $CONNECTOR_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Running SDK tester (all test files) ==="
docker run \
    --mount type=bind,source="${DATA_FOLDER}",target=/data \
    -a STDIN -a STDOUT -a STDERR -it \
    -e WORKING_DIR="${DATA_FOLDER}" \
    -e GRPC_HOSTNAME=host.docker.internal \
    --network=host \
    "${IMAGE}" \
    --tester-type destination --port ${PORT} --plain-text --disable-operation-delay

echo "=== SDK tester completed ==="

echo "=== Running Java validation tests ==="
gradle test --tests "com.teradata.fivetran.destination.tester.*"

echo "=== All tests passed ==="
