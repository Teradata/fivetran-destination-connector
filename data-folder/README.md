# Fivetran SDK Destination Tester - Test Data

This directory contains JSON input files for the [Fivetran SDK Destination Tester](https://github.com/fivetran/fivetran_partner_sdk/tree/main/tools/destination-connector-tester).

## Test Files

| File | Category | Operations Tested |
|------|----------|-------------------|
| `01_basic_operations.json` | Basic CRUD | All 15 data types, upsert, truncate, soft_truncate, update, delete, soft_delete, alter_table, history mode, composite PKs |
| `02_schema_migrations_ddl.json` | Schema DDL | add_column, change_column_data_type, drop_column |
| `03_schema_migrations_dml.json` | Schema DML | copy_column, update_column_value, add_column_with_default_value, set_column_to_null, copy_table, rename_column, rename_table, drop_table |
| `04_schema_migrations_sync_modes.json` | Sync Modes | add/drop column in history mode, copy_table_to_history_mode, migrate_soft_delete_to_history, migrate_history_to_soft_delete |
| `05_operations_on_nonexistent_records/` | Edge Cases | 7 files testing delete/update/soft_delete on missing records, truncate on missing tables |

## How to Run

### Prerequisites
- Docker Desktop >= 4.23.0 or Rancher Desktop >= 1.12.1
- JDK 17, Gradle 8
- Live Teradata instance accessible from Docker
- Environment variables: `TERADATA_HOST`, `TERADATA_USER`, `TERADATA_PASSWORD`, `TERADATA_DATABASE`, `TERADATA_SCHEMA`, `TERADATA_LOGMECH`, `TERADATA_TMODE`

### Step 1: Build and start the connector
```bash
gradle jar
java -jar build/libs/TeradataDestination.jar
```

### Step 2: Pull the SDK tester Docker image
```bash
gcloud auth configure-docker us-docker.pkg.dev
docker pull us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<version>
```

### Step 3: Run a test file
```bash
docker run \
  --mount type=bind,source=$(pwd)/data-folder,target=/data \
  -a STDIN -a STDOUT -a STDERR -it \
  -e WORKING_DIR=$(pwd)/data-folder \
  -e GRPC_HOSTNAME=host.docker.internal \
  --network=host \
  us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<version> \
  --tester-type destination --port 50052 \
  --input-file 01_basic_operations.json --plain-text
```

To run all test files (alphabetical order):
```bash
docker run \
  --mount type=bind,source=$(pwd)/data-folder,target=/data \
  -a STDIN -a STDOUT -a STDERR -it \
  -e WORKING_DIR=$(pwd)/data-folder \
  -e GRPC_HOSTNAME=host.docker.internal \
  --network=host \
  us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<version> \
  --tester-type destination --port 50052 --plain-text
```

### Step 4: Run Java validation tests
After the SDK tester completes, verify database state:
```bash
gradle test --tests "com.teradata.fivetran.destination.tester.*"
```

## CLI Options

| Option | Description |
|--------|-------------|
| `--port <N>` | gRPC port (default: 50052) |
| `--plain-text` | Disable encryption/compression for debugging |
| `--input-file <file>` | Run a single test file instead of all |
| `--schema-name <name>` | Custom schema (default: "tester") |
| `--disable-operation-delay` | Remove simulated delays |
| `--batch-file-type CSV\|PARQUET` | Batch file format (default: CSV) |
