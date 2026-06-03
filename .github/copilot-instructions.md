# Copilot Instructions — Fivetran Teradata Destination Connector

## Build & Test

```bash
# Build fat JAR
gradle jar

# Run all tests (requires live Teradata instance)
gradle test

# Run a single test class
gradle test --tests "com.teradata.fivetran.destination.CreateTableTest"

# Run a single test method
gradle test --tests "com.teradata.fivetran.destination.CreateTableTest.someMethodName"

# Run the connector locally (gRPC server on port 50052)
java -jar build/libs/TeradataDestination.jar

# Enable debug logging
java -Ddebuglog=yes -jar build/libs/TeradataDestination.jar
```

## Required Environment Variables for Tests

Tests are integration tests that connect to a real Teradata instance:

- `TERADATA_HOST`
- `TERADATA_USER`
- `TERADATA_PASSWORD`
- `TERADATA_DATABASE`
- `TERADATA_SCHEMA`
- `TERADATA_LOGMECH` (TD2, LDAP, or BROWSER)
- `TERADATA_TMODE` (ANSI, TERA, or DEFAULT)

## Architecture

This is a **Fivetran SDK v2 destination connector** implemented as a gRPC server. Fivetran's sync engine acts as the gRPC client; this connector is the server.

### Core Flow

1. **`TeradataDestination`** — Entry point. Starts a gRPC server (default port 50052).
2. **`TeradataDestinationServiceImpl`** — Implements the `DestinationConnector` gRPC service defined in `destination_sdk.proto`. Handles all SDK operations: `ConfigurationForm`, `Test`, `DescribeTable`, `CreateTable`, `AlterTable`, `Truncate`, `WriteBatch`, `WriteHistoryBatch`, `Migrate`.
3. **`TeradataJDBCUtil`** — Shared JDBC utility layer for connection management, SQL generation, DDL operations, and type mapping.
4. **`TeradataConfiguration`** — Parses the Fivetran configuration map into typed connection properties.

### Writer Hierarchy

All writers extend the abstract `Writer` class which handles file decryption (AES), decompression (ZSTD/GZIP), and CSV parsing. Subclasses implement the actual SQL operations:

- **`LoadDataWriter`** — Inserts new rows via batch `PreparedStatement` through a temp table pattern.
- **`FastLoadDataWriter`** / **`FastLoad`** / **`FastLoadThread`** — High-performance parallel loading using Teradata FastLoad protocol (beta). Bypasses standard SQL for empty tables.
- **`UpdateWriter`** — Updates existing rows.
- **`DeleteWriter`** — Deletes rows by primary key.
- **`UpdateHistoryWriter`** / **`DeleteHistoryWriter`** / **`EarliestStartHistoryWriter`** — Handle history-mode (SCD Type 2) write operations.

### Proto / Generated Code

Proto files in `src/main/proto/` are from [fivetran/fivetran_sdk](https://github.com/fivetran/fivetran_sdk). Generated Java sources go to `build/generated/source/proto/main/{grpc,java}` and are referenced via `sourceSets` in `build.gradle`. The package is `fivetran_sdk.v2`.

### Warning Handlers

The `warning_util` package contains operation-specific `WarningHandler` subclasses that propagate non-fatal issues back to Fivetran through gRPC response observers (e.g., `AlterTableWarningHandler`, `WriteBatchWarningHandler`).

## Key Conventions

- **Logging**: Use `Logger.logMessage(Logger.LogLevel.INFO, msg)` from the custom `Logger` class for structured JSON logging to stdout. Debug logging is gated by the `-Ddebuglog=yes` system property. Do not use `System.out.println` directly.
- **SQL identifiers**: Always escape table names via `TeradataJDBCUtil.escapeTable(database, table)`. Schema/table naming follows pattern `{schemaName}_{tableName}`.
- **Input validation**: Host values are validated against an allow-list pattern; driver parameters are sanitized to prevent injection. Batch file paths are canonicalized and verified before opening.
- **Configuration keys**: Use dot-notation (`ssl.mode`, `default.varchar.size`, `use.fastload`, `batch.size`, `query.band`). Mapped through `TeradataConfiguration`.
- **Batch processing**: Rows are committed in batches controlled by configurable `batch.size` (default 10000).
- **Transaction mode awareness**: DDL in ANSI mode requires explicit commits. The connector handles this in `alterTable` by disabling auto-commit and committing per query.
- **Test structure**: All integration tests extend `IntegrationTestBase` which provides connection setup, table creation helpers, and result verification utilities.
