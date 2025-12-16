package com.teradata.fivetran.destination;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MigrateTest extends IntegrationTestBase {
    @Test
    public void dropTable() throws Exception {
        String tableName = IntegrationTestBase.schema + "_" + "dropTable";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(),tableName));
            } catch (Exception e) {
                // handle or ignore
            }
            System.out.println("Creating table: " + TeradataJDBCUtil.escapeTable(conf.database(),tableName));
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + "(a INT)");
            System.out.println("Created table: " + TeradataJDBCUtil.escapeTable(conf.database(),tableName));
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("dropTable")
                            .setSchema(IntegrationTestBase.schema)
                            .setDrop(
                                    DropOperation.newBuilder()
                                            .setDropTable(true)
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            Assertions.assertThrows(TableNotExistException.class, () -> {
                TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            });
        }
    }

    @Test
    public void renameTable() throws Exception {
        String tableName = IntegrationTestBase.schema + "_" + "renameTable";
        String toTableName = IntegrationTestBase.schema + "_" + "renameTable1";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {

            // Drop both tables if exist (from previous runs)
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception e) {
                // ignore
            }
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), toTableName));
            } catch (Exception e) {
                // ignore
            }

            // Create source table
            System.out.println("Creating table: " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(a INT)");
            System.out.println("Created table: " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));

            // Build rename request
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("renameTable")  // logical table name
                            .setSchema(IntegrationTestBase.schema)
                            .setRename(
                                    RenameOperation.newBuilder()
                                            .setRenameTable(
                                                    RenameTable.newBuilder()
                                                            .setFromTable("renameTable")
                                                            .setToTable("renameTable1")
                                            )
                            ))
                    .build();

            // Generate and execute SQL statements
            List<TeradataJDBCUtil.QueryWithCleanup> queries =
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            // Validate renamed table
            Table renamed = TeradataJDBCUtil.getTable(
                    conf,
                    database,
                    toTableName,
                    toTableName,
                    testWarningHandle
            );

            Assertions.assertEquals(toTableName, renamed.getName());
        }
    }

    @Test
    public void renameColumn() throws Exception {
        String tableName = IntegrationTestBase.schema + "_" + "renameColumn";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {

            // Drop table if it exists
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception e) {
                // ignore
            }

            // Create initial table
            System.out.println("Creating table: " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " (a INT, b INT)");
            System.out.println("Created table: " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));

            // Build migration request
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("renameColumn")
                            .setSchema(IntegrationTestBase.schema)
                            .setRename(
                                    RenameOperation.newBuilder()
                                            .setRenameColumn(
                                                    RenameColumn.newBuilder()
                                                            .setFromColumn("b")
                                                            .setToColumn("c")
                                            )
                            ))
                    .build();

            // Generate and execute SQL statements
            List<TeradataJDBCUtil.QueryWithCleanup> queries =
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            // Validate column was renamed
            Table renamed = TeradataJDBCUtil.getTable(
                    conf,
                    database,
                    tableName,
                    tableName,
                    testWarningHandle
            );

            Assertions.assertEquals("a", renamed.getColumns(0).getName());
            Assertions.assertEquals("c", renamed.getColumns(1).getName());
        }
    }

    @Test
    public void copyTable() throws Exception {
        String fromTableName = IntegrationTestBase.schema + "_" + "copyTable";
        String toTableName   = IntegrationTestBase.schema + "_" + "copyTable1";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {

            // Drop tables from previous runs
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), fromTableName));
            } catch (Exception e) { }

            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), toTableName));
            } catch (Exception e) { }

            // Create original table
            System.out.println("Creating table: " + TeradataJDBCUtil.escapeTable(conf.database(), fromTableName));
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), fromTableName) + " (a INT)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), fromTableName) + " VALUES (1)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), fromTableName) + " VALUES (2)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), fromTableName) + " VALUES (3)");

            // Build migration request
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyTable")
                            .setSchema(IntegrationTestBase.schema)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyTable(
                                                    CopyTable.newBuilder()
                                                            .setFromTable("copyTable")
                                                            .setToTable("copyTable1")
                                            )
                            )
                    )
                    .build();

            // Execute generated queries
            List<TeradataJDBCUtil.QueryWithCleanup> queries =
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            // Validate original table still exists
            Table originalTable = TeradataJDBCUtil.getTable(
                    conf, database, fromTableName, fromTableName, testWarningHandle
            );
            Assertions.assertEquals(fromTableName, originalTable.getName());

            // Validate copied table exists
            Table copy = TeradataJDBCUtil.getTable(
                    conf, database, toTableName, toTableName, testWarningHandle
            );
            Assertions.assertEquals(toTableName, copy.getName());

            // Validate copied data
            checkResult(
                    "SELECT a FROM " + TeradataJDBCUtil.escapeTable(conf.database(), toTableName) + " ORDER BY a",
                    Arrays.asList(
                            Collections.singletonList("1"),
                            Collections.singletonList("2"),
                            Collections.singletonList("3")
                    )
            );
        }
    }

    @Test
    public void copyColumn() throws Exception {
        String tableName = IntegrationTestBase.schema + "_" + "copyColumn";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {

            // Drop table if exists
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception e) { }

            // Create table
            System.out.println("Creating table: " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " (a DECIMAL(8, 4))");

            // Insert test data
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES (123)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES (124)");

            // Build migrate request
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyColumn")
                            .setSchema(IntegrationTestBase.schema)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyColumn(
                                                    CopyColumn.newBuilder()
                                                            .setFromColumn("a")
                                                            .setToColumn("b")
                                            )
                            )
                    )
                    .build();

            // Execute migration queries
            List<TeradataJDBCUtil.QueryWithCleanup> queries =
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            // Validate table structure
            Table copy = TeradataJDBCUtil.getTable(
                    conf, database, tableName, tableName, testWarningHandle
            );

            List<Column> columns = copy.getColumnsList();

            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("b", columns.get(1).getName());
            Assertions.assertEquals(8, columns.get(1).getParams().getDecimal().getPrecision());
            Assertions.assertEquals(4, columns.get(1).getParams().getDecimal().getScale());

            // Validate copied data
            checkResult(
                    "SELECT a, b FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " ORDER BY a",
                    Arrays.asList(
                            Arrays.asList("123.0000", "123.0000"),
                            Arrays.asList("124.0000", "124.0000")
                    )
            );
        }
    }

    @Test
    public void addColumnWithDefaultValue() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "addColumnWithDefaultValue";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {

            // Drop table if exists
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception e) {}

            // Create initial table
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                            " (a INT)"
            );

            // -----------------------------
            // ADD COLUMN b INT DEFAULT 1
            // -----------------------------
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(
                            MigrationDetails.newBuilder()
                                    .setTable("addColumnWithDefaultValue")
                                    .setSchema(IntegrationTestBase.schema)
                                    .setAdd(
                                            AddOperation.newBuilder()
                                                    .setAddColumnWithDefaultValue(
                                                            AddColumnWithDefaultValue.newBuilder()
                                                                    .setColumn("b")
                                                                    .setDefaultValue("1")
                                                                    .setColumnType(DataType.INT)
                                                                    .build()
                                                    )
                                    )
                    )
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries =
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            // Validate column b
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            Optional<Column> optionalB = t.getColumnsList().stream()
                    .filter(c -> c.getName().equals("b"))
                    .findFirst();

            Assertions.assertTrue(optionalB.isPresent());
            Column b = optionalB.get();
            Assertions.assertEquals("b", b.getName());
            Assertions.assertEquals(DataType.INT, b.getType());

            // -----------------------------
            // ADD COLUMN c TIMESTAMP DEFAULT <date>
            // -----------------------------

            request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(
                            MigrationDetails.newBuilder()
                                    .setTable("addColumnWithDefaultValue")
                                    .setSchema(IntegrationTestBase.schema)
                                    .setAdd(
                                            AddOperation.newBuilder()
                                                    .setAddColumnWithDefaultValue(
                                                            AddColumnWithDefaultValue.newBuilder()
                                                                    .setColumn("c")
                                                                    .setDefaultValue("2025-11-24T10:14:54.123Z")
                                                                    .setColumnType(DataType.NAIVE_DATETIME)
                                                                    .build()
                                                    )
                                    )
                    )
                    .build();

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                q.execute(conn);
            }

            // Validate column c
            t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            Optional<Column> optionalC = t.getColumnsList().stream()
                    .filter(c -> c.getName().equals("c"))
                    .findFirst();

            Assertions.assertTrue(optionalC.isPresent());
            Column c = optionalC.get();
            Assertions.assertEquals("c", c.getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, c.getType());
        }
    }

    @Test
    public void updateColumnValueOperation() throws Exception {
        String tableName = IntegrationTestBase.schema + "_" + "updateColumnValueOperation";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {

            // Drop table if exists
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception e) {
                // ignore if not exists
            }

            // Create table
            stmt.execute(
                    "CREATE MULTISET TABLE " +
                            TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                            " (a INT)"
            );

            // Insert data
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES (1)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES (2)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES (3)");

            // Build migration request
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("updateColumnValueOperation")
                            .setSchema(IntegrationTestBase.schema)
                            .setUpdateColumnValue(
                                    UpdateColumnValueOperation.newBuilder()
                                            .setColumn("a")
                                            .setValue("4")
                                            .build()
                            )
                    )
                    .build();

            // Execute migrations
            List<TeradataJDBCUtil.QueryWithCleanup> queries =
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            // Validate result
            checkResult(
                    "SELECT * FROM " +
                            TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                            " ORDER BY a",
                    Arrays.asList(
                            Collections.singletonList("4"),
                            Collections.singletonList("4"),
                            Collections.singletonList("4")
                    )
            );
        }
    }

    @Test
    public void liveToSoftDelete() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "liveToSoftDelete";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " (a INT)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES (1)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES (2)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("liveToSoftDelete")
                            .setSchema(IntegrationTestBase.schema)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.LIVE_TO_SOFT_DELETE)
                                            .setSoftDeletedColumn("b")
                            ))
                    .build();

            for (TeradataJDBCUtil.QueryWithCleanup q :
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle)) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("b", columns.get(1).getName());

            checkResult(
                    "SELECT a, b FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " ORDER BY a",
                    Arrays.asList(
                            Arrays.asList("1", "0"),
                            Arrays.asList("2", "0")
                    )
            );
        }
    }

    @Test
    public void liveToHistory() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "liveToHistory";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                    + " (a INT PRIMARY KEY NOT NULL)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES (1)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES (2)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("liveToHistory")
                            .setSchema(IntegrationTestBase.schema)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.LIVE_TO_HISTORY)
                            ))
                    .build();

            for (TeradataJDBCUtil.QueryWithCleanup q :
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle)) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            List<Column> columns = t.getColumnsList();

            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(1).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(3).getName());

            checkResult(
                    "SELECT a, _fivetran_end, _fivetran_active FROM "
                            + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " ORDER BY a",
                    Arrays.asList(
                            Arrays.asList("1", "9999-12-31 23:59:59.999999", "1"),
                            Arrays.asList("2", "9999-12-31 23:59:59.999999", "1")
                    )
            );
        }
    }

    @Test
    public void softDeleteToHistory() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "softDeleteToHistory";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            // Drop table if exists
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            // Create table
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT PRIMARY KEY NOT NULL, _fivetran_synced TIMESTAMP(6), _fivetran_deleted BYTEINT)"
            );

            // Insert data
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, '2020-01-01 01:01:01', 0)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (2, '2020-01-01 01:01:01', 1)"
            );

            // Build migrate request
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("softDeleteToHistory")  // logical name
                            .setSchema(IntegrationTestBase.schema)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.SOFT_DELETE_TO_HISTORY)
                                            .setSoftDeletedColumn("_fivetran_deleted")
                            ))
                    .build();

            // Execute migration queries
            for (TeradataJDBCUtil.QueryWithCleanup q :
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle)) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            // Fetch metadata
            Table t = TeradataJDBCUtil.getTable(
                    conf, conf.database(), tableName, tableName, testWarningHandle
            );

            List<Column> columns = t.getColumnsList();

            // Assertions
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(3).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(4).getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(2).getType());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(3).getType());
            Assertions.assertEquals(DataType.BOOLEAN, columns.get(4).getType());
            Assertions.assertTrue(columns.get(2).getPrimaryKey());

            // Check results
            checkResult(
                    "SELECT a, _fivetran_start, _fivetran_end, _fivetran_active FROM "
                            + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " ORDER BY a",
                    Arrays.asList(
                            Arrays.asList("1", "2020-01-01 01:01:01.0", "9999-12-31 23:59:59.999999", "1"),
                            Arrays.asList("2", "1000-01-01 00:00:00.0", "1000-01-01 00:00:00.0", "0")
                    )
            );
        }
    }

    @Test
    public void softDeleteToLive() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "softDeleteToLive";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            // DROP TABLE IF EXISTS
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            // CREATE TABLE
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT PRIMARY KEY NOT NULL, _fivetran_deleted BYTEINT)"
            );

            // INSERT DATA (separate statements — matching your standard)
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, 0)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (2, 1)"
            );

            // BUILD REQUEST
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("softDeleteToLive")  // logical name
                            .setSchema(IntegrationTestBase.schema)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.SOFT_DELETE_TO_LIVE)
                                            .setSoftDeletedColumn("_fivetran_deleted")
                            ))
                    .build();

            // EXECUTE MIGRATION QUERIES
            for (TeradataJDBCUtil.QueryWithCleanup q :
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle)) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            // FETCH METADATA
            Table t = TeradataJDBCUtil.getTable(
                    conf,
                    conf.database(),
                    tableName,
                    tableName,
                    testWarningHandle
            );

            List<Column> columns = t.getColumnsList();

            // ASSERTIONS
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals(1, columns.size());

            // RESULT CHECK
            checkResult(
                    "SELECT a FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " ORDER BY a",
                    Collections.singletonList(
                            Collections.singletonList("1")
                    )
            );
        }
    }

    @Test
    public void historyToSoftDelete() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "historyToSoftDelete";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            //
            // FIRST RUN
            //

            // DROP IF EXISTS
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            // CREATE TABLE
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT NOT NULL, _fivetran_start TIMESTAMP(6) NOT NULL, "
                            + "_fivetran_end TIMESTAMP(6), _fivetran_active BYTEINT, "
                            + "PRIMARY KEY(_fivetran_start, a))"
            );

            // INSERT DATA
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, '2021-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (3, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0)"
            );

            // MIGRATION REQUEST
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("historyToSoftDelete")
                            .setSchema(IntegrationTestBase.schema)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.HISTORY_TO_SOFT_DELETE)
                                            .setSoftDeletedColumn("_fivetran_deleted")
                            ))
                    .build();
            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            // EXECUTE MIGRATION
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            // FETCH NEW TABLE METADATA
            Table t = TeradataJDBCUtil.getTable(
                    conf, conf.database(), tableName, tableName, testWarningHandle
            );

            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_deleted", columns.get(1).getName());
            Assertions.assertEquals(2, columns.size());

            // VALIDATE RESULTS
            checkResult(
                    "SELECT a, _fivetran_deleted FROM "
                            + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " ORDER BY a",
                    Arrays.asList(
                            Arrays.asList("1", "0"),
                            Arrays.asList("2", "0"),
                            Arrays.asList("3", "1")
                    )
            );

            //
            // SECOND RUN (different PK definition)
            //

            // DROP OLD TABLE
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            // CREATE TABLE AGAIN (different PK structure)
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT NOT NULL, _fivetran_start TIMESTAMP(6) NOT NULL, "
                            + "_fivetran_end TIMESTAMP(6), _fivetran_active BYTEINT, "
                            + "PRIMARY KEY(_fivetran_start))"
            );

            // INSERT DATA
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, '2021-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (2, '2022-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (3, '2023-01-01 01:01:01', '2021-01-01 01:01:01', 0)"
            );

            // GENERATE MIGRATION AGAIN
            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            // FETCH METADATA AGAIN
            t = TeradataJDBCUtil.getTable(
                    conf, conf.database(), tableName, tableName, testWarningHandle
            );

            columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_deleted", columns.get(1).getName());
            Assertions.assertEquals(2, columns.size());

            // VALIDATE RESULTS
            checkResult(
                    "SELECT a, _fivetran_deleted FROM "
                            + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " ORDER BY a, _fivetran_deleted",
                    Arrays.asList(
                            Arrays.asList("1", "0"),
                            Arrays.asList("1", "1"),
                            Arrays.asList("2", "0"),
                            Arrays.asList("3", "1")
                    )
            );
        }
    }

    @Test
    public void historyToLive() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "historyToLive";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            //
            // FIRST RUN
            //

            // DROP IF EXISTS
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            // CREATE TABLE
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT NOT NULL, "
                            + "_fivetran_start TIMESTAMP(6), "
                            + "_fivetran_end TIMESTAMP(6), "
                            + "_fivetran_active BYTEINT, "
                            + "PRIMARY KEY(a))"
            );

            // INSERT DATA
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)"
            );

            // MIGRATION REQUEST
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("historyToLive")
                            .setSchema(IntegrationTestBase.schema)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.HISTORY_TO_LIVE)
                                            .setKeepDeletedRows(true)
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries =
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            // EXECUTE MIGRATION
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            // FETCH TABLE METADATA
            Table t = TeradataJDBCUtil.getTable(
                    conf, conf.database(), tableName, tableName, testWarningHandle
            );

            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals(1, columns.size());

            // VALIDATE RESULTS
            checkResult(
                    "SELECT a FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " ORDER BY a",
                    Arrays.asList(
                            Collections.singletonList("1"),
                            Collections.singletonList("2")
                    )
            );

            //
            // SECOND RUN (keepDeletedRows = false)
            //

            // DROP THE TABLE
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            // CREATE AGAIN
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT NOT NULL, "
                            + "_fivetran_start TIMESTAMP(6), "
                            + "_fivetran_end TIMESTAMP(6), "
                            + "_fivetran_active BYTEINT, "
                            + "PRIMARY KEY(a))"
            );

            // INSERT DATA AGAIN
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)"
            );

            // SECOND MIGRATION REQUEST
            request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("historyToLive")
                            .setSchema(IntegrationTestBase.schema)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.HISTORY_TO_LIVE)
                                            .setKeepDeletedRows(false)
                            ))
                    .build();

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            // FETCH METADATA AGAIN
            t = TeradataJDBCUtil.getTable(
                    conf, conf.database(), tableName, tableName, testWarningHandle
            );

            columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals(1, columns.size());

            // EXPECT ONLY ROW WHERE active=1
            checkResult(
                    "SELECT a FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " ORDER BY a",
                    Collections.singletonList(
                            Collections.singletonList("2")
                    )
            );
        }
    }

    @Test
    public void copyTableToHistoryMode() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "copyTableToHistoryMode";
        String newTableName = tableName + "New";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            //
            // FIRST RUN — WITH _fivetran_deleted column
            //

            // DROP IF EXISTS
            try { stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)); } catch (Exception ignored) {}
            try { stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), newTableName)); } catch (Exception ignored) {}

            // CREATE SOURCE TABLE
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT NOT NULL PRIMARY KEY, _fivetran_synced TIMESTAMP(6), _fivetran_deleted BYTEINT)"
            );

            // INSERT DATA
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                    + " VALUES (1, '2020-01-01 01:01:01', 0)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                    + " VALUES (2, '2020-01-01 01:01:01', 1)");

            // BUILD MIGRATION REQUEST
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyTableToHistoryMode")
                            .setSchema(IntegrationTestBase.schema)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyTableToHistoryMode(
                                                    CopyTableToHistoryMode.newBuilder()
                                                            .setFromTable("copyTableToHistoryMode")
                                                            .setToTable("copyTableToHistoryModeNew")
                                                            .setSoftDeletedColumn("_fivetran_deleted")
                                            )
                            ))
                    .build();

            // EXECUTE MIGRATION
            for (TeradataJDBCUtil.QueryWithCleanup q :
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle)) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            // VALIDATE RESULTING TABLE STRUCTURE
            Table t = TeradataJDBCUtil.getTable(
                    conf, conf.database(), newTableName, newTableName, testWarningHandle);

            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(3).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(4).getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(2).getType());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(3).getType());
            Assertions.assertEquals(DataType.BOOLEAN, columns.get(4).getType());
            Assertions.assertTrue(columns.get(2).getPrimaryKey());

            // VALIDATE ROWS
            checkResult(
                    "SELECT a, _fivetran_start, _fivetran_end, _fivetran_active FROM "
                            + TeradataJDBCUtil.escapeTable(conf.database(), newTableName)
                            + " ORDER BY a",
                    Arrays.asList(
                            Arrays.asList("1", "2020-01-01 01:01:01.0", "9999-12-31 23:59:59.999999", "1"),
                            Arrays.asList("2", "1000-01-01 00:00:00.0", "1000-01-01 00:00:00.0", "0")
                    )
            );

            //
            // SECOND RUN — WITHOUT soft delete column
            //

            // CLEANUP
            try { stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)); } catch (Exception ignored) {}
            try { stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), newTableName)); } catch (Exception ignored) {}

            // CREATE SOURCE TABLE WITHOUT _fivetran_deleted
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT NOT NULL PRIMARY KEY)"
            );

            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (2)"
            );

            // NEW REQUEST
            request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyTableToHistoryMode")
                            .setSchema(IntegrationTestBase.schema)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyTableToHistoryMode(
                                                    CopyTableToHistoryMode.newBuilder()
                                                            .setFromTable("copyTableToHistoryMode")
                                                            .setToTable("copyTableToHistoryModeNew")
                                            )
                            ))
                    .build();

            // EXECUTE MIGRATION
            for (TeradataJDBCUtil.QueryWithCleanup q :
                    TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle)) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            // VALIDATE NEW TABLE STRUCTURE
            t = TeradataJDBCUtil.getTable(conf, conf.database(), newTableName, newTableName, testWarningHandle);
            columns = t.getColumnsList();

            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(1).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(3).getName());
            Assertions.assertTrue(columns.get(1).getPrimaryKey());

            // VALIDATE ROWS
            checkResult(
                    "SELECT a, _fivetran_end, _fivetran_active FROM "
                            + TeradataJDBCUtil.escapeTable(conf.database(), newTableName)
                            + " ORDER BY a",
                    Arrays.asList(
                            Arrays.asList("1", "9999-12-31 23:59:59.999999", "1"),
                            Arrays.asList("2", "9999-12-31 23:59:59.999999", "1")
                    )
            );
        }
    }

    @Test
    public void dropColumnInHistoryMode() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "dropColumnInHistoryMode";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            //
            // FIRST RUN — EMPTY OPERATION SHOULD PRODUCE NO QUERIES
            //

            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT NOT NULL, b INT, _fivetran_start TIMESTAMP(6) NOT NULL, _fivetran_end TIMESTAMP(6), "
                            + "_fivetran_active BYTEINT, PRIMARY KEY(_fivetran_start, a))"
            );

            // EMPTY REQUEST → MUST GENERATE ZERO QUERIES
            final MigrateRequest emptyRequest = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("dropColumnInHistoryMode")
                            .setSchema(IntegrationTestBase.schema)
                            .setDrop(
                                    DropOperation.newBuilder()
                                            .setDropColumnInHistoryMode(
                                                    DropColumnInHistoryMode.newBuilder()
                                                            .setColumn("b")
                                                            .setOperationTimestamp("2021-01-01 01:01:01")
                                            )
                            )
                    ).build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries =
                    TeradataJDBCUtil.generateMigrateQueries(emptyRequest, testWarningHandle);

            Assertions.assertEquals(0, queries.size());

            //
            // INSERT TEST DATA
            //

            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, 1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0)"
            );
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (2, 2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)"
            );

            //
            // VALID RUN
            //

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("dropColumnInHistoryMode")
                            .setSchema(IntegrationTestBase.schema)
                            .setDrop(
                                    DropOperation.newBuilder()
                                            .setDropColumnInHistoryMode(
                                                    DropColumnInHistoryMode.newBuilder()
                                                            .setColumn("b")
                                                            .setOperationTimestamp("2021-01-01 01:01:01")
                                            )
                            )
                    ).build();

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            //
            // VERIFY METADATA
            //

            Table t = TeradataJDBCUtil.getTable(
                    conf,
                    conf.database(),
                    tableName,
                    tableName,
                    testWarningHandle
            );

            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("b", columns.get(1).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(3).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(4).getName());
            Assertions.assertEquals(5, columns.size());

            //
            // VERIFY RESULT DATA
            //

            checkResult(
                    "SELECT * FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " ORDER BY _fivetran_start, a",
                    Arrays.asList(
                            Arrays.asList("1", "1", "2020-01-01 01:01:01.0", "2021-01-01 01:01:01.0", "0"),
                            Arrays.asList("2", "2", "2020-01-01 01:01:01.0", "2021-01-01 01:01:00.999999", "0"),
                            Arrays.asList("2", null, "2021-01-01 01:01:01.0", "9999-12-31 11:59:59.999999", "1")
                    )
            );

            //
            // INVALID REQUEST — MUST THROW ERROR
            //

            Assertions.assertThrows(IllegalArgumentException.class, () -> {
                final MigrateRequest invalidRequest = MigrateRequest.newBuilder()
                        .putAllConfiguration(confMap)
                        .setDetails(MigrationDetails.newBuilder()
                                .setTable("dropColumnInHistoryMode")
                                .setSchema(IntegrationTestBase.schema)
                                .setDrop(
                                        DropOperation.newBuilder()
                                                .setDropColumnInHistoryMode(
                                                        DropColumnInHistoryMode.newBuilder()
                                                                .setColumn("b")
                                                                .setOperationTimestamp("2001-01-01 01:01:01")
                                                )
                                )
                        ).build();

                TeradataJDBCUtil.generateMigrateQueries(invalidRequest, testWarningHandle);
            });
        }
    }

    @Test
    public void addColumnInHistoryMode() throws Exception {

        String tableName = IntegrationTestBase.schema + "_" + "addColumnInHistoryMode";

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            //
            // CLEANUP
            //
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName));
            } catch (Exception ignored) {}

            //
            // CREATE TABLE
            //
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " (a INT NOT NULL, "
                            + "_fivetran_start TIMESTAMP(6) NOT NULL, "
                            + "_fivetran_end TIMESTAMP(6), "
                            + "_fivetran_active BYTEINT, "
                            + "PRIMARY KEY(_fivetran_start, a))"
            );

            //
            // EMPTY REQUEST
            //
            final MigrateRequest emptyRequest = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("addColumnInHistoryMode")
                            .setSchema(IntegrationTestBase.schema)
                            .setAdd(
                                    AddOperation.newBuilder()
                                            .setAddColumnInHistoryMode(
                                                    AddColumnInHistoryMode.newBuilder()
                                                            .setColumn("b")
                                                            .setColumnType(DataType.INT)
                                                            .setDefaultValue("3")
                                                            .setOperationTimestamp("2021-01-01 01:01:01")
                                            )
                            )
                    ).build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries =
                    TeradataJDBCUtil.generateMigrateQueries(emptyRequest, testWarningHandle);

            Assertions.assertEquals(1, queries.size());

            //
            // INSERT TEST DATA
            //
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0)"
            );

            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " VALUES (2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)"
            );

            //
            // VALID RUN
            //
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("addColumnInHistoryMode")
                            .setSchema(IntegrationTestBase.schema)
                            .setAdd(
                                    AddOperation.newBuilder()
                                            .setAddColumnInHistoryMode(
                                                    AddColumnInHistoryMode.newBuilder()
                                                            .setColumn("b")
                                                            .setColumnType(DataType.INT)
                                                            .setDefaultValue("3")
                                                            .setOperationTimestamp("2021-01-01 01:01:01")
                                            )
                            )
                    ).build();

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);

            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                q.execute(conn);
            }

            //
            // VERIFY METADATA
            //
            Table t = TeradataJDBCUtil.getTable(
                    conf,
                    conf.database(),
                    tableName,
                    tableName,
                    testWarningHandle
            );

            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(1).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(3).getName());
            Assertions.assertEquals("b", columns.get(4).getName());
            Assertions.assertEquals(DataType.INT, columns.get(4).getType());
            Assertions.assertEquals(5, columns.size());

            //
            // VERIFY DATA
            //
            checkResult(
                    "SELECT a, b, _fivetran_start, _fivetran_end, _fivetran_active FROM "
                            + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                            + " ORDER BY _fivetran_start, a",
                    Arrays.asList(
                            Arrays.asList("1", null, "2020-01-01 01:01:01.0", "2021-01-01 01:01:01.0", "0"),
                            Arrays.asList("2", null, "2020-01-01 01:01:01.0", "2021-01-01 01:01:00.999999", "0"),
                            Arrays.asList("2", "3", "2021-01-01 01:01:01.0", "9999-12-31 11:59:59.999999", "1")
                    )
            );

            //
            // INVALID REQUEST
            //
            Assertions.assertThrows(IllegalArgumentException.class, () -> {
                final MigrateRequest invalidRequest = MigrateRequest.newBuilder()
                        .putAllConfiguration(confMap)
                        .setDetails(MigrationDetails.newBuilder()
                                .setTable("addColumnInHistoryMode")
                                .setSchema(IntegrationTestBase.schema)
                                .setAdd(
                                        AddOperation.newBuilder()
                                                .setAddColumnInHistoryMode(
                                                        AddColumnInHistoryMode.newBuilder()
                                                                .setColumn("b")
                                                                .setColumnType(DataType.INT)
                                                                .setDefaultValue("3")
                                                                .setOperationTimestamp("2001-01-01 01:01:01")
                                                )
                                )
                        ).build();

                TeradataJDBCUtil.generateMigrateQueries(invalidRequest, testWarningHandle);
            });
        }
    }

}
