package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import fivetran_sdk.v2.*;

public class MigrateTest extends IntegrationTestBase {

    @Test
    public void shouldAddColumnWithDefaultValue() throws Exception {
        String tableName = "addColumnDefault"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create table without schema prefix - use TeradataJDBCUtil.escapeTable
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " (id INT)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " VALUES (1)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("addColumnDefault")
                    .setAdd(AddOperation.newBuilder()
                            .setAddColumnWithDefaultValue(AddColumnWithDefaultValue.newBuilder()
                                    .setColumn("col_def")
                                    .setColumnType(DataType.INT)
                                    .setDefaultValue("100")
                                    .build())
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "addColumnDefault");

            checkResult("SELECT col_def FROM " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName),
                    Arrays.asList(Arrays.asList("100")));
        }
    }

    @Test
    public void shouldUpdateColumnValue() throws Exception {
        String tableName = "updateColVal"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " (id INT, val INT)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " VALUES (1, 10)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("updateColVal")
                    .setUpdateColumnValue(UpdateColumnValueOperation.newBuilder()
                            .setColumn("val")
                            .setValue("20")
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "updateColVal");

            checkResult("SELECT val FROM " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName),
                    Arrays.asList(Arrays.asList("20")));
        }
    }

    @Test
    public void shouldRenameTable() throws Exception {
        String fromTable = "renameFrom"; // Remove schema prefix
        String toTable = "renameTo"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, fromTable) + " (id INT)");

            // Ensure target table doesn't exist
            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, toTable));
            } catch (SQLException e) {
                // Ignore if not exists
            }

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("renameFrom") // Use just the table name
                    .setRename(RenameOperation.newBuilder()
                            .setRenameTable(RenameTable.newBuilder()
                                    .setFromTable("renameFrom")
                                    .setToTable("renameTo")
                                    .build())
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "renameFrom");

            // Verify old table gone (should fail to select or return false check)
            boolean oldExists = TeradataJDBCUtil.checkTableExists(stmt, IntegrationTestBase.schema, fromTable);
            assertFalse(oldExists, "Old table should not exist");

            // Verify new table exists
            boolean newExists = TeradataJDBCUtil.checkTableExists(stmt, IntegrationTestBase.schema, toTable);
            assertTrue(newExists, "New table should exist");
        }
    }

    @Test
    public void shouldRenameColumn() throws Exception {
        String tableName = "renameCol"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " (old_col INT)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("renameCol")
                    .setRename(RenameOperation.newBuilder()
                            .setRenameColumn(RenameColumn.newBuilder()
                                    .setFromColumn("old_col")
                                    .setToColumn("new_col")
                                    .build())
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "renameCol");

            Table t = TeradataJDBCUtil.getTable(conf, IntegrationTestBase.schema, tableName, "renameCol", testWarningHandle);
            boolean found = t.getColumnsList().stream().anyMatch(c -> c.getName().equals("new_col"));
            assertTrue(found, "Column should be renamed to new_col");
        }
    }

    @Test
    public void shouldCopyTable() throws Exception {
        String fromTable = "copyFrom"; // Remove schema prefix
        String toTable = "copyTo"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, fromTable) + " (id INT)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, fromTable) + " VALUES (99)");

            try {
                stmt.execute("DROP TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, toTable));
            } catch (SQLException e) {
                // Ignore
            }

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("copyFrom")
                    .setCopy(CopyOperation.newBuilder()
                            .setCopyTable(CopyTable.newBuilder()
                                    .setFromTable("copyFrom")
                                    .setToTable("copyTo")
                                    .build())
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "copyFrom");

            checkResult("SELECT id FROM " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, toTable),
                    Arrays.asList(Arrays.asList("99")));
        }
    }

    @Test
    public void shouldCopyColumn() throws Exception {
        String tableName = "copyCol"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Must create target column first as per current implementation limitation
            stmt.execute(
                    "CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " (src INT, dst INT)");
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " VALUES (55, NULL)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("copyCol")
                    .setCopy(CopyOperation.newBuilder()
                            .setCopyColumn(CopyColumn.newBuilder()
                                    .setFromColumn("src")
                                    .setToColumn("dst")
                                    .build())
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "copyCol");

            checkResult("SELECT dst FROM " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName),
                    Arrays.asList(Arrays.asList("55")));
        }
    }

    @Test
    public void shouldDropTable() throws Exception {
        String tableName = "dropTable"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " (id INT)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("dropTable")
                    .setDrop(DropOperation.newBuilder()
                            .setDropTable(true)
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "dropTable");

            assertFalse(TeradataJDBCUtil.checkTableExists(stmt, IntegrationTestBase.schema, tableName));
        }
    }

    @Test
    public void shouldHandleTableSyncModeMigration_LiveToHistory() throws Exception {
        String tableName = "liveToHist"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " (id INT)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " VALUES (1)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("liveToHist")
                    .setTableSyncModeMigration(TableSyncModeMigrationOperation.newBuilder()
                            .setType(TableSyncModeMigrationType.LIVE_TO_HISTORY)
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "liveToHist");

            // Verify metadata columns exist and values are correct
            // _fivetran_active = 1, _fivetran_end = 9999..., _fivetran_start is present
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT _fivetran_active FROM " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName))) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    public void shouldHandleTableSyncModeMigration_SoftDeleteToHistory() throws Exception {
        String tableName = "softToHist"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName)
                    + " (id INT, _fivetran_deleted BYTEINT, _fivetran_synced TIMESTAMP(6))");
            // Row 1: deleted
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName)
                    + " VALUES (1, 1, CURRENT_TIMESTAMP)");
            // Row 2: active
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName)
                    + " VALUES (2, 0, CURRENT_TIMESTAMP)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("softToHist")
                    .setTableSyncModeMigration(TableSyncModeMigrationOperation.newBuilder()
                            .setType(TableSyncModeMigrationType.SOFT_DELETE_TO_HISTORY)
                            .setSoftDeletedColumn("_fivetran_deleted")
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "softToHist");

            // Verify _fivetran_deleted dropped
            Table t = TeradataJDBCUtil.getTable(conf, IntegrationTestBase.schema, tableName, "softToHist", testWarningHandle);
            boolean hasDeletedCol = t.getColumnsList().stream().anyMatch(c -> c.getName().equals("_fivetran_deleted"));
            assertFalse(hasDeletedCol, "_fivetran_deleted should be dropped");

            // Verify values
            try (ResultSet rs = stmt.executeQuery("SELECT _fivetran_active, id FROM "
                    + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " ORDER BY id")) {
                assertTrue(rs.next()); // id 1
                assertEquals(0, rs.getInt(1)); // active=0 (was deleted)

                assertTrue(rs.next()); // id 2
                assertEquals(1, rs.getInt(1)); // active=1 (was not deleted)
            }
        }
    }

    @Test
    public void shouldHandleTableSyncModeMigration_HistoryToLive() throws Exception {
        String tableName = "histToLive"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) +
                    " (id INT, _fivetran_active BYTEINT, _fivetran_start TIMESTAMP(6), _fivetran_end TIMESTAMP(6))");
            // Active row
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) +
                    " VALUES (1, 1, CURRENT_TIMESTAMP, TIMESTAMP '9999-12-31 23:59:59')");
            // Inactive row
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) +
                    " VALUES (1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("histToLive")
                    .setTableSyncModeMigration(TableSyncModeMigrationOperation.newBuilder()
                            .setType(TableSyncModeMigrationType.HISTORY_TO_LIVE)
                            // Default keepDeletedRows = false
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "histToLive");

            // Verify columns dropped
            Table t = TeradataJDBCUtil.getTable(conf, IntegrationTestBase.schema, tableName, "histToLive", testWarningHandle);
            boolean hasActive = t.getColumnsList().stream().anyMatch(c -> c.getName().equals("_fivetran_active"));
            assertFalse(hasActive);

            // Verify inactive row deleted, active row remains
            checkResult("SELECT id FROM " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName),
                    Arrays.asList(Arrays.asList("1")));
        }
    }

    @Test
    public void shouldHandleTableSyncModeMigration_HistoryToSoftDelete() throws Exception {
        String tableName = "histToSoft"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) +
                    " (id INT, _fivetran_active BYTEINT, _fivetran_start TIMESTAMP(6), _fivetran_end TIMESTAMP(6))");
            // Active row
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) +
                    " VALUES (1, 1, CURRENT_TIMESTAMP, TIMESTAMP '9999-12-31 23:59:59')");
            // Inactive row
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) +
                    " VALUES (2, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("histToSoft")
                    .setTableSyncModeMigration(TableSyncModeMigrationOperation.newBuilder()
                            .setType(TableSyncModeMigrationType.HISTORY_TO_SOFT_DELETE)
                            .setSoftDeletedColumn("_fivetran_deleted")
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "histToSoft");

            // Verify _fivetran_deleted exists
            Table t = TeradataJDBCUtil.getTable(conf, IntegrationTestBase.schema, tableName, "histToSoft", testWarningHandle);
            boolean hasDeleted = t.getColumnsList().stream().anyMatch(c -> c.getName().equals("_fivetran_deleted"));
            assertTrue(hasDeleted);

            checkResult("SELECT id, _fivetran_deleted FROM " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName),
                    Arrays.asList(Arrays.asList("1", "0")));
        }
    }

    @Test
    public void shouldHandleTableSyncModeMigration_SoftDeleteToLive() throws Exception {
        String tableName = "softToLive"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName)
                    + " (id INT, _fivetran_deleted BYTEINT)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " VALUES (1, 0)"); // Active
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " VALUES (2, 1)"); // Deleted

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("softToLive")
                    .setTableSyncModeMigration(TableSyncModeMigrationOperation.newBuilder()
                            .setType(TableSyncModeMigrationType.SOFT_DELETE_TO_LIVE)
                            .setSoftDeletedColumn("_fivetran_deleted")
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "softToLive");

            // Verify _fivetran_deleted dropped
            Table t = TeradataJDBCUtil.getTable(conf, IntegrationTestBase.schema, tableName, "softToLive", testWarningHandle);
            boolean hasDeleted = t.getColumnsList().stream().anyMatch(c -> c.getName().equals("_fivetran_deleted"));
            assertFalse(hasDeleted);

            // Verify only active row remains
            checkResult("SELECT id FROM " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName),
                    Arrays.asList(Arrays.asList("1")));
        }
    }

    @Test
    public void shouldHandleTableSyncModeMigration_LiveToSoftDelete() throws Exception {
        String tableName = "liveToSoft"; // Remove schema prefix
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " (id INT)");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName) + " VALUES (1)");

            MigrationDetails details = MigrationDetails.newBuilder()
                    .setSchema(IntegrationTestBase.schema)
                    .setTable("liveToSoft")
                    .setTableSyncModeMigration(TableSyncModeMigrationOperation.newBuilder()
                            .setType(TableSyncModeMigrationType.LIVE_TO_SOFT_DELETE)
                            .setSoftDeletedColumn("_fivetran_deleted")
                            .build())
                    .build();

            TeradataMigrationUtil.handleMigration(conn, stmt, details, IntegrationTestBase.schema, "liveToSoft");

            // Verify _fivetran_deleted added and set to 0
            checkResult("SELECT id, _fivetran_deleted FROM " + TeradataJDBCUtil.escapeTable(IntegrationTestBase.schema, tableName),
                    Arrays.asList(Arrays.asList("1", "0")));
        }
    }
}