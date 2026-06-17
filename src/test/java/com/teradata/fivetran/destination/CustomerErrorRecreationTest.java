package com.teradata.fivetran.destination;

import com.google.common.collect.ImmutableMap;
import com.teradata.fivetran.destination.writers.UpdateHistoryWriter;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test case that recreates the exact customer error scenario.
 *
 * Original Error:
 * Error 5407: "Invalid operation for DateTime or Interval"
 * Table: "demo_user"."bhargav_teradata_history_mode_testing_5_june_single_store_applwajwlbnkppqqc_users"
 * Operation: writeHistoryBatch (History Mode sync)
 * Query: INSERT INTO ... SELECT ... WHERE _fivetran_active = 1 AND "id" = ?
 *
 * This test verifies the fix works for the exact customer scenario.
 */
public class CustomerErrorRecreationTest extends IntegrationTestBase {

    /**
     * Recreates the exact customer error: Error 5407 during History Mode writeHistoryBatch
     *
     * Customer reported:
     * - Table with status, amount, and timestamp columns
     * - writeHistoryBatch operation failing with Error 5407
     * - UPDATE...INTERVAL '1' SECOND operation was the failure point
     * - Timestamps with varying fractional second precision
     */
    @Test
    public void testCustomerScenarioHistoryModeWriteWithVaryingTimestampPrecision() throws Exception {
        String tableName = "customer_scenario_history_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            // Recreate the exact table structure from customer's error
            // Table: "demo_user"."bhargav_teradata_history_mode_testing_5_june_single_store_applwajwlbnkppqqc_users"
            // Columns: id (PK), status, amount, _fivetran_active, _fivetran_start (PK), _fivetran_end, last_modified, name
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id VARCHAR(256) NOT NULL, " +
                    "status VARCHAR(256), " +
                    "amount INT, " +
                    "last_modified TIMESTAMP(6), " +
                    "name VARCHAR(256), " +
                    "_fivetran_synced TIMESTAMP(6), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "_fivetran_earliest BYTEINT, " +
                    "PRIMARY KEY(id, _fivetran_start))");

            // Insert initial record with status and amount columns (as per customer's table)
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES('user123', 'active', 100, '2026-06-15 09:36:00.000000', 'Alice', " +
                    "'2026-06-15 09:36:00.000000', 1, '2026-06-15 09:36:00.000000', " +
                    "'9999-12-31 23:59:59.999999', 1)");

            // Get table metadata for UpdateHistoryWriter
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder()
                    .setNullString("NULL")
                    .setUnmodifiedString("unm")
                    .build();

            // Create UpdateHistoryWriter
            UpdateHistoryWriter writer = new UpdateHistoryWriter(conn, database, tableName, t.getColumnsList(), params, null, 10);

            // Set header matching the customer's data structure
            writer.setHeader(Arrays.asList(
                    "id",
                    "status",
                    "amount",
                    "last_modified",
                    "name",
                    "_fivetran_synced",
                    "_fivetran_start",
                    "_fivetran_active",
                    "_fivetran_end",
                    "_fivetran_earliest"));

            // THIS IS THE KEY TEST: Write a row with timestamp having 1 decimal place
            // This is the exact timestamp precision that was failing in the customer's case
            // Original error occurred with timestamps like: 2026-06-15T09:36:10.5Z
            writer.writeRow(Arrays.asList(
                    "user123",                          // id
                    "updated",                          // status (modified column)
                    "150",                              // amount (modified column)
                    "2026-06-15 09:36:05.000000",     // last_modified
                    "Alice",                            // name (unmodified)
                    "2026-06-15 09:36:10.5",           // _fivetran_synced - 1 DECIMAL PLACE (THE PROBLEM!)
                    "2026-06-15 09:36:10.5",           // _fivetran_start - 1 DECIMAL PLACE (THE PROBLEM!)
                    "1",                                // _fivetran_active
                    "9999-12-31 23:59:59.999999",     // _fivetran_end
                    "1"                                 // _fivetran_earliest
            ));

            // If we reach here without Error 5407, the fix worked!
            // Verify the record was processed correctly
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as total, SUM(CASE WHEN _fivetran_active = 1 THEN 1 ELSE 0 END) as active " +
                    "FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " WHERE id = 'user123'")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt("total") >= 2, "Should have at least 2 history records");
                assertEquals(1, rs.getInt("active"), "Only 1 record should be active");
            }
        }
    }

    /**
     * Tests the exact UPDATE...INTERVAL '1' SECOND operation that was failing
     */
    @Test
    public void testUpdateIntervalArithmeticWithCustomerData() throws Exception {
        String tableName = "customer_interval_test_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id VARCHAR(256) NOT NULL, " +
                    "status VARCHAR(256), " +
                    "amount INT, " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "PRIMARY KEY(id, _fivetran_start))");

            // Insert record with exact customer data structure
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES('user123', 'active', 100, 1, '2026-06-15 09:36:10.5', '9999-12-31 23:59:59.999999')");

            // Execute the EXACT failing query from customer's error log
            // This was causing Error 5407 before the fix
            String updateQuery = "UPDATE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " SET _fivetran_active = 0, _fivetran_end = ? - INTERVAL '1' SECOND " +
                    "WHERE _fivetran_active = 1 AND _fivetran_start < ? AND id = ?";

            java.sql.PreparedStatement pstmt = conn.prepareStatement(updateQuery);

            // Use timestamps with 1 decimal place (the problem case)
            pstmt.setTimestamp(1, java.sql.Timestamp.valueOf("2026-06-15 09:36:11.5"));
            pstmt.setTimestamp(2, java.sql.Timestamp.valueOf("2026-06-15 09:36:11.5"));
            pstmt.setString(3, "user123");

            // This should NOT throw Error 5407
            pstmt.execute();
            pstmt.close();

            // Verify the update worked
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT _fivetran_active FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " WHERE id = 'user123'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Record should be deactivated");
            }
        }
    }

    /**
     * Tests INSERT SELECT operation using UpdateHistoryWriter
     * This is the core operation in UpdateHistoryWriter.insertNewRow()
     *
     * Note: Direct JDBC setTimestamp() doesn't use our formatISODateTime() fix.
     * This test verifies the fix works through the UpdateHistoryWriter path.
     */
    @Test
    public void testInsertSelectWithCustomerTableStructure() throws Exception {
        String tableName = "customer_insert_select_test_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id VARCHAR(256) NOT NULL, " +
                    "status VARCHAR(256), " +
                    "amount INT, " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "PRIMARY KEY(id, _fivetran_start))");

            // Insert initial record
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES('user123', 'active', 100, 1, '2026-06-15 09:36:10.000000', '9999-12-31 23:59:59.999999')");

            // Use UpdateHistoryWriter which uses formatISODateTime() for proper timestamp formatting
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder()
                    .setNullString("NULL")
                    .setUnmodifiedString("unm")
                    .build();

            UpdateHistoryWriter writer = new UpdateHistoryWriter(conn, database, tableName, t.getColumnsList(), params, null, 10);
            writer.setHeader(Arrays.asList("id", "status", "amount", "_fivetran_start", "_fivetran_active", "_fivetran_end"));

            // Write with timestamp having 1 decimal place (problem case)
            // This calls UpdateHistoryWriter which uses our formatISODateTime() fix
            writer.writeRow(Arrays.asList(
                    "user123",                              // id
                    "updated",                              // status (modified)
                    "150",                                  // amount (modified)
                    "2026-06-15 09:36:10.5",               // _fivetran_start (1 decimal - problem case!)
                    "1",                                    // _fivetran_active
                    "9999-12-31 23:59:59.999999"           // _fivetran_end
            ));

            // Verify the insert worked
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " WHERE id = 'user123'")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 records after INSERT SELECT through UpdateHistoryWriter");
            }
        }
    }

    /**
     * Complete end-to-end test matching customer's exact error scenario
     *
     * This simulates the exact flow that was failing:
     * 1. Create history mode table with customer's structure
     * 2. Insert initial record
     * 3. Run UpdateHistoryWriter with timestamp having 1 decimal place
     * 4. Verify INSERT SELECT and UPDATE operations complete without Error 5407
     */
    @Test
    public void testCompleteCustomerErrorScenario() throws Exception {
        String tableName = "customer_complete_scenario_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            // Create exact customer table structure
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id VARCHAR(256) NOT NULL, " +
                    "status VARCHAR(256), " +
                    "amount INT, " +
                    "last_modified TIMESTAMP(6), " +
                    "name VARCHAR(256), " +
                    "_fivetran_synced TIMESTAMP(6), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "_fivetran_earliest BYTEINT, " +
                    "PRIMARY KEY(id, _fivetran_start))");

            // Insert base records (customer's initial data)
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES('user1', 'active', 100, '2026-06-15 09:36:00.000000', 'Alice', " +
                    "'2026-06-15 09:36:00.000000', 1, '2026-06-15 09:36:00.000000', " +
                    "'9999-12-31 23:59:59.999999', 1)");

            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES('user2', 'active', 200, '2026-06-15 09:36:00.000000', 'Bob', " +
                    "'2026-06-15 09:36:00.000000', 1, '2026-06-15 09:36:00.000000', " +
                    "'9999-12-31 23:59:59.999999', 1)");

            // Get table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder()
                    .setNullString("NULL")
                    .setUnmodifiedString("unm")
                    .build();

            // Create UpdateHistoryWriter
            UpdateHistoryWriter writer = new UpdateHistoryWriter(conn, database, tableName, t.getColumnsList(), params, null, 10);
            writer.setHeader(Arrays.asList(
                    "id", "status", "amount", "last_modified", "name", "_fivetran_synced",
                    "_fivetran_start", "_fivetran_active", "_fivetran_end", "_fivetran_earliest"));

            // Execute multiple writes with varying timestamp precision (all the problematic cases)

            // Write 1: 1 decimal place (customer's exact error case)
            writer.writeRow(Arrays.asList("user1", "updated", "150", "2026-06-15 09:36:05.000000", "Alice",
                    "2026-06-15 09:36:10.5", "2026-06-15 09:36:10.5", "1", "9999-12-31 23:59:59.999999", "1"));

            // Write 2: 2 decimal places
            writer.writeRow(Arrays.asList("user2", "updated", "250", "2026-06-15 09:36:05.000000", "Bob",
                    "2026-06-15 09:36:10.52", "2026-06-15 09:36:10.52", "1", "9999-12-31 23:59:59.999999", "1"));

            // If we reach here, Error 5407 is FIXED!
            // Verify all records were processed
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as total, SUM(CASE WHEN _fivetran_active = 1 THEN 1 ELSE 0 END) as active " +
                    "FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName))) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt("total"), "Should have 4 total history records (2 original + 2 new)");
                assertEquals(2, rs.getInt("active"), "Should have 2 active records");
            }
        }
    }
}
