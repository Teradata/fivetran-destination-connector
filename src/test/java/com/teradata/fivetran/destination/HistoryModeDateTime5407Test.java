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
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test suite for Error 5407 fix: "Invalid operation for DateTime or Interval"
 *
 * These tests specifically verify that History Mode DateTime arithmetic (INTERVAL operations)
 * works correctly with timestamps having varying fractional second precision.
 */
public class HistoryModeDateTime5407Test extends IntegrationTestBase {

    /**
     * Test that UPDATE with INTERVAL '1' SECOND arithmetic works with normalized timestamps.
     * This is the exact scenario that was failing with Error 5407.
     */
    @Test
    public void testUpdateWithIntervalArithmetic() throws Exception {
        String tableName = "test_datetime_interval_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            // Create history table with TIMESTAMP(6) columns
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id INT NOT NULL, " +
                    "data VARCHAR(50), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "PRIMARY KEY(id, _fivetran_start))");

            // Insert test data with various timestamp formats
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES(1, 'initial', 1, '2026-01-01 10:00:00.000000', '9999-12-31 23:59:59.999999')");

            // This is the critical query from UpdateHistoryWriter.updateOldRow()
            // It should NOT fail with Error 5407 anymore
            String updateQuery = "UPDATE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " SET _fivetran_active = 0, _fivetran_end = ? - INTERVAL '1' SECOND " +
                    "WHERE _fivetran_active = 1 AND _fivetran_start < ? AND id = ?";

            java.sql.PreparedStatement pstmt = conn.prepareStatement(updateQuery);
            // These timestamps have different fractional second precision
            pstmt.setTimestamp(1, java.sql.Timestamp.valueOf("2026-01-02 10:00:00.500000"));
            pstmt.setTimestamp(2, java.sql.Timestamp.valueOf("2026-01-02 10:00:00.500000"));
            pstmt.setInt(3, 1);

            // This should execute without Error 5407
            pstmt.execute();
            pstmt.close();

            // Verify the update worked
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT _fivetran_active FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Row should be marked inactive");
            }
        }
    }

    /**
     * Regression test: Verify History Mode writes with single primary key work correctly.
     */
    @Test
    public void testHistoryModeSinglePrimaryKeyWithVaryingTimestamps() throws Exception {
        String tableName = "test_history_single_pk_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id INT NOT NULL, " +
                    "name VARCHAR(50), " +
                    "amount INT, " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "PRIMARY KEY(id, _fivetran_start))");

            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES(1, 'Alice', 100, 1, '2026-01-01 10:00:00.000000', '9999-12-31 23:59:59.999999')");

            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").setUnmodifiedString("unm").build();

            UpdateHistoryWriter writer = new UpdateHistoryWriter(conn, database, tableName, t.getColumnsList(), params, null, 10);
            writer.setHeader(Arrays.asList("id", "name", "amount", "_fivetran_start", "_fivetran_active", "_fivetran_end"));

            // Write a row with varying timestamp precision (this should not throw Error 5407)
            writer.writeRow(Arrays.asList("1", "Alice", "200", "2026-01-02 10:00:00.5", "1", "9999-12-31 23:59:59.999999"));

            // Verify the write succeeded by checking row count
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 history records after write");
            }
        }
    }

    /**
     * Regression test: Verify History Mode with multiple primary keys works correctly.
     */
    @Test
    public void testHistoryModeMultiplePrimaryKeysWithTimestamps() throws Exception {
        String tableName = "test_history_multi_pk_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id1 INT NOT NULL, " +
                    "id2 VARCHAR(50) NOT NULL, " +
                    "amount INT, " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "PRIMARY KEY(id1, id2, _fivetran_start))");

            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES(1, 'A', 100, 1, '2026-01-01 10:00:00.123456', '9999-12-31 23:59:59.999999')");

            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").setUnmodifiedString("unm").build();

            UpdateHistoryWriter writer = new UpdateHistoryWriter(conn, database, tableName, t.getColumnsList(), params, null, 10);
            writer.setHeader(Arrays.asList("id1", "id2", "amount", "_fivetran_start", "_fivetran_active", "_fivetran_end"));

            // Write with timestamp that has no fractional seconds (would fail before fix)
            writer.writeRow(Arrays.asList("1", "A", "150", "2026-01-02 10:00:00", "1", "9999-12-31 23:59:59.999999"));

            // Verify success
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 history records");
            }
        }
    }

    /**
     * Regression test: Verify that timestamps with 1-5 decimal places are properly normalized.
     * This specific case was broken before the fix.
     */
    @Test
    public void testTimestampNormalizationEdgeCases() throws Exception {
        String tableName = "test_timestamp_edge_cases_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id INT NOT NULL, " +
                    "status VARCHAR(50), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "PRIMARY KEY(id, _fivetran_start))");

            // Insert base record
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES(1, 'v1', 1, '2026-01-01 10:00:00.000000', '9999-12-31 23:59:59.999999')");

            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").setUnmodifiedString("unm").build();

            UpdateHistoryWriter writer = new UpdateHistoryWriter(conn, database, tableName, t.getColumnsList(), params, null, 10);
            writer.setHeader(Arrays.asList("id", "status", "_fivetran_start", "_fivetran_active", "_fivetran_end"));

            // Test case 1: 1 decimal place (would fail with Error 5407 before fix)
            writer.writeRow(Arrays.asList("1", "v2", "2026-01-02 10:00:00.1", "1", "9999-12-31 23:59:59.999999"));

            // Test case 2: 2 decimal places
            writer.writeRow(Arrays.asList("1", "v3", "2026-01-03 10:00:00.12", "1", "9999-12-31 23:59:59.999999"));

            // Test case 3: 3 decimal places
            writer.writeRow(Arrays.asList("1", "v4", "2026-01-04 10:00:00.123", "1", "9999-12-31 23:59:59.999999"));

            // Test case 4: 4 decimal places
            writer.writeRow(Arrays.asList("1", "v5", "2026-01-05 10:00:00.1234", "1", "9999-12-31 23:59:59.999999"));

            // Test case 5: 5 decimal places
            writer.writeRow(Arrays.asList("1", "v6", "2026-01-06 10:00:00.12345", "1", "9999-12-31 23:59:59.999999"));

            // Verify all writes succeeded
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName))) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) >= 5, "All timestamp precision variations should be written successfully");
            }
        }
    }

    /**
     * Regression test: Verify INSERT SELECT with INTERVAL arithmetic in UpdateHistoryWriter.
     */
    @Test
    public void testInsertSelectWithIntervalArithmetic() throws Exception {
        String tableName = "test_insert_select_interval_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id INT NOT NULL, " +
                    "status VARCHAR(50), " +
                    "amount INT, " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "PRIMARY KEY(id, _fivetran_start))");

            // Insert initial active record
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES(1, 'ACTIVE', 100, 1, '2026-01-01 10:00:00.000000', '9999-12-31 23:59:59.999999')");

            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").setUnmodifiedString("unm").build();

            UpdateHistoryWriter writer = new UpdateHistoryWriter(conn, database, tableName, t.getColumnsList(), params, null, 10);
            writer.setHeader(Arrays.asList("id", "status", "amount", "_fivetran_start", "_fivetran_active", "_fivetran_end"));

            // Write updated record with timestamp having fractional seconds
            // This triggers INSERT SELECT which was failing with Error 5407
            writer.writeRow(Arrays.asList("1", "ACTIVE", "150", "2026-01-02 10:00:00.5", "1", "9999-12-31 23:59:59.999999"));

            // Verify both records exist and the old one was deactivated
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*), SUM(CASE WHEN _fivetran_active = 1 THEN 1 ELSE 0 END) as active_count " +
                    "FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 history records");
                assertEquals(1, rs.getInt(2), "Only 1 should be active");
            }
        }
    }

    /**
     * Regression test: Verify that the fix doesn't break UPDATE with multiple WHERE conditions.
     */
    @Test
    public void testUpdateWithMultipleWhereConditions() throws Exception {
        String tableName = "test_update_multiple_where_" + System.nanoTime();
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id1 INT NOT NULL, " +
                    "id2 INT NOT NULL, " +
                    "status VARCHAR(50), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "PRIMARY KEY(id1, id2, _fivetran_start))");

            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES(1, 100, 'v1', 1, '2026-01-01 10:00:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES(2, 200, 'v2', 1, '2026-01-01 10:00:00.000000', '9999-12-31 23:59:59.999999')");

            // Test UPDATE with multiple WHERE conditions and INTERVAL arithmetic
            String updateQuery = "UPDATE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " SET _fivetran_active = 0, _fivetran_end = ? - INTERVAL '1' SECOND " +
                    "WHERE _fivetran_active = 1 AND _fivetran_start < ? AND id1 = ? AND id2 = ?";

            java.sql.PreparedStatement pstmt = conn.prepareStatement(updateQuery);
            pstmt.setTimestamp(1, java.sql.Timestamp.valueOf("2026-01-02 10:00:00.999999"));
            pstmt.setTimestamp(2, java.sql.Timestamp.valueOf("2026-01-02 10:00:00.999999"));
            pstmt.setInt(3, 1);
            pstmt.setInt(4, 100);

            pstmt.execute();
            pstmt.close();

            // Verify only the matching record was updated
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " WHERE _fivetran_active = 0")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Only 1 record should be deactivated");
            }
        }
    }
}
