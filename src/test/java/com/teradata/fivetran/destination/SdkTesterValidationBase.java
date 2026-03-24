package com.teradata.fivetran.destination;
import com.teradata.fivetran.destination.warning_util.WarningHandler;
import fivetran_sdk.v2.Table;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Base class for SDK tester validation tests.
 * Provides helper methods to verify Teradata database state
 * after the Fivetran SDK destination tester has run.
 */
public class SdkTesterValidationBase extends IntegrationTestBase {

    /**
     * Schema name used by the SDK tester (default: "tester").
     * Override via SDK_TESTER_SCHEMA env var.
     */
    protected static String testerSchema = System.getenv("SDK_TESTER_SCHEMA") != null
            ? System.getenv("SDK_TESTER_SCHEMA")
            : "tester";

    protected String getTableName(String logicalName) {
        return testerSchema + "_" + logicalName;
    }

    protected String getFullTableRef(String logicalName) {
        return TeradataJDBCUtil.escapeTable(database, getTableName(logicalName));
    }

    /**
     * Assert that a table exists in the database.
     */
    protected void assertTableExists(String logicalName) throws Exception {
        String tableName = getTableName(logicalName);
        try {
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            assertNotNull(t, "Table " + tableName + " should exist");
        } catch (TableNotExistException e) {
            fail("Table " + tableName + " should exist but was not found");
        }
    }

    /**
     * Assert that a table does NOT exist in the database.
     */
    protected void assertTableNotExists(String logicalName) {
        String tableName = getTableName(logicalName);
        assertThrows(TableNotExistException.class, () -> {
            TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
        }, "Table " + tableName + " should not exist");
    }

    /**
     * Assert the row count of a table.
     */
    protected void assertRowCount(String logicalName, int expectedCount) throws Exception {
        String query = String.format("SELECT COUNT(*) FROM %s", getFullTableRef(logicalName));
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            assertTrue(rs.next());
            assertEquals(expectedCount, rs.getInt(1),
                    "Row count mismatch for table " + getTableName(logicalName));
        }
    }

    /**
     * Assert that a column exists in a table with the expected Fivetran data type.
     */
    protected void assertColumnExists(String logicalName, String columnName) throws Exception {
        String tableName = getTableName(logicalName);
        Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
        boolean found = t.getColumnsList().stream()
                .anyMatch(c -> c.getName().equals(columnName));
        assertTrue(found, "Column " + columnName + " should exist in table " + tableName);
    }

    /**
     * Assert that a column does NOT exist in a table.
     */
    protected void assertColumnNotExists(String logicalName, String columnName) throws Exception {
        String tableName = getTableName(logicalName);
        Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
        boolean found = t.getColumnsList().stream()
                .anyMatch(c -> c.getName().equals(columnName));
        assertFalse(found, "Column " + columnName + " should not exist in table " + tableName);
    }

    /**
     * Assert the column count of a table.
     */
    protected void assertColumnCount(String logicalName, int expectedCount) throws Exception {
        String tableName = getTableName(logicalName);
        Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
        assertEquals(expectedCount, t.getColumnsList().size(),
                "Column count mismatch for table " + tableName);
    }

    /**
     * Assert that a table has specific column names (in any order).
     */
    protected void assertTableHasColumns(String logicalName, String... expectedColumns) throws Exception {
        String tableName = getTableName(logicalName);
        Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
        List<String> actualColumns = new ArrayList<>();
        t.getColumnsList().forEach(c -> actualColumns.add(c.getName()));
        for (String expected : expectedColumns) {
            assertTrue(actualColumns.contains(expected),
                    "Column " + expected + " not found in table " + tableName +
                            ". Actual columns: " + actualColumns);
        }
    }

    /**
     * Assert exact rows returned by a query. Delegates to IntegrationTestBase.checkResult().
     */
    protected void assertRows(String logicalName, String selectColumns, String orderBy,
                              List<List<String>> expectedRows) throws Exception {
        String query = String.format("SELECT %s FROM %s ORDER BY %s",
                selectColumns, getFullTableRef(logicalName), orderBy);
        checkResult(query, expectedRows);
    }

    /**
     * Assert a table is empty.
     */
    protected void assertTableEmpty(String logicalName) throws Exception {
        assertRowCount(logicalName, 0);
    }

    /**
     * Execute a raw query and return the single integer result.
     */
    protected int queryInt(String sql) throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    /**
     * Execute a raw query and return the single string result.
     */
    protected String queryString(String sql) throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }
}
