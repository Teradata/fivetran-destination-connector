package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.writers.UpdateHistoryWriter;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test class for UpdateHistoryWriter.
 */
public class UpdateHistoryWriterTest extends IntegrationTestBase {

    /**
     * Test to verify behavior when the _fivetran_start column is missing.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void noFivetranStart() throws Exception {
        String tableName = IntegrationTestBase.class.getSimpleName() + "_" + "noFivetranStartUpdate";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table without the _fivetran_start column
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + "(" +
                    "id INT PRIMARY KEY NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_end TIMESTAMP(6))");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 'a', 1, '2005-05-24 20:57:00.0')");

            // Retrieve table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Initialize UpdateHistoryWriter and expect an exception due to missing _fivetran_start column
            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            Exception exception = assertThrows(IllegalArgumentException.class, () -> {
                u.setHeader(Arrays.asList("id", "data", "_fivetran_active", "_fivetran_end"));
            });

            assertEquals("File doesn't contain _fivetran_start column", exception.getMessage());
        }
    }

    /**
     * Test to verify behavior with a single primary key.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void singlePK() throws Exception {
        String tableName = IntegrationTestBase.class.getSimpleName() + "_" + "singlePKUpdate";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table with a single primary key
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + "(" +
                    "id INT NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL," +
                    "_fivetran_end TIMESTAMP(6)," +
                    "PRIMARY KEY(id, _fivetran_start))");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(2, 'b', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(3, 'c', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 'd', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(2, 'e', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");

            // Retrieve table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Initialize UpdateHistoryWriter and write rows
            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            u.setHeader(Arrays.asList("id", "data", "_fivetran_start"));
            u.writeRow(Arrays.asList("1", "f", "2005-05-25T20:57:00Z"));
            u.writeRow(Arrays.asList("2", "unm", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("5", "unm", "2005-05-26T20:57:00Z"));
            u.commit();
        }

        // Verify the results
        checkResult("SELECT * FROM " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " ORDER BY id, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "d", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "a", "0", "2005-05-24 20:57:00.0", "2005-05-25 20:56:59.0"),
                Arrays.asList("1", "f", "1", "2005-05-25 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("2", "e", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "b", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:56:59.0"),
                Arrays.asList("2", "b", "1", "2005-05-26 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("3", "c", "1", "2005-05-24 20:57:00.0", "9999-12-31 23:59:59.999999")
        ));
    }

    /**
     * Test to verify behavior with multiple primary keys.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void multiPK() throws Exception {
        String tableName = IntegrationTestBase.class.getSimpleName() + "_" + "multiPKUpdate";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table with multiple primary keys
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + "(" +
                    "id1 INT NOT NULL, " +
                    "id2 INT NOT NULL, " +
                    "data1 VARCHAR(10), " +
                    "data2 VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL," +
                    "_fivetran_end TIMESTAMP(6)," +
                    "PRIMARY KEY(id1, id2, _fivetran_start))");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 1, 'a', 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 2, 'a', 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(2, 2, 'b', 'b', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(3, 3, 'c', 'c', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 1, 'd', 'd', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(2, 2, 'e', 'e', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");

            // Retrieve table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Initialize UpdateHistoryWriter and write rows
            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            u.setHeader(Arrays.asList("id1", "id2", "data1", "data2", "_fivetran_start"));
            u.writeRow(Arrays.asList("1", "1", "f", "f", "2005-05-25T20:57:00Z"));
            u.writeRow(Arrays.asList("2", "2", "unm", "f", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("3", "3", "unm", "unm", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("5", "5", "unm", "unm", "2005-05-26T20:57:00Z"));
            u.commit();
        }

        // Verify the results
        checkResult("SELECT * FROM " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " ORDER BY id1, id2, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "1", "d", "d", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "1", "a", "a", "0", "2005-05-24 20:57:00.0", "2005-05-25 20:56:59.0"),
                Arrays.asList("1", "1", "f", "f", "1", "2005-05-25 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("1", "2", "a", "a", "1", "2005-05-24 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("2", "2", "e", "e", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "2", "b", "b", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:56:59.0"),
                Arrays.asList("2", "2", "b", "f", "1", "2005-05-26 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("3", "3", "c", "c", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:56:59.0"),
                Arrays.asList("3", "3", "c", "c", "1", "2005-05-26 20:57:00.0", "9999-12-31 23:59:59.999999")
        ));
    }
}