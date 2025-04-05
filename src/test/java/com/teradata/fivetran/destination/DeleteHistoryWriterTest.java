package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.writers.*;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test class for DeleteHistoryWriter.
 */
public class DeleteHistoryWriterTest extends IntegrationTestBase {

    /**
     * Test to verify behavior when the _fivetran_end column is missing.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void noFivetranEnd() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table without the _fivetran_end column
            stmt.execute("CREATE TABLE " + conf.database() + ".noFivetranEnd(" +
                    "id INT PRIMARY KEY NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6))");
            stmt.execute("INSERT INTO " + conf.database() + ".noFivetranEnd VALUES(1, 'a', 1, '2005-05-24 20:57:00.000000')");

            // Retrieve table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, schema, "noFivetranEnd", "noFivetranEnd", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Initialize DeleteHistoryWriter and expect an exception due to missing _fivetran_end column
            DeleteHistoryWriter d = new DeleteHistoryWriter(conn, database, schema, t.getName(), t.getColumnsList(), params, null, 123);
            Exception exception = assertThrows(IllegalArgumentException.class, () -> {
                d.setHeader(Arrays.asList("id", "data", "_fivetran_active", "_fivetran_start"));
            });

            assertEquals("File doesn't contain _fivetran_end column", exception.getMessage());
        }
    }

    /**
     * Test to verify behavior with a single primary key.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void singlePK() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table with a single primary key
            stmt.execute("CREATE TABLE " + conf.database() + ".singlePK(" +
                    "id INT NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL," +
                    "_fivetran_end TIMESTAMP(6)," +
                    "PRIMARY KEY(id, _fivetran_start))");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePK VALUES(1, 'a', 1, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePK VALUES(2, 'b', 1, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePK VALUES(3, 'c', 1, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePK VALUES(1, 'd', 0, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePK VALUES(2, 'e', 0, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");

            // Retrieve table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, schema, "singlePK", "singlePK", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Initialize DeleteHistoryWriter and write rows
            DeleteHistoryWriter d = new DeleteHistoryWriter(conn, database, schema, t.getName(), t.getColumnsList(), params, null, 123);
            d.setHeader(Arrays.asList("id", "_fivetran_end"));
            d.writeRow(Arrays.asList("1", "2005-05-25T20:57:00Z"));
            d.writeRow(Arrays.asList("2", "2005-05-26T20:57:00Z"));
            d.writeRow(Arrays.asList("5", "2005-05-26T20:57:00Z"));
            d.commit();
        }

        // Verify the results
        checkResult("SELECT * FROM " + conf.database() + ".singlePK ORDER BY id, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "d", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "a", "0", "2005-05-24 20:57:00.0", "2005-05-25 20:57:00.0"),
                Arrays.asList("2", "e", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "b", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:57:00.0"),
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
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table with multiple primary keys
            stmt.execute("CREATE TABLE " + conf.database() + ".multiPK(" +
                    "id1 INT NOT NULL, " +
                    "id2 INT NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL," +
                    "_fivetran_end TIMESTAMP(6)," +
                    "PRIMARY KEY(id1, id2, _fivetran_start))");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPK VALUES(1, 2, 'a', 1, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPK VALUES(1, 1, 'a', 1, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPK VALUES(2, 2, 'b', 1, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPK VALUES(3, 3, 'c', 1, '2005-05-24 20:57:00.000000', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPK VALUES(1, 1, 'd', 0, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPK VALUES(2, 2, 'e', 0, '2005-05-23 20:57:00.000000', '2005-05-24 20:56:59.999999')");

            // Retrieve table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, schema, "multiPK", "multiPK", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Initialize DeleteHistoryWriter and write rows
            DeleteHistoryWriter d = new DeleteHistoryWriter(conn, database, schema, t.getName(), t.getColumnsList(), params, null, 123);
            d.setHeader(Arrays.asList("id1", "id2", "_fivetran_end"));
            d.writeRow(Arrays.asList("1", "1", "2005-05-25T20:57:00Z"));
            d.writeRow(Arrays.asList("2", "2", "2005-05-26T20:57:00Z"));
            d.writeRow(Arrays.asList("5", "5", "2005-05-26T20:57:00Z"));
            d.commit();
        }

        // Verify the results
        checkResult("SELECT * FROM " + conf.database() + ".multiPK ORDER BY id1, id2, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "1", "d", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "1", "a", "0", "2005-05-24 20:57:00.0", "2005-05-25 20:57:00.0"),
                Arrays.asList("1", "2", "a", "1", "2005-05-24 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("2", "2", "e", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "2", "b", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:57:00.0"),
                Arrays.asList("3", "3", "c", "1", "2005-05-24 20:57:00.0", "9999-12-31 23:59:59.999999")
        ));
    }
}