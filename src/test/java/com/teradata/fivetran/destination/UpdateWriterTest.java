package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import com.teradata.fivetran.destination.writers.UpdateWriter;
import org.junit.jupiter.api.Test;
import com.teradata.fivetran.destination.writers.LoadDataWriter;

import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;

public class UpdateWriterTest extends IntegrationTestBase {

    // Test for updating all types of columns in a table
    @Test
    public void allTypes() throws Exception {
        String tableName = IntegrationTestBase.schema + "_" + "allTypesTable";
        // Create a table with various data types
        createAllTypesTable();

        try (Connection conn = TeradataJDBCUtil.createConnection(conf)) {
            // Retrieve the table metadata
            Table allTypesTable = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").build();
            LoadDataWriter w = new LoadDataWriter(conf, conn, database, allTypesTable.getName(),
                    allTypesTable.getColumnsList(), params, null, 123, false, testWarningHandle);
            w.setHeader(allTypesColumns);
            // Write a row of data into the table
            w.writeRow(List.of(
                    "1", // id
                    "true", // byteintColumn
                    "32767", // smallintColumn
                    "9223372036854775807", // bigintColumn
                    "12345.6789", // decimalColumn
                    "1234.56", // floatColumn
                    "9876.543", // doubleColumn
                    "10:15:30", // timeColumn
                    "2024-01-01", // dateColumn
                    "2024-01-01 12:34:56", // timestampColumn
                    "DEADBEEF1DE", // blobColumn (empty blob)
                    "{\"key\": \"value\"}", // jsonColumn
                    "<root><child>value</child></root>", // xmlColumn
                    "Sample String" // varcharColumn
            ));
            // Commit the data to the table
            w.commit();
            w.deleteInsert();

            // Update the row of data in the table
            UpdateWriter u = new UpdateWriter(conn, database, allTypesTable.getName(), allTypesTable.getColumnsList(),
                    params, null, 123);
            u.setHeader(allTypesColumns);
            u.writeRow(List.of(
                    "1", // id
                    "true", // byteintColumn
                    "1234", // smallintColumn
                    "9223372036854775807", // bigintColumn
                    "12345.6789", // decimalColumn
                    "1234.56", // floatColumn
                    "1234.543", // doubleColumn
                    "10:15:30", // timeColumn
                    "2025-01-01", // dateColumn
                    "2025-01-01 12:34:56", // timestampColumn
                    "DEADBEEF1DE", // blobColumn (empty blob)
                    "{\"key\": \"value\"}", // jsonColumn
                    "<root><child>value</child></root>", // xmlColumn
                    "Sample String" // varcharColumn
            ));
            // Commit the updated data to the table
            u.commit();
        }

        // Verify the data in the table after update
        checkResult("SELECT * FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " ORDER BY id",
                Arrays.asList(Arrays.asList(
                        "1",
                        "1", // byteintColumn
                        "1234", // smallintColumn
                        "9223372036854775807", // bigintColumn
                        "12345.6790", // decimalColumn
                        "1234.56", // floatColumn
                        "1234.543", // doubleColumn
                        "10:15:30", // timeColumn
                        "2025-01-01", // dateColumn
                        "2025-01-01 12:34:56.0", // timestampColumn
                        "DEADBEEF1DE", // blobColumn (hex encoded)
                        "{\"key\": \"value\"}", // jsonColumn
                        "<root><child>value</child></root>", // xmlColumn
                        "Sample String" // varcharColumn
                )));
    }

    // Test for partially updating columns in a table
    @Test
    public void partialUpdate() throws Exception {
        String tableName = IntegrationTestBase.schema + "_" + "partialUpdate";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement()) {
            // Create a table with specified columns
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                    + "(id INT PRIMARY KEY NOT NULL, a INT, b INT)");
            // Insert data into the table
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES(1, 2, 3)");
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES(4, 5, 6)");
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES(7, 8, 9)");
            stmt.execute(
                    "INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES(10, 11, 12)");

            // Retrieve the table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Update specific columns in the table
            UpdateWriter u = new UpdateWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            u.setHeader(List.of("id", "a", "b"));
            u.writeRow(List.of("4", "unm", "1"));
            u.writeRow(List.of("7", "10", "unm"));
            u.writeRow(List.of("10", "unm", "unm"));
            u.writeRow(List.of("unm", "unm", "unm"));
            // Commit the updated data to the table
            u.commit();
        }

        // Verify the data in the table after partial update
        checkResult("SELECT * FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " ORDER BY id",
                Arrays.asList(Arrays.asList("1", "2", "3"), Arrays.asList("4", "5", "1"),
                        Arrays.asList("7", "10", "9"), Arrays.asList("10", "11", "12")));
    }

    // Test for updating a table with all possible byte values
    @Test
    public void allBytes() throws Exception {
        String tableName = IntegrationTestBase.schema + "_" + "allBytes";
        // Create a byte array with all possible byte values
        byte[] data = new byte[256];
        for (int i = 0; i < 256; i++) {
            data[i] = (byte) i;
        }

        // Encode the byte array to Base64
        String dataBase64 = Base64.getEncoder().encodeToString(data);
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
                Statement stmt = conn.createStatement();) {
            // Create a table with a BLOB column
            stmt.executeQuery("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName)
                    + "(a INT PRIMARY KEY NOT NULL, b BLOB, c INT)");
            // Retrieve the table metadata
            Table allBytesTable = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").build();
            LoadDataWriter w = new LoadDataWriter(conf, conn, database, allBytesTable.getName(),
                    allBytesTable.getColumnsList(), params, null, 123, false, testWarningHandle);
            w.setHeader(List.of("a", "b", "c"));
            // Write a row of data into the table
            w.writeRow(List.of("1", dataBase64, "123"));
            // Commit the data to the table
            w.commit();
            w.deleteInsert();

            // Update the row of data in the table
            UpdateWriter u = new UpdateWriter(conn, database, allBytesTable.getName(), allBytesTable.getColumnsList(),
                    params, null, 123);
            u.setHeader(List.of("a", "b", "c"));
            u.writeRow(List.of("1", dataBase64, "456"));
            // Commit the updated data to the table
            u.commit();

            // Verify the data in the table after update
            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName))) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertArrayEquals(data, rs.getBytes(2));
                assertEquals(456, rs.getInt(3));
                assertFalse(rs.next());
            }
        }
    }
}