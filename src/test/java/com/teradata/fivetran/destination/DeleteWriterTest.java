package com.teradata.fivetran.destination;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import com.teradata.fivetran.destination.writers.DeleteWriter;
import com.teradata.fivetran.destination.writers.LoadDataWriter;
import org.junit.jupiter.api.Test;

import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;

public class DeleteWriterTest extends IntegrationTestBase {

    // Test for deleting all rows from a table with various data types
    @Test
    public void allTypesTest() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            // Create a table with various data types
            stmt.execute("CREATE TABLE " + conf.database() + ".allTypesTableBigKey (\n" +
                    "  id INTEGER PRIMARY KEY NOT NULL,\n" +
                    "  byteintColumn BYTEINT,\n" +
                    "  smallintColumn SMALLINT,\n" +
                    "  bigintColumn BIGINT,\n" +
                    "  decimalColumn DECIMAL(18,4),\n" +
                    "  floatColumn FLOAT,\n" +
                    "  doubleColumn DOUBLE PRECISION,\n" +
                    "  dateColumn DATE,\n" +
                    "  timestampColumn TIMESTAMP,\n" +
                    "  blobColumn BLOB,\n" +
                    "  jsonColumn JSON,\n" +
                    "  xmlColumn XML,\n" +
                    "  varcharColumn VARCHAR(100)\n" +
                    ")");

            // Get the table metadata
            Table allTypesTableBigKey =
                    TeradataJDBCUtil.getTable(conf, database, "allTypesTableBigKey", "allTypesTableBigKey", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").build();

            // Load data into the table
            LoadDataWriter w = new LoadDataWriter(conn, database, allTypesTableBigKey.getName(),
                    allTypesTableBigKey.getColumnsList(), params, null, 123, testWarningHandle);
            w.setHeader(allTypesColumns);
            w.writeRow(List.of(
                    "1",                          // id
                    "1",                         // byteintColumn
                    "32767",                       // smallintColumn
                    "9223372036854775807",         // bigintColumn
                    "12345.6789",                  // decimalColumn
                    "1234.56",                     // floatColumn
                    "9876.543",                  // doubleColumn
                    "2024-01-01",                  // dateColumn
                    "2024-01-01 12:34:56",         // timestampColumn
                    "DEADBEEF1DE",              // blobColumn (empty blob)
                    "{\"key\": \"value\"}",        // jsonColumn
                    "<root><child>value</child></root>",  // xmlColumn
                    "Sample String"                // varcharColumn
            ));
            w.commit();

            // Delete the data from the table
            DeleteWriter d = new DeleteWriter(conn, database, allTypesTableBigKey.getName(),
                    allTypesTableBigKey.getColumnsList(), params, null, 123);
            d.setHeader(allTypesColumns);
            d.writeRow(List.of(
                    "1",                          // id
                    "1",                         // byteintColumn
                    "32767",                       // smallintColumn
                    "9223372036854775807",         // bigintColumn
                    "12345.6789",                  // decimalColumn
                    "1234.56",                     // floatColumn
                    "9876.543",                  // doubleColumn
                    "2024-01-01",                  // dateColumn
                    "2024-01-01 12:34:56",         // timestampColumn
                    "DEADBEEF1DE",              // blobColumn (empty blob)
                    "{\"key\": \"value\"}",        // jsonColumn
                    "<root><child>value</child></root>",  // xmlColumn
                    "Sample String"                // varcharColumn
            ));
            d.commit();
        }

        // Verify that the table is empty
        checkResult("SELECT * FROM " + conf.database() + ".allTypesTableBigKey ORDER BY id", Arrays.asList());
    }

    // Test for deleting specific rows from a table
    @Test
    public void deletePartOfRows() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table and insert data
            stmt.execute("CREATE TABLE " + conf.database() + ".deletePartOfRows(id INT PRIMARY KEY NOT NULL, a INT, b INT)");
            stmt.execute("INSERT INTO " + conf.database() + ".deletePartOfRows VALUES(1, 2, 3)");
            stmt.execute("INSERT INTO " + conf.database() + ".deletePartOfRows VALUES(4, 5, 6)");
            stmt.execute("INSERT INTO " + conf.database() + ".deletePartOfRows VALUES(7, 8, 9)");
            stmt.execute("INSERT INTO " + conf.database() + ".deletePartOfRows VALUES(10, 11, 12)");

            // Get the table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, "deletePartOfRows", "deletePartOfRows", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Delete specific rows from the table
            DeleteWriter d = new DeleteWriter(conn, database, t.getName(), t.getColumnsList(),
                    params, null, 123);
            d.setHeader(List.of("id", "a", "b"));
            d.writeRow(List.of("4", "unm", "unm"));
            d.writeRow(List.of("7", "unm", "unm"));
            d.writeRow(List.of("100", "unm", "unm"));
            d.commit();
        }

        // Verify the remaining data in the table
        checkResult("SELECT * FROM " + conf.database() + ".deletePartOfRows ORDER BY id",
                Arrays.asList(Arrays.asList("1", "2", "3"), Arrays.asList("10", "11", "12")));
    }

    // Test for deleting rows with BLOB data
    @Test
    public void allBytes() throws Exception {
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
            stmt.executeQuery("CREATE TABLE " + conf.database() + ".allBytes(a INT PRIMARY KEY NOT NULL, b BLOB, c INT)");
            Table allBytesTable = TeradataJDBCUtil.getTable(conf, database, "allBytes", "allBytes", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").build();

            // Load data into the table
            LoadDataWriter w = new LoadDataWriter(conn, database, allBytesTable.getName(), allBytesTable.getColumnsList(), params, null, 123, testWarningHandle);
            w.setHeader(List.of("a", "b", "c"));
            w.writeRow(List.of("1", dataBase64, "123"));
            w.commit();

            // Delete the data from the table
            DeleteWriter d = new DeleteWriter(conn, database, allBytesTable.getName(),
                    allBytesTable.getColumnsList(), params, null, 123);
            d.setHeader(List.of("a", "b", "c"));
            d.writeRow(List.of("1", dataBase64, "123"));
            d.commit();
        }

        // Verify that the table is empty
        checkResult("SELECT * FROM " + conf.database() + ".allBytes", Arrays.asList());
    }

    // Test for deleting rows in batches
    @Test
    public void batchSize() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table
            stmt.execute("CREATE TABLE " + conf.database() + ".batchSize(id INT PRIMARY KEY NOT NULL)");

            FileParams params = FileParams.newBuilder().setNullString("NULL").build();
            Table t = TeradataJDBCUtil.getTable(conf, database, "batchSize", "batchSize", testWarningHandle);

            // Load data into the table in batches
            LoadDataWriter w = new LoadDataWriter(conn, database, t.getName(), t.getColumnsList(),
                    params, null, 1000, testWarningHandle);
            StringBuilder data = new StringBuilder("id\n");
            for (Integer i = 0; i < 2000; i++) {
                data.append(i.toString() + "\n");
            }
            w.write(null, new ByteArrayInputStream(data.toString().getBytes()));
            w.commit();

            // Delete data from the table in batches
            DeleteWriter d = new DeleteWriter(conn, database, t.getName(), t.getColumnsList(),
                    params, null, 1010);
            data = new StringBuilder("id\n");
            for (Integer i = 0; i < 100; i++) {
                data.append(i.toString() + "\n");
            }
            d.write(null, new ByteArrayInputStream(data.toString().getBytes()));
            d.commit();
        }

        // Verify the remaining data in the table
        List<List<String>> res = new ArrayList<>();
        for (Integer i = 100; i < 2000; i++) {
            res.add(Arrays.asList(i.toString()));
        }

        checkResult("SELECT * FROM " + conf.database() + ".batchSize ORDER BY id", res);
    }
}