package com.teradata.fivetran.destination;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.teradata.fivetran.destination.writers.LoadDataWriter;

import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;

import static org.junit.jupiter.api.Assertions.*;

public class LoadDataWriterTest extends IntegrationTestBase {

    // Test for loading data with various data types into a table
    @Test
    public void allTypes() throws Exception {
        // Create a table with various data types
        createAllTypesTable();

        try (Connection conn = TeradataJDBCUtil.createConnection(conf)) {
            // Retrieve the table metadata
            Table allTypesTable = TeradataJDBCUtil.getTable(conf, database, "allTypesTable", "allTypesTable", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").build();
            LoadDataWriter w = new LoadDataWriter(conn, database, allTypesTable.getName(),
                    allTypesTable.getColumnsList(), params, null, 123, testWarningHandle);
            w.setHeader(List.of(
                    "id", "byteintColumn", "smallintColumn", "bigintColumn", "decimalColumn",
                    "floatColumn", "doubleColumn", "dateColumn", "timestampColumn", "blobColumn",
                    "jsonColumn", "xmlColumn", "varcharColumn"
            ));
            // Write a row of data into the table
            w.writeRow(List.of(
                    "1",                          // id
                    "true",                         // byteintColumn
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

            // Write another row of data into the table
            w.writeRow(List.of(
                    "2",                          // id
                    "false",                         // byteintColumn
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

            // Commit the data to the table
            w.commit();
            w.deleteInsert();
        }

        // Verify the data in the table
        checkResult("SELECT * FROM  " + conf.database() + ".allTypesTable ORDER BY id", Arrays.asList(
                Arrays.asList("1",
                        "1",                    // byteintColumn
                        "32767",                  // smallintColumn
                        "9223372036854775807",    // bigintColumn
                        "12345.6789",             // decimalColumn
                        "1234.56",                // floatColumn
                        "9876.543",             // doubleColumn
                        "2024-01-01",             // dateColumn
                        "2024-01-01 12:34:56.0",    // timestampColumn
                        "DEADBEEF1DE",            // blobColumn (hex encoded)
                        "{\"key\": \"value\"}",   // jsonColumn
                        "<root><child>value</child></root>", // xmlColumn
                        "Sample String"           // varcharColumn
                ),
                Arrays.asList("2",
                        "0",                    // byteintColumn
                        "32767",                  // smallintColumn
                        "9223372036854775807",    // bigintColumn
                        "12345.6789",             // decimalColumn
                        "1234.56",                // floatColumn
                        "9876.543",             // doubleColumn
                        "2024-01-01",             // dateColumn
                        "2024-01-01 12:34:56.0",    // timestampColumn
                        "DEADBEEF1DE",            // blobColumn (hex encoded)
                        "{\"key\": \"value\"}",   // jsonColumn
                        "<root><child>value</child></root>", // xmlColumn
                        "Sample String"           // varcharColumn
                )
        ));
    }

    // Test for loading data with all possible byte values into a table
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
            stmt.executeQuery("CREATE TABLE " + conf.database() + ".allBytes(a INT, b BLOB)");
            Table allBytesTable = TeradataJDBCUtil.getTable(conf, database, "allBytes", "allBytes", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL").build();
            LoadDataWriter w = new LoadDataWriter(conn, database, allBytesTable.getName(),
                    allBytesTable.getColumnsList(), params, null, 123, testWarningHandle);
            w.setHeader(List.of("a", "b"));
            // Write a row of data into the table
            w.writeRow(List.of("1", dataBase64));
            // Commit the data to the table
            w.commit();
            w.deleteInsert();

            // Verify the data in the table
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + conf.database() + ".allBytes")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertArrayEquals(data, rs.getBytes(2));
                assertFalse(rs.next());
            }
        }
    }
}