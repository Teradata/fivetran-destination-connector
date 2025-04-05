package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Test;

import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.Table;

public class DescribeTableTest extends IntegrationTestBase {

    // Test for describing a table with various data types
    @Test
    public void allTypes() throws Exception {
        // Create a table with various data types
        createAllTypesTable();

        try (Connection conn = TeradataJDBCUtil.createConnection(conf)) {
            // Retrieve the table metadata
            Table allTypesTable = TeradataJDBCUtil.getTable(conf, database, "allTypesTable", "allTypesTable", testWarningHandle);
            assertEquals("allTypesTable", allTypesTable.getName());
            List<Column> columns = allTypesTable.getColumnsList();

            // Verify the table structure
            assertEquals("id", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            assertEquals("byteintColumn", columns.get(1).getName());
            assertEquals(DataType.BOOLEAN, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());

            assertEquals("smallintColumn", columns.get(2).getName());
            assertEquals(DataType.SHORT, columns.get(2).getType());
            assertEquals(false, columns.get(2).getPrimaryKey());

            assertEquals("bigintColumn", columns.get(3).getName());
            assertEquals(DataType.LONG, columns.get(3).getType());
            assertEquals(false, columns.get(3).getPrimaryKey());

            assertEquals("decimalColumn", columns.get(4).getName());
            assertEquals(DataType.DECIMAL, columns.get(4).getType());
            assertEquals(false, columns.get(4).getPrimaryKey());

            assertEquals("floatColumn", columns.get(5).getName());
            assertEquals(DataType.FLOAT, columns.get(5).getType());
            assertEquals(false, columns.get(5).getPrimaryKey());

            assertEquals("doubleColumn", columns.get(6).getName());
            assertEquals(DataType.FLOAT, columns.get(6).getType());
            assertEquals(false, columns.get(6).getPrimaryKey());

            assertEquals("dateColumn", columns.get(7).getName());
            assertEquals(DataType.NAIVE_DATE, columns.get(7).getType());
            assertEquals(false, columns.get(7).getPrimaryKey());

            assertEquals("timestampColumn", columns.get(8).getName());
            assertEquals(DataType.NAIVE_DATETIME, columns.get(8).getType());
            assertEquals(false, columns.get(8).getPrimaryKey());

            assertEquals("blobColumn", columns.get(9).getName());
            assertEquals(DataType.BINARY, columns.get(9).getType());
            assertEquals(false, columns.get(9).getPrimaryKey());

            assertEquals("jsonColumn", columns.get(10).getName());
            assertEquals(DataType.JSON, columns.get(10).getType());
            assertEquals(false, columns.get(10).getPrimaryKey());

            assertEquals("xmlColumn", columns.get(11).getName());
            assertEquals(DataType.XML, columns.get(11).getType());
            assertEquals(false, columns.get(11).getPrimaryKey());

            assertEquals("varcharColumn", columns.get(12).getName());
            assertEquals(DataType.STRING, columns.get(12).getType());
            assertEquals(false, columns.get(12).getPrimaryKey());
        }
    }

    // Test for describing a table with decimal columns having different scale and precision
    @Test
    public void scaleAndPrecision() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            // Create a table with decimal columns
            stmt.executeQuery("CREATE TABLE " + conf.database() + ".scaleAndPrecision(" + "dec1 DECIMAL(38, 30), "
                    + "dec2 DECIMAL(10, 5)" + ")");

            // Retrieve the table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, "scaleAndPrecision", "scaleAndPrecision", testWarningHandle);
            assertEquals("scaleAndPrecision", t.getName());
            List<Column> columns = t.getColumnsList();

            // Verify the table structure
            assertEquals("dec1", columns.get(0).getName());
            assertEquals(DataType.DECIMAL, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());
            assertEquals(38, columns.get(0).getParams().getDecimal().getPrecision());
            assertEquals(30, columns.get(0).getParams().getDecimal().getScale());

            assertEquals("dec2", columns.get(1).getName());
            assertEquals(DataType.DECIMAL, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());
            assertEquals(10, columns.get(1).getParams().getDecimal().getPrecision());
            assertEquals(5, columns.get(1).getParams().getDecimal().getScale());
        }
    }
}