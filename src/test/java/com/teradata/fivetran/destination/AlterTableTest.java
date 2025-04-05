package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Test;

public class AlterTableTest extends IntegrationTestBase {

    // Test for adding a column to a table
    @Test
    public void addColumn() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            // Create a table with one column
            String fullyQualifiedTableName = TeradataJDBCUtil.escapeTable(conf.database(), IntegrationTestBase.schema, "addColumn");
            stmt.execute("CREATE TABLE " + fullyQualifiedTableName + " (a INT)");
            Table table = Table.newBuilder().setName(IntegrationTestBase.schema + "_addColumn")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.INT).build(),
                            Column.newBuilder().setName("b").setType(DataType.INT).build()))
                    .build();

            // Create an AlterTableRequest to add a new column
            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            // Execute the alter table query
            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }

            // Verify the table structure
            Table result = TeradataJDBCUtil.getTable(conf, database, schema,"addColumn", "addColumn", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());

            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.INT, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());
        }
    }

    // Test for changing the data type of a column
    @Test
    public void changeDataType() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            // Create a table with an integer column
            String fullyQualifiedTableName = TeradataJDBCUtil.escapeTable(conf.database(), IntegrationTestBase.schema, "changeDataType");
            stmt.execute("CREATE TABLE " + fullyQualifiedTableName + " (a INT) NO PRIMARY INDEX");
            stmt.execute("INSERT INTO " + fullyQualifiedTableName + " VALUES (5)");

            // Create an AlterTableRequest to change the column type to STRING
            Table table = Table.newBuilder().setName(IntegrationTestBase.schema + "_changeDataType")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.STRING).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            // Execute the alter table query
            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }

            // Verify the table structure
            Table result = TeradataJDBCUtil.getTable(conf, database, schema, "changeDataType", "changeDataType", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.STRING, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());

            // Verify the data in the table
            checkResult("SELECT * FROM " + fullyQualifiedTableName, Arrays.asList(Arrays.asList("          5")));
        }
    }

    // Test for changing the primary key of a column
    @Test
    public void changeKey() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            // Create a table without a primary key
            String fullyQualifiedTableName = TeradataJDBCUtil.escapeTable(conf.database(), IntegrationTestBase.schema, "changeKey");
            stmt.execute("CREATE TABLE " + fullyQualifiedTableName + " (a INT)");
            Table table = Table.newBuilder().setName(IntegrationTestBase.schema + "_changeKey").addAllColumns(Arrays.asList(Column
                            .newBuilder().setName("a").setType(DataType.INT).setPrimaryKey(true).build()))
                    .build();

            // Create an AlterTableRequest to set the column as primary key
            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            // Execute the alter table query
            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }

            // Verify the table structure
            Table result = TeradataJDBCUtil.getTable(conf, database, schema,"changeKey", "changeKey", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());
        }
    }

    // Test for performing several operations on a table
    @Test
    public void severalOperations() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            // Create a table with two columns
            String fullyQualifiedTableName = TeradataJDBCUtil.escapeTable(conf.database(), IntegrationTestBase.schema, "severalOperations");
            stmt.execute("CREATE TABLE " + fullyQualifiedTableName + "(a INT, b INT)");
            stmt.execute("INSERT INTO " + conf.database() + ".severalOperations VALUES (5, 6)");

            // Create an AlterTableRequest to change column types and add a new column
            Table table = Table.newBuilder().setName("severalOperations")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.LONG).setPrimaryKey(true).build(),
                            Column.newBuilder().setName("b").setType(DataType.LONG).build(),
                            Column.newBuilder().setName("c").setType(DataType.LONG).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            // Execute the alter table query
            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }

            // Verify the table structure
            Table result = TeradataJDBCUtil.getTable(conf, database, schema,"severalOperations", "severalOperations", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.LONG, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.LONG, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());

            assertEquals("c", columns.get(2).getName());
            assertEquals(DataType.LONG, columns.get(2).getType());
            assertEquals(false, columns.get(2).getPrimaryKey());

            // Verify the data in the table
            checkResult("SELECT * FROM " + fullyQualifiedTableName,
                    Arrays.asList(Arrays.asList("5", "6", null)));
        }
    }

    // Test for changing the scale and precision of a decimal column
    @Test
    public void changeScaleAndPrecision() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            // Create a table with a decimal column
            String fullyQualifiedTableName = TeradataJDBCUtil.escapeTable(conf.database(), IntegrationTestBase.schema, "changeScaleAndPrecision");
            stmt.execute("CREATE TABLE " + fullyQualifiedTableName + " (a INT, b DECIMAL(38, 30))");
            stmt.execute("INSERT INTO " + fullyQualifiedTableName + "  VALUES (1, '5.123')");

            // Create an AlterTableRequest to change the scale and precision of the decimal column
            Table table = Table.newBuilder().setName("changeScaleAndPrecision").addAllColumns(
                            Arrays.asList(
                                    Column.newBuilder().setName("a").setType(DataType.INT).build(),
                                    Column.newBuilder().setName("b").setType(DataType.DECIMAL)
                                            .setParams(DataTypeParams.newBuilder()
                                                    .setDecimal(DecimalParams.newBuilder()
                                                            .setScale(5)
                                                            .setPrecision(10))
                                                    .build())
                                            .build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            // Execute the alter table query
            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }

            // Verify the table structure
            Table result = TeradataJDBCUtil.getTable(conf, database, schema, "changeScaleAndPrecision",
                    "changeScaleAndPrecision", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.DECIMAL, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());
            assertEquals(10, columns.get(1).getParams().getDecimal().getPrecision());
            assertEquals(5, columns.get(1).getParams().getDecimal().getScale());

            // Verify the data in the table
            checkResult("SELECT * FROM " + fullyQualifiedTableName,
                    Arrays.asList(Arrays.asList("1", "5.12300")));
        }
    }

    // Test for ignoring different datetime columns
    @Test
    public void shouldIgnoreDifferentDatetimeColumns() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            // Create a table with a naive datetime column
            Table naiveDatetimeTable =
                    Table.newBuilder().setName("shouldIgnoreDifferentDatetimeColumns")
                            .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                                    .setType(DataType.NAIVE_DATETIME).build()))
                            .build();

            // Create a table with a UTC datetime column
            Table utcDatetimeTable =
                    Table.newBuilder().setName("shouldIgnoreDifferentDatetimeColumns")
                            .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                                    .setType(DataType.UTC_DATETIME).build()))
                            .build();

            // Create a CreateTableRequest for the naive datetime table
            CreateTableRequest createRequest = CreateTableRequest.newBuilder()
                    .setSchemaName(database).setTable(naiveDatetimeTable).build();

            // Execute the create table query
            String query = TeradataJDBCUtil.generateCreateTableQuery(conf, stmt, createRequest);
            stmt.execute(query);

            // Create an AlterTableRequest for the UTC datetime table
            AlterTableRequest alterRequest =
                    AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                            .setSchemaName(database).setTable(utcDatetimeTable).build();

            // Generate the alter table query and verify it is null
            query = TeradataJDBCUtil.generateAlterTableQuery(alterRequest, testWarningHandle);
            assertNull(query);
        }
    }

    // Test for changing the type of a primary key column
    @Test
    public void changeTypeOfKey() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            // Create a table with an integer primary key column
            String fullyQualifiedTableName = TeradataJDBCUtil.escapeTable(conf.database(), IntegrationTestBase.schema, "changeTypeOfKey");
            stmt.execute("CREATE TABLE " + fullyQualifiedTableName + " (a INT NOT NULL, PRIMARY KEY(a))");
            stmt.execute("INSERT INTO " + fullyQualifiedTableName + " (a) VALUES (1)");
            stmt.execute("INSERT INTO " + fullyQualifiedTableName + " (a) VALUES (2)");
            Table table = Table.newBuilder().setName("changeTypeOfKey")
                    .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                            .setType(DataType.LONG).setPrimaryKey(true).build()))
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("b").setType(DataType.LONG).build()))
                    .build();

            // Create an AlterTableRequest to change the primary key column type to LONG
            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            // Execute the alter table query
            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }

            // Verify the table structure
            Table result = TeradataJDBCUtil.getTable(conf, database, schema, "changeTypeOfKey", "changeTypeOfKey", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.LONG, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.LONG, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());

            // Verify the data in the table
            checkResult("SELECT * FROM " + fullyQualifiedTableName + " ORDER BY a",
                    Arrays.asList(Arrays.asList("1", null), Arrays.asList("2", null)));
        }
    }
}