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

    @Test
    public void addColumn() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute("CREATE TABLE " + conf.database() + ".addColumn(a INT)");
            Table table = Table.newBuilder().setName("addColumn")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.INT).build(),
                            Column.newBuilder().setName("b").setType(DataType.INT).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }
            Table result = TeradataJDBCUtil.getTable(conf, database, "addColumn", "addColumn", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());

            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.INT, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());
        }
    }

    @Test
    public void changeDataType() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute("CREATE TABLE " + conf.database() + ".changeDataType(a INT) NO PRIMARY INDEX");
            stmt.execute("INSERT INTO " + conf.database() + ".changeDataType VALUES (5)");

            Table table = Table.newBuilder().setName("changeDataType")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.STRING).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }
            Table result = TeradataJDBCUtil.getTable(conf, database, "changeDataType", "changeDataType", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.STRING, columns.get(0).getType());
            assertEquals(false, columns.get(0).getPrimaryKey());

            checkResult("SELECT * FROM " + conf.database() + ".changeDataType", Arrays.
                    asList(Arrays.asList("          5")));
        }
    }

    @Test
    public void changeKey() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute("CREATE TABLE " + conf.database() + ".changeKey(a INT)");
            Table table = Table.newBuilder().setName("changeKey").addAllColumns(Arrays.asList(Column
                            .newBuilder().setName("a").setType(DataType.INT).setPrimaryKey(true).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }
            Table result = TeradataJDBCUtil.getTable(conf, database, "changeKey", "changeKey", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

        }
    }

    @Test
    public void severalOperations() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute("CREATE TABLE " + conf.database() + ".severalOperations(a INT, b INT)");
            stmt.execute("INSERT INTO " + conf.database() + ".severalOperations VALUES (5, 6)");

            Table table = Table.newBuilder().setName("severalOperations")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.LONG).setPrimaryKey(true).build(),
                            Column.newBuilder().setName("b").setType(DataType.LONG).build(),
                            Column.newBuilder().setName("c").setType(DataType.LONG).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }
            Table result =
                    TeradataJDBCUtil.getTable(conf, database, "severalOperations", "severalOperations", testWarningHandle);
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

            checkResult("SELECT * FROM " + conf.database() + ".severalOperations",
                    Arrays.asList(Arrays.asList("5", "6", null)));
        }
    }

    @Test
    public void changeScaleAndPrecision() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute("CREATE TABLE " + conf.database() + ".changeScaleAndPrecision(a INT, b DECIMAL(38, 30))");
            stmt.execute("INSERT INTO " + conf.database() + ".changeScaleAndPrecision VALUES (1, '5.123')");

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

            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);
            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }
            Table result = TeradataJDBCUtil.getTable(conf, database, "changeScaleAndPrecision",
                    "changeScaleAndPrecision", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.INT, columns.get(0).getType());
            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.DECIMAL, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());
            assertEquals(10, columns.get(1).getParams().getDecimal().getPrecision());
            assertEquals(5, columns.get(1).getParams().getDecimal().getScale());

            checkResult("SELECT * FROM " + conf.database() + ".changeScaleAndPrecision",
                    Arrays.asList(Arrays.asList("1", "5.12300")));
        }
    }

    @Test
    public void shouldIgnoreDifferentDatetimeColumns() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            Table naiveDatetimeTable =
                    Table.newBuilder().setName("shouldIgnoreDifferentDatetimeColumns")
                            .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                                    .setType(DataType.NAIVE_DATETIME).build()))
                            .build();

            Table utcDatetimeTable =
                    Table.newBuilder().setName("shouldIgnoreDifferentDatetimeColumns")
                            .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                                    .setType(DataType.UTC_DATETIME).build()))
                            .build();

            CreateTableRequest createRequest = CreateTableRequest.newBuilder()
                    .setSchemaName(database).setTable(naiveDatetimeTable).build();

            String query = TeradataJDBCUtil.generateCreateTableQuery(conf, stmt, createRequest);
            stmt.execute(query);

            AlterTableRequest alterRequest =
                    AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                            .setSchemaName(database).setTable(utcDatetimeTable).build();

            query = TeradataJDBCUtil.generateAlterTableQuery(alterRequest, testWarningHandle);
            assertNull(query);
        }
    }

    @Test
    public void changeTypeOfKey() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.execute(
                    String.format("CREATE TABLE %s.changeTypeOfKey(a INT NOT NULL, PRIMARY KEY(a))", database));
            stmt.execute(String.format("INSERT INTO %s.changeTypeOfKey (a) VALUES (1)", database));
            stmt.execute(String.format("INSERT INTO %s.changeTypeOfKey (a) VALUES (2)", database));
            Table table = Table.newBuilder().setName("changeTypeOfKey")
                    .addAllColumns(Arrays.asList(Column.newBuilder().setName("a")
                            .setType(DataType.LONG).setPrimaryKey(true).build()))
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("b").setType(DataType.LONG).build()))
                    .build();

            AlterTableRequest request = AlterTableRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTable(table).build();

            String query = TeradataJDBCUtil.generateAlterTableQuery(request, testWarningHandle);

            String[] queries = query.split(";");
            for (String q : queries) {
                if (!q.trim().isEmpty()) {
                    stmt.execute(q.trim() + ";");
                }
            }
            Table result = TeradataJDBCUtil.getTable(conf, database, "changeTypeOfKey", "changeTypeOfKey", testWarningHandle);
            List<Column> columns = result.getColumnsList();

            assertEquals("a", columns.get(0).getName());
            assertEquals(DataType.LONG, columns.get(0).getType());
            assertEquals(true, columns.get(0).getPrimaryKey());

            assertEquals("b", columns.get(1).getName());
            assertEquals(DataType.LONG, columns.get(1).getType());
            assertEquals(false, columns.get(1).getPrimaryKey());

            checkResult(String.format("SELECT * FROM %s.changeTypeOfKey ORDER BY a", database),
                    Arrays.asList(Arrays.asList("1", null), Arrays.asList("2", null)));
        }
    }
}