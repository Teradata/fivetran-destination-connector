package com.teradata.fivetran.destination;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MigrateTest extends IntegrationTestBase {
    @Test
    public void dropTable() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE dropTable");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE dropTable(a INT)");
            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("dropTable")
                            .setSchema(database)
                            .setDrop(
                                    DropOperation.newBuilder()
                                            .setDropTable(true)
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            Assertions.assertThrows(TableNotExistException.class, () -> {
                TeradataJDBCUtil.getTable(conf, database, "dropTable", "dropTable", testWarningHandle);
            });
        }
    }

    @Test
    public void renameTable() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE renameTable");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE renameTable(a INT)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("renameTable")
                            .setSchema(database)
                            .setRename(
                                    RenameOperation.newBuilder()
                                            .setRenameTable(
                                                    RenameTable.newBuilder()
                                                            .setFromTable("renameTable")
                                                            .setToTable("renameTable1")
                                            )
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table renamed = TeradataJDBCUtil.getTable(conf, database, "renameTable1", "renameTable1", testWarningHandle);
            Assertions.assertEquals("renameTable1", renamed.getName());
        }
    }

    @Test
    public void renameColumn() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE renameColumn");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE renameColumn(a INT)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("renameColumn")
                            .setSchema(database)
                            .setRename(
                                    RenameOperation.newBuilder()
                                            .setRenameColumn(
                                                    RenameColumn.newBuilder()
                                                            .setFromColumn("a")
                                                            .setToColumn("b")
                                            )
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table renamed = TeradataJDBCUtil.getTable(conf, database, "renameColumn", "renameTable1", testWarningHandle);
            renamed.getColumnsList().forEach(c -> Assertions.assertEquals("b", c.getName()));
        }
    }

    @Test
    public void copyTable() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE copyTable");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE copyTable(a INT)");
            stmt.execute("INSERT INTO copyTable VALUES (1), (2), (3)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyTable")
                            .setSchema(database)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyTable(
                                                    CopyTable.newBuilder()
                                                            .setFromTable("copyTable")
                                                            .setToTable("copyTable1")
                                            )
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table originalTable = TeradataJDBCUtil.getTable(conf, database, "copyTable", "copyTable", testWarningHandle);
            Assertions.assertEquals("copyTable", originalTable.getName());

            Table copy = TeradataJDBCUtil.getTable(conf, database, "copyTable1", "copyTable1", testWarningHandle);
            Assertions.assertEquals("copyTable1", copy.getName());

            checkResult("SELECT * FROM `copyTable1` ORDER BY a",
                    Arrays.asList(Collections.singletonList("1"),
                            Collections.singletonList("2"),
                            Collections.singletonList("3")));
        }
    }

    @Test
    public void copyColumn() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE dropTable");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE dropTable(a DECIMAL(8, 4))");
            stmt.execute("INSERT INTO copyColumn VALUES (123), (124)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyColumn")
                            .setSchema(database)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyColumn(
                                                    CopyColumn.newBuilder()
                                                            .setFromColumn("a")
                                                            .setToColumn("b")
                                            )
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table copy = TeradataJDBCUtil.getTable(conf, database, "copyColumn", "copyColumn", testWarningHandle);
            List<Column> columns = copy.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("b", columns.get(1).getName());
            Assertions.assertEquals(8, columns.get(1).getParams().getDecimal().getPrecision());
            Assertions.assertEquals(4, columns.get(1).getParams().getDecimal().getScale());

            checkResult("SELECT a, b FROM copyColumn ORDER BY a", Arrays.asList(
                    Arrays.asList("123.0000", "123.0000"),
                    Arrays.asList("124.0000", "124.0000")
            ));
        }
    }

    @Test
    public void addColumnWithDefaultValue() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE addColumnWithDefaultValue");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE addColumnWithDefaultValue(a INT)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("addColumnWithDefaultValue")
                            .setSchema(database)
                            .setAdd(AddOperation.newBuilder()
                                    .setAddColumnWithDefaultValue(
                                            AddColumnWithDefaultValue.newBuilder()
                                                    .setColumn("b")
                                                    .setDefaultValue("1")
                                                    .setColumnType(DataType.INT)
                                                    .build()
                                    )
                            )
                    )
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                q.execute(conn);
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "addColumnWithDefaultValue", "addColumnWithDefaultValue", testWarningHandle);
            Optional<Column> optionalB = t.getColumnsList().stream().filter(c -> c.getName().equals("b")).findFirst();
            Assertions.assertTrue(optionalB.isPresent());
            Column b = optionalB.get();
            Assertions.assertEquals("b", b.getName());
            Assertions.assertEquals(DataType.INT, b.getType());

            request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("addColumnWithDefaultValue")
                            .setSchema(database)
                            .setAdd(AddOperation.newBuilder()
                                    .setAddColumnWithDefaultValue(
                                            AddColumnWithDefaultValue.newBuilder()
                                                    .setColumn("c")
                                                    .setDefaultValue("2025-11-24T10:14:54.123Z")
                                                    .setColumnType(DataType.NAIVE_DATETIME)
                                                    .build()
                                    )
                            )
                    )
                    .build();

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                q.execute(conn);
            }

            t = TeradataJDBCUtil.getTable(conf, database, "addColumnWithDefaultValue", "addColumnWithDefaultValue", testWarningHandle);
            Optional<Column> optionalC = t.getColumnsList().stream().filter(c -> c.getName().equals("c")).findFirst();
            Assertions.assertTrue(optionalC.isPresent());
            Column c = optionalC.get();
            Assertions.assertEquals("c", c.getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, c.getType());
        }
    }

    @Test
    public void updateColumnValueOperation() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE updateColumnValueOperation");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE updateColumnValueOperation(a INT)");
            stmt.execute("INSERT INTO updateColumnValueOperation VALUES (1), (2), (3)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("updateColumnValueOperation")
                            .setSchema(database)
                            .setUpdateColumnValue(UpdateColumnValueOperation.newBuilder()
                                    .setColumn("a")
                                    .setValue("4")
                                    .build()
                            )
                    )
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                q.execute(conn);
            }

            checkResult("SELECT * FROM updateColumnValueOperation ORDER BY a", Arrays.asList(
                    Collections.singletonList("4"),
                    Collections.singletonList("4"),
                    Collections.singletonList("4")
            ));
        }
    }

    @Test
    public void liveToSoftDelete() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE liveToSoftDelete");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE liveToSoftDelete(a INT)");
            stmt.execute("INSERT INTO liveToSoftDelete VALUES (1), (2)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("liveToSoftDelete")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.LIVE_TO_SOFT_DELETE)
                                            .setSoftDeletedColumn("b")
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "liveToSoftDelete", "liveToSoftDelete", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("b", columns.get(1).getName());

            checkResult("SELECT a, b FROM liveToSoftDelete ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "0"),
                    Arrays.asList("2", "0")
            ));
        }
    }

    @Test
    public void liveToHistory() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE liveToHistory");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE liveToHistory(a INT PRIMARY KEY)");
            stmt.execute("INSERT INTO liveToHistory VALUES (1), (2)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("liveToHistory")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.LIVE_TO_HISTORY)
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "liveToHistory", "liveToHistory", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(1).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(3).getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(1).getType());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(2).getType());
            Assertions.assertEquals(DataType.BOOLEAN, columns.get(3).getType());
            Assertions.assertTrue(columns.get(1).getPrimaryKey());

            checkResult("SELECT a, _fivetran_end, _fivetran_active FROM liveToHistory ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "9999-12-31 23:59:59.999999", "1"),
                    Arrays.asList("2", "9999-12-31 23:59:59.999999", "1")
            ));
        }
    }

    @Test
    public void softDeleteToHistory() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE softDeleteToHistory");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE softDeleteToHistory(a INT PRIMARY KEY, _fivetran_synced DATETIME(6), _fivetran_deleted BOOL)");
            stmt.execute("INSERT INTO softDeleteToHistory VALUES (1, '2020-01-01 01:01:01', 0), (2, '2020-01-01 01:01:01', 1)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("softDeleteToHistory")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.SOFT_DELETE_TO_HISTORY)
                                            .setSoftDeletedColumn("_fivetran_deleted")
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "softDeleteToHistory", "softDeleteToHistory", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(3).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(4).getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(2).getType());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(3).getType());
            Assertions.assertEquals(DataType.BOOLEAN, columns.get(4).getType());
            Assertions.assertTrue(columns.get(2).getPrimaryKey());

            checkResult("SELECT a, _fivetran_start, _fivetran_end, _fivetran_active FROM softDeleteToHistory ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "2020-01-01 01:01:01.000000", "9999-12-31 23:59:59.999999", "1"),
                    Arrays.asList("2", "1000-01-01 00:00:00.000000", "1000-01-01 00:00:00.000000", "0")
            ));
        }
    }

    @Test
    public void softDeleteToLive() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE softDeleteToLive");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE softDeleteToLive(a INT PRIMARY KEY, _fivetran_deleted BOOL)");
            stmt.execute("INSERT INTO softDeleteToLive VALUES (1, 0), (2, 1)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("softDeleteToLive")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.SOFT_DELETE_TO_LIVE)
                                            .setSoftDeletedColumn("_fivetran_deleted")
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "softDeleteToLive", "softDeleteToLive", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals(1, columns.size());

            checkResult("SELECT a FROM softDeleteToLive ORDER BY a", Collections.singletonList(
                    Collections.singletonList("1")
            ));
        }
    }

    @Test
    public void historyToSoftDelete() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE historyToSoftDelete");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE historyToSoftDelete(a INT, _fivetran_start DATETIME(6), _fivetran_end DATETIME(6), _fivetran_active BOOL, PRIMARY KEY(_fivetran_start, a))");
            stmt.execute("INSERT INTO historyToSoftDelete VALUES " +
                    "(1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0), " +
                    "(1, '2021-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1), " +
                    "(2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1), " +
                    "(3, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("historyToSoftDelete")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.HISTORY_TO_SOFT_DELETE)
                                            .setSoftDeletedColumn("_fivetran_deleted")
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "historyToSoftDelete", "historyToSoftDelete", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_deleted", columns.get(1).getName());
            Assertions.assertEquals(2, columns.size());

            checkResult("SELECT a, _fivetran_deleted FROM historyToSoftDelete ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "0"),
                    Arrays.asList("2", "0"),
                    Arrays.asList("3", "1")
            ));

            try {
                stmt.execute("DROP TABLE historyToSoftDelete");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE historyToSoftDelete(a INT, _fivetran_start DATETIME(6), _fivetran_end DATETIME(6), _fivetran_active BOOL, PRIMARY KEY(_fivetran_start))");
            stmt.execute("INSERT INTO historyToSoftDelete VALUES " +
                    "(1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0), " +
                    "(1, '2021-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1), " +
                    "(2, '2022-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1), " +
                    "(3, '2023-01-01 01:01:01', '2021-01-01 01:01:01', 0)");

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                System.out.println(q.getQuery());
                stmt.execute(q.getQuery());
            }

            t = TeradataJDBCUtil.getTable(conf, database, "historyToSoftDelete", "historyToSoftDelete", testWarningHandle);
            columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_deleted", columns.get(1).getName());
            Assertions.assertEquals(2, columns.size());

            checkResult("SELECT a, _fivetran_deleted FROM historyToSoftDelete ORDER BY a, _fivetran_deleted", Arrays.asList(
                    Arrays.asList("1", "0"),
                    Arrays.asList("1", "1"),
                    Arrays.asList("2", "0"),
                    Arrays.asList("3", "1")
            ));
        }
    }

    @Test
    public void historyToLive() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE historyToLive");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE historyToLive(a INT PRIMARY KEY, _fivetran_start DATETIME(6), _fivetran_end DATETIME(6), _fivetran_active BOOL)");
            stmt.execute("INSERT INTO historyToLive VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0), (2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("historyToLive")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.HISTORY_TO_LIVE)
                                            .setKeepDeletedRows(true)
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "historyToLive", "historyToLive", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals(1, columns.size());

            checkResult("SELECT a FROM historyToLive ORDER BY a", Arrays.asList(
                    Collections.singletonList("1"),
                    Collections.singletonList("2")
            ));
            try {
                stmt.execute("DROP TABLE historyToLive");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE historyToLive(a INT PRIMARY KEY, _fivetran_start DATETIME(6), _fivetran_end DATETIME(6), _fivetran_active BOOL)");
            stmt.execute("INSERT INTO historyToLive VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0), (2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)");

            request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("historyToLive")
                            .setSchema(database)
                            .setTableSyncModeMigration(
                                    TableSyncModeMigrationOperation.newBuilder()
                                            .setType(TableSyncModeMigrationType.HISTORY_TO_LIVE)
                                            .setKeepDeletedRows(false)
                            ))
                    .build();

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            t = TeradataJDBCUtil.getTable(conf, database, "historyToLive", "historyToLive", testWarningHandle);
            columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals(1, columns.size());

            checkResult("SELECT a FROM historyToLive ORDER BY a", Collections.singletonList(
                    Collections.singletonList("2")
            ));
        }
    }

    @Test
    public void copyTableToHistoryMode() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE copyTableToHistoryMode");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE copyTableToHistoryMode(a INT PRIMARY KEY, _fivetran_synced DATETIME(6), _fivetran_deleted BOOL)");
            stmt.execute("INSERT INTO copyTableToHistoryMode VALUES (1, '2020-01-01 01:01:01', 0), (2, '2020-01-01 01:01:01', 1)");

            MigrateRequest request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyTableToHistoryMode")
                            .setSchema(database)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyTableToHistoryMode(
                                                    CopyTableToHistoryMode.newBuilder()
                                                            .setFromTable("copyTableToHistoryMode")
                                                            .setToTable("copyTableToHistoryModeNew")
                                                            .setSoftDeletedColumn("_fivetran_deleted")
                                            )
                            ))
                    .build();

            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "copyTableToHistoryModeNew", "copyTableToHistoryModeNew", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(3).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(4).getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(2).getType());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(3).getType());
            Assertions.assertEquals(DataType.BOOLEAN, columns.get(4).getType());
            Assertions.assertTrue(columns.get(2).getPrimaryKey());

            checkResult("SELECT a, _fivetran_start, _fivetran_end, _fivetran_active FROM copyTableToHistoryModeNew ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "2020-01-01 01:01:01.000000", "9999-12-31 23:59:59.999999", "1"),
                    Arrays.asList("2", "1000-01-01 00:00:00.000000", "1000-01-01 00:00:00.000000", "0")
            ));
            try {
                stmt.execute("DROP TABLE copyTableToHistoryMode");
            } catch (Exception e) {
                // handle or ignore
            }
            try {
                stmt.execute("DROP TABLE copyTableToHistoryModeNew");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("DROP TABLE copyTableToHistoryMode");
            stmt.execute("DROP TABLE copyTableToHistoryModeNew");
            stmt.execute("CREATE TABLE copyTableToHistoryMode(a INT PRIMARY KEY)");
            stmt.execute("INSERT INTO copyTableToHistoryMode VALUES (1), (2)");

            request = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                            .setTable("copyTableToHistoryMode")
                            .setSchema(database)
                            .setCopy(
                                    CopyOperation.newBuilder()
                                            .setCopyTableToHistoryMode(
                                                    CopyTableToHistoryMode.newBuilder()
                                                            .setFromTable("copyTableToHistoryMode")
                                                            .setToTable("copyTableToHistoryModeNew")
                                            )
                            ))
                    .build();

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                stmt.execute(q.getQuery());
            }

            t = TeradataJDBCUtil.getTable(conf, database, "copyTableToHistoryModeNew", "copyTableToHistoryModeNew", testWarningHandle);
            columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(1).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(3).getName());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(1).getType());
            Assertions.assertEquals(DataType.NAIVE_DATETIME, columns.get(2).getType());
            Assertions.assertEquals(DataType.BOOLEAN, columns.get(3).getType());
            Assertions.assertTrue(columns.get(1).getPrimaryKey());

            checkResult("SELECT a, _fivetran_end, _fivetran_active FROM copyTableToHistoryModeNew ORDER BY a", Arrays.asList(
                    Arrays.asList("1", "9999-12-31 23:59:59.999999", "1"),
                    Arrays.asList("2", "9999-12-31 23:59:59.999999", "1")
            ));
        }
    }

    @Test
    public void dropColumnInHistoryMode() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE dropColumnInHistoryMode");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE dropColumnInHistoryMode(a INT, b INT, _fivetran_start DATETIME(6), _fivetran_end DATETIME(6), _fivetran_active BOOL, PRIMARY KEY(_fivetran_start, a))");

            final MigrateRequest emptyRequest = MigrateRequest.newBuilder()
                .putAllConfiguration(confMap)
                .setDetails(MigrationDetails.newBuilder()
                    .setTable("dropColumnInHistoryMode")
                    .setSchema(database)
                    .setDrop(
                        DropOperation.newBuilder()
                            .setDropColumnInHistoryMode(
                                DropColumnInHistoryMode.newBuilder()
                                    .setColumn("b")
                                    .setOperationTimestamp("2021-01-01 01:01:01")
                            )
                    )
                ).build();
            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(emptyRequest, testWarningHandle);
            Assertions.assertEquals(0, queries.size());

            stmt.execute("INSERT INTO dropColumnInHistoryMode VALUES (1, 1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0), (2, 2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)");

            MigrateRequest request = MigrateRequest.newBuilder()
                .putAllConfiguration(confMap)
                .setDetails(MigrationDetails.newBuilder()
                    .setTable("dropColumnInHistoryMode")
                    .setSchema(database)
                    .setDrop(
                        DropOperation.newBuilder()
                            .setDropColumnInHistoryMode(
                                DropColumnInHistoryMode.newBuilder()
                                    .setColumn("b")
                                    .setOperationTimestamp("2021-01-01 01:01:01")
                            )
                    )
                ).build();

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                q.execute(conn);
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "dropColumnInHistoryMode", "dropColumnInHistoryMode", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("b", columns.get(1).getName());
            Assertions.assertEquals("_fivetran_start", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(3).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(4).getName());
            Assertions.assertEquals(5, columns.size());

            checkResult("SELECT * FROM dropColumnInHistoryMode ORDER BY _fivetran_start, a", Arrays.asList(
                Arrays.asList("1", "1", "2020-01-01 01:01:01.000000", "2021-01-01 01:01:01.000000", "0"),
                Arrays.asList("2", "2", "2020-01-01 01:01:01.000000", "2021-01-01 01:01:00.999999", "0"),
                Arrays.asList("2", null, "2021-01-01 01:01:01.000000", "9999-12-31 11:59:59.999999", "1")
            ));

            Assertions.assertThrows(IllegalArgumentException.class, () -> {
                final MigrateRequest invalidRequest = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                        .setTable("dropColumnInHistoryMode")
                        .setSchema(database)
                        .setDrop(
                            DropOperation.newBuilder()
                                .setDropColumnInHistoryMode(
                                    DropColumnInHistoryMode.newBuilder()
                                        .setColumn("b")
                                        .setOperationTimestamp("2001-01-01 01:01:01")
                                )
                        )
                    ).build();
                TeradataJDBCUtil.generateMigrateQueries(invalidRequest, testWarningHandle);
            });
        }
    }

    @Test
    public void addColumnInHistoryMode() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            try {
                stmt.execute("DROP TABLE addColumnInHistoryMode");
            } catch (Exception e) {
                // handle or ignore
            }
            stmt.execute("CREATE TABLE addColumnInHistoryMode(a INT, _fivetran_start DATETIME(6), _fivetran_end DATETIME(6), _fivetran_active BOOL, PRIMARY KEY(_fivetran_start, a))");

            final MigrateRequest emptyRequest = MigrateRequest.newBuilder()
                .putAllConfiguration(confMap)
                .setDetails(MigrationDetails.newBuilder()
                    .setTable("addColumnInHistoryMode")
                    .setSchema(database)
                    .setAdd(
                        AddOperation.newBuilder()
                            .setAddColumnInHistoryMode(
                                AddColumnInHistoryMode.newBuilder()
                                    .setColumn("b")
                                    .setColumnType(DataType.INT)
                                    .setOperationTimestamp("2021-01-01 01:01:01")
                                    .setDefaultValue("3")
                            )
                    )
                ).build();
            List<TeradataJDBCUtil.QueryWithCleanup> queries = TeradataJDBCUtil.generateMigrateQueries(emptyRequest, testWarningHandle);
            Assertions.assertEquals(1, queries.size());

            stmt.execute("INSERT INTO addColumnInHistoryMode VALUES (1, '2020-01-01 01:01:01', '2021-01-01 01:01:01', 0), (2, '2020-01-01 01:01:01', '9999-12-31 11:59:59.999999', 1)");

            MigrateRequest request = MigrateRequest.newBuilder()
                .putAllConfiguration(confMap)
                .setDetails(MigrationDetails.newBuilder()
                    .setTable("addColumnInHistoryMode")
                    .setSchema(database)
                    .setAdd(
                        AddOperation.newBuilder()
                            .setAddColumnInHistoryMode(
                                AddColumnInHistoryMode.newBuilder()
                                    .setColumn("b")
                                    .setColumnType(DataType.INT)
                                    .setOperationTimestamp("2021-01-01 01:01:01")
                                    .setDefaultValue("3")
                            )
                    )
                ).build();

            queries = TeradataJDBCUtil.generateMigrateQueries(request, testWarningHandle);
            for (TeradataJDBCUtil.QueryWithCleanup q : queries) {
                q.execute(conn);
            }

            Table t = TeradataJDBCUtil.getTable(conf, database, "addColumnInHistoryMode", "addColumnInHistoryMode", testWarningHandle);
            List<Column> columns = t.getColumnsList();
            Assertions.assertEquals("a", columns.get(0).getName());
            Assertions.assertEquals("b", columns.get(4).getName());
            Assertions.assertEquals(DataType.INT, columns.get(4).getType());
            Assertions.assertEquals("_fivetran_start", columns.get(1).getName());
            Assertions.assertEquals("_fivetran_end", columns.get(2).getName());
            Assertions.assertEquals("_fivetran_active", columns.get(3).getName());
            Assertions.assertEquals(5, columns.size());

            checkResult("SELECT a, b, _fivetran_start, _fivetran_end, _fivetran_active FROM addColumnInHistoryMode ORDER BY _fivetran_start, a", Arrays.asList(
                Arrays.asList("1", null, "2020-01-01 01:01:01.000000", "2021-01-01 01:01:01.000000", "0"),
                Arrays.asList("2", null, "2020-01-01 01:01:01.000000", "2021-01-01 01:01:00.999999", "0"),
                Arrays.asList("2", "3", "2021-01-01 01:01:01.000000", "9999-12-31 11:59:59.999999", "1")
            ));

            Assertions.assertThrows(IllegalArgumentException.class, () -> {
                final MigrateRequest invalidRequest = MigrateRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setDetails(MigrationDetails.newBuilder()
                        .setTable("addColumnInHistoryMode")
                        .setSchema(database)
                        .setAdd(
                            AddOperation.newBuilder()
                                .setAddColumnInHistoryMode(
                                    AddColumnInHistoryMode.newBuilder()
                                        .setColumn("b")
                                        .setOperationTimestamp("2001-01-01 01:01:01")
                                        .setColumnType(DataType.INT)
                                        .setDefaultValue("3")
                                )
                        )
                    ).build();
                TeradataJDBCUtil.generateMigrateQueries(invalidRequest, testWarningHandle);
            });
        }
    }
}
