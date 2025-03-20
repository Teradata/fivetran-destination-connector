package com.teradata.fivetran.destination;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import com.google.protobuf.Timestamp;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.CreateTableRequest;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.SoftTruncate;
import fivetran_sdk.v2.Table;
import fivetran_sdk.v2.TruncateRequest;

public class TruncateTest extends IntegrationTestBase {
    @Test
    public void softTruncate() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            Table t = Table.newBuilder().setName("softTruncate")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.INT)
                                    .setPrimaryKey(false).build(),
                            Column.newBuilder().setName("_fivetran_synced")
                                    .setType(DataType.UTC_DATETIME).setPrimaryKey(false).build(),
                            Column.newBuilder().setName("_fivetran_deleted")
                                    .setType(DataType.BOOLEAN).setPrimaryKey(false).build()))
                    .build();

            CreateTableRequest cr =
                    CreateTableRequest.newBuilder().setSchemaName(database).setTable(t).build();
            stmt.execute(TeradataJDBCUtil.generateCreateTableQuery(conf, stmt, cr));

            stmt.execute("INSERT INTO " + conf.database() + ".softTruncate VALUES (1, '2038-01-19 03:14:07.455', 0)");
            stmt.execute("INSERT INTO " + conf.database() + ".softTruncate VALUES (2, '2038-01-19 03:14:07.456', 1)");
            stmt.execute("INSERT INTO " + conf.database() + ".softTruncate VALUES (3, '2038-01-19 03:14:07.456', 0)");
            stmt.execute("INSERT INTO " + conf.database() + ".softTruncate VALUES (4, '2038-01-19 03:14:07.457', 0)");
            stmt.execute("INSERT INTO " + conf.database() + ".softTruncate VALUES (5, '2038-01-19 03:14:07.461', 0)");

            TruncateRequest tr = TruncateRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTableName("softTruncate")
                    .setSoft(SoftTruncate.newBuilder().setDeletedColumn("_fivetran_deleted"))
                    .setSyncedColumn("_fivetran_synced")
                    .setUtcDeleteBefore(
                            Timestamp.newBuilder().setSeconds(2147483647L).setNanos(458000))
                    .build();

            String queryNanosToTimestamp = String.format("SELECT TO_TIMESTAMP(CAST('%d' AS BIGINT)) + INTERVAL '0.%06d' SECOND",
                    2147483647L, 458000);
            stmt.execute(queryNanosToTimestamp);
            String utcDeleteBefore = TeradataJDBCUtil.getSingleValue(stmt.getResultSet());

            stmt.execute(TeradataJDBCUtil.generateTruncateTableQuery(conf, tr, utcDeleteBefore));

            checkResult("SELECT * FROM " + conf.database() + ".softTruncate ORDER BY a",
                    Arrays.asList(Arrays.asList("1", "2038-01-19 03:14:07.455", "1"),
                            Arrays.asList("2", "2038-01-19 03:14:07.456", "1"),
                            Arrays.asList("3", "2038-01-19 03:14:07.456", "1"),
                            Arrays.asList("4", "2038-01-19 03:14:07.457", "1"),
                            Arrays.asList("5", "2038-01-19 03:14:07.461", "0")));
        }
    }

    @Test
    public void hardTruncate() throws SQLException, Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            Table t = Table.newBuilder().setName("hardTruncate")
                    .addAllColumns(Arrays.asList(
                            Column.newBuilder().setName("a").setType(DataType.INT)
                                    .setPrimaryKey(false).build(),
                            Column.newBuilder().setName("_fivetran_synced")
                                    .setType(DataType.UTC_DATETIME).setPrimaryKey(false).build(),
                            Column.newBuilder().setName("_fivetran_deleted")
                                    .setType(DataType.BOOLEAN).setPrimaryKey(false).build()))
                    .build();

            CreateTableRequest cr =
                    CreateTableRequest.newBuilder().setSchemaName(database).setTable(t).build();
            stmt.execute(TeradataJDBCUtil.generateCreateTableQuery(conf, stmt, cr));

            stmt.execute("INSERT INTO " + conf.database() + ".hardTruncate VALUES (1, '2038-01-19 03:14:07.455', 0)");
            stmt.execute("INSERT INTO " + conf.database() + ".hardTruncate VALUES (2, '2038-01-19 03:14:07.456', 1)");
            stmt.execute("INSERT INTO " + conf.database() + ".hardTruncate VALUES (3, '2038-01-19 03:14:07.456', 0)");
            stmt.execute("INSERT INTO " + conf.database() + ".hardTruncate VALUES (4, '2038-01-19 03:14:07.457', 0)");
            stmt.execute("INSERT INTO " + conf.database() + ".hardTruncate VALUES (5, '2038-01-19 03:14:07.461', 0)");

            TruncateRequest tr = TruncateRequest.newBuilder().putAllConfiguration(confMap)
                    .setSchemaName(database).setTableName("hardTruncate")
                    .setSyncedColumn("_fivetran_synced")
                    .setUtcDeleteBefore(
                            Timestamp.newBuilder().setSeconds(2147483647L).setNanos(458000))
                    .build();

            String queryNanosToTimestamp = String.format("SELECT TO_TIMESTAMP(CAST('%d' AS BIGINT)) + INTERVAL '0.%06d' SECOND",
                    2147483647L, 458000);
            stmt.execute(queryNanosToTimestamp);
            String utcDeleteBefore = TeradataJDBCUtil.getSingleValue(stmt.getResultSet());

            stmt.execute(TeradataJDBCUtil.generateTruncateTableQuery(conf, tr, utcDeleteBefore));

            checkResult("SELECT * FROM " + conf.database() + ".hardTruncate ORDER BY a",
                    Arrays.asList(Arrays.asList("5", "2038-01-19 03:14:07.461", "0")));
        }
    }
}