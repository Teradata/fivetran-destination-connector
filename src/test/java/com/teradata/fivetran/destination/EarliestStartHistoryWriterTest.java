package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.writers.EarliestStartHistoryWriter;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EarliestStartHistoryWriterTest extends IntegrationTestBase {
    @Test
    public void noFivetranStart() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + conf.database() + ".noFivetranStart(" +
                    "id INT PRIMARY KEY NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_end TIMESTAMP(6))");
            stmt.execute("INSERT INTO " + conf.database() + ".noFivetranStart VALUES(1, 'a', 1, '2005-05-24 20:57:00.0')");

            Table t = TeradataJDBCUtil.getTable(conf, database, "noFivetranStart", "noFivetranStart", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            EarliestStartHistoryWriter e = new EarliestStartHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            Exception exception = assertThrows(IllegalArgumentException.class, () -> {
                e.setHeader(Arrays.asList("id", "data", "_fivetran_active", "_fivetran_end"));
            });

            assertEquals("File doesn't contain _fivetran_start column", exception.getMessage());
        }
    }

    @Test
    public void singlePK() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + conf.database() + ".singlePKEarliestStart(" +
                    "id INT NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL," +
                    "_fivetran_end TIMESTAMP(6)," +
                    "PRIMARY KEY(id, _fivetran_start))");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePKEarliestStart VALUES(1, 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePKEarliestStart VALUES(2, 'b', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePKEarliestStart VALUES(3, 'c', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePKEarliestStart VALUES(1, 'd', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".singlePKEarliestStart VALUES(2, 'e', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");

            Table t = TeradataJDBCUtil.getTable(conf, database, "singlePKEarliestStart", "singlePKEarliestStart", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            EarliestStartHistoryWriter e = new EarliestStartHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            e.setHeader(Arrays.asList("id", "_fivetran_start"));
            e.writeRow(Arrays.asList("1", "2005-05-23T21:57:00Z"));
            e.writeRow(Arrays.asList("2", "2005-05-26T20:57:00Z"));
            e.writeRow(Arrays.asList("5", "2005-05-26T20:57:00Z"));
            e.commit();
        }

        checkResult("SELECT * FROM " + conf.database() + ".singlePKEarliestStart ORDER BY id, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "d", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "e", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "b", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:56:59.0"),
                Arrays.asList("3", "c", "1", "2005-05-24 20:57:00.0", "9999-12-31 23:59:59.999999")
        ));
    }

    @Test
    public void multiPK() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + conf.database() + ".multiPKEarliestStart(" +
                    "id1 INT NOT NULL, " +
                    "id2 INT NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL," +
                    "_fivetran_end TIMESTAMP(6)," +
                    "PRIMARY KEY(id1, id2, _fivetran_start))");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPKEarliestStart VALUES(1, 2, 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPKEarliestStart VALUES(1, 1, 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPKEarliestStart VALUES(2, 2, 'b', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPKEarliestStart VALUES(3, 3, 'c', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPKEarliestStart VALUES(1, 1, 'd', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO " + conf.database() + ".multiPKEarliestStart VALUES(2, 2, 'e', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");

            Table t = TeradataJDBCUtil.getTable(conf, database, "multiPKEarliestStart", "multiPKEarliestStart", testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            EarliestStartHistoryWriter e = new EarliestStartHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            e.setHeader(Arrays.asList("id1", "id2", "_fivetran_start"));
            e.writeRow(Arrays.asList("1", "1", "2005-05-23T21:57:00Z"));
            e.writeRow(Arrays.asList("2", "2", "2005-05-26T20:57:00Z"));
            e.writeRow(Arrays.asList("5", "5", "2005-05-26T20:57:00Z"));
            e.commit();
        }

        checkResult("SELECT * FROM " + conf.database() + ".multiPKEarliestStart ORDER BY id1, id2, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "1", "d", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "2", "a", "1", "2005-05-24 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("2", "2", "e", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "2", "b", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:56:59.0"),
                Arrays.asList("3", "3", "c", "1", "2005-05-24 20:57:00.0", "9999-12-31 23:59:59.999999")
        ));
    }
}