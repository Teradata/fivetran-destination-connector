package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.writers.UpdateHistoryWriter;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Table;
import fivetran_sdk.v2.WriteBatchResponse;
import fivetran_sdk.v2.WriteHistoryBatchRequest;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for UpdateHistoryWriter.
 */
public class UpdateHistoryWriterTest extends IntegrationTestBase {

    /**
     * Test to verify behavior when the _fivetran_start column is missing.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void noFivetranStart() throws Exception {
        String tableName = IntegrationTestBase.class.getSimpleName() + "_" + "noFivetranStartUpdate";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table without the _fivetran_start column
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + "(" +
                    "id INT PRIMARY KEY NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_end TIMESTAMP(6))");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 'a', 1, '2005-05-24 20:57:00.0')");

            // Retrieve table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Initialize UpdateHistoryWriter and expect an exception due to missing _fivetran_start column
            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            Exception exception = assertThrows(IllegalArgumentException.class, () -> {
                u.setHeader(Arrays.asList("id", "data", "_fivetran_active", "_fivetran_end"));
            });

            assertEquals("File doesn't contain _fivetran_start column", exception.getMessage());
        }
    }

    /**
     * Test to verify behavior with a single primary key.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void singlePK() throws Exception {
        String tableName = IntegrationTestBase.class.getSimpleName() + "_" + "singlePKUpdate";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table with a single primary key
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + "(" +
                    "id INT NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL," +
                    "_fivetran_end TIMESTAMP(6)," +
                    "PRIMARY KEY(id, _fivetran_start))");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(2, 'b', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(3, 'c', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 'd', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(2, 'e', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");

            // Retrieve table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Initialize UpdateHistoryWriter and write rows
            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            u.setHeader(Arrays.asList("id", "data", "_fivetran_start"));
            u.writeRow(Arrays.asList("1", "f", "2005-05-25T20:57:00Z"));
            u.writeRow(Arrays.asList("2", "unm", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("5", "unm", "2005-05-26T20:57:00Z"));
            u.commit();
        }

        // Verify the results
        checkResult("SELECT * FROM " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " ORDER BY id, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "d", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "a", "0", "2005-05-24 20:57:00.0", "2005-05-25 20:56:59.0"),
                Arrays.asList("1", "f", "1", "2005-05-25 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("2", "e", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "b", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:56:59.0"),
                Arrays.asList("2", "b", "1", "2005-05-26 20:57:00.0", "9999-12-31 23:59:59.999999"),
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
        String tableName = IntegrationTestBase.class.getSimpleName() + "_" + "multiPKUpdate";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Create a table with multiple primary keys
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + "(" +
                    "id1 INT NOT NULL, " +
                    "id2 INT NOT NULL, " +
                    "data1 VARCHAR(10), " +
                    "data2 VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL," +
                    "_fivetran_end TIMESTAMP(6)," +
                    "PRIMARY KEY(id1, id2, _fivetran_start))");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 1, 'a', 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 2, 'a', 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(2, 2, 'b', 'b', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(3, 3, 'c', 'c', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(1, 1, 'd', 'd', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " VALUES(2, 2, 'e', 'e', 0, '2005-05-23 20:57:00.0', '2005-05-24 20:56:59.999999')");

            // Retrieve table metadata
            Table t = TeradataJDBCUtil.getTable(conf, database, tableName, tableName, testWarningHandle);
            FileParams params = FileParams.newBuilder().setNullString("NULL")
                    .setUnmodifiedString("unm").build();

            // Initialize UpdateHistoryWriter and write rows
            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, t.getName(), t.getColumnsList(), params, null, 123);
            u.setHeader(Arrays.asList("id1", "id2", "data1", "data2", "_fivetran_start"));
            u.writeRow(Arrays.asList("1", "1", "f", "f", "2005-05-25T20:57:00Z"));
            u.writeRow(Arrays.asList("2", "2", "unm", "f", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("3", "3", "unm", "unm", "2005-05-26T20:57:00Z"));
            u.writeRow(Arrays.asList("5", "5", "unm", "unm", "2005-05-26T20:57:00Z"));
            u.commit();
        }

        // Verify the results
        checkResult("SELECT * FROM " + TeradataJDBCUtil.escapeTable(conf.database(),tableName) + " ORDER BY id1, id2, _fivetran_start", Arrays.asList(
                Arrays.asList("1", "1", "d", "d", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("1", "1", "a", "a", "0", "2005-05-24 20:57:00.0", "2005-05-25 20:56:59.0"),
                Arrays.asList("1", "1", "f", "f", "1", "2005-05-25 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("1", "2", "a", "a", "1", "2005-05-24 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("2", "2", "e", "e", "0", "2005-05-23 20:57:00.0", "2005-05-24 20:56:59.999999"),
                Arrays.asList("2", "2", "b", "b", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:56:59.0"),
                Arrays.asList("2", "2", "b", "f", "1", "2005-05-26 20:57:00.0", "9999-12-31 23:59:59.999999"),
                Arrays.asList("3", "3", "c", "c", "0", "2005-05-24 20:57:00.0", "2005-05-26 20:56:59.0"),
                Arrays.asList("3", "3", "c", "c", "1", "2005-05-26 20:57:00.0", "9999-12-31 23:59:59.999999")
        ));
    }

    /**
     * Regression test for IDE-26747: _fivetran_earliest is never materialized as a physical
     * column (see TeradataJDBCUtil DDL filtering), but Fivetran still includes it in the
     * WriteHistoryBatchRequest's column list and in the update batch file's header. This drives
     * the real TeradataDestinationServiceImpl#writeHistoryBatch entry point end-to-end (rather
     * than re-implementing its filtering locally), so a regression in the service's own
     * _fivetran_earliest exclusion would fail this test: without it, UpdateHistoryWriter's
     * explicit target column list would reference a column that does not exist on the table
     * and the incremental update would fail.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void writeHistoryBatchExcludesFivetranEarliestFromUpdateDml() throws Exception {
        writeHistoryBatchExcludesFivetranEarliest("earliestServiceTestTrailing",
                "id,data,_fivetran_start,_fivetran_earliest",
                "1,f,2005-05-25T20:57:00Z,0");
    }

    /**
     * Same regression as {@link #writeHistoryBatchExcludesFivetranEarliestFromUpdateDml()},
     * but with _fivetran_earliest placed in the middle of the update file header rather than
     * last, confirming the positional header handling isn't accidentally relying on the marker
     * always being the final column.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void writeHistoryBatchExcludesFivetranEarliestFromUpdateDml_midHeader() throws Exception {
        writeHistoryBatchExcludesFivetranEarliest("earliestServiceTestMidHeader",
                "id,_fivetran_earliest,data,_fivetran_start",
                "1,0,f,2005-05-25T20:57:00Z");
    }

    /**
     * Regression test for IDE-26747: _fivetran_earliest is never materialized as a physical
     * column (see TeradataJDBCUtil DDL filtering), but Fivetran still includes it in the
     * WriteHistoryBatchRequest's column list and in the update batch file's header. This drives
     * the real TeradataDestinationServiceImpl#writeHistoryBatch entry point end-to-end (rather
     * than re-implementing its filtering locally), so a regression in the service's own
     * _fivetran_earliest exclusion would fail this test: without it, UpdateHistoryWriter's
     * explicit target column list would reference a column that does not exist on the table
     * and the incremental update would fail.
     *
     * @param logicalTable  The (unique) table name to use for this test run.
     * @param updateHeader  The update batch file's CSV header line.
     * @param updateRow     The update batch file's single CSV data line.
     * @throws Exception If an error occurs during the test.
     */
    private void writeHistoryBatchExcludesFivetranEarliest(String logicalTable, String updateHeader, String updateRow)
            throws Exception {
        String schemaName = "e2e";
        String tableName = TeradataJDBCUtil.getTableName(schemaName, logicalTable);

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            // Physical table WITHOUT _fivetran_earliest, as the fixed connector creates it.
            stmt.execute("CREATE TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + "(" +
                    "id INT NOT NULL, " +
                    "data VARCHAR(10), " +
                    "_fivetran_active BYTEINT, " +
                    "_fivetran_start TIMESTAMP(6) NOT NULL, " +
                    "_fivetran_end TIMESTAMP(6), " +
                    "PRIMARY KEY(id, _fivetran_start))");
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " VALUES(1, 'a', 1, '2005-05-24 20:57:00.0', '9999-12-31 23:59:59.999999')");
        }

        // Update batch file: Fivetran still emits _fivetran_earliest positionally in the header.
        Path updateFile = Files.createTempFile("ide26747-update", ".csv");
        try {
            Files.write(updateFile, Arrays.asList(updateHeader, updateRow));

            // Simulate the WriteHistoryBatchRequest's Table.columns: Fivetran still sends
            // _fivetran_earliest ahead of writeHistoryBatch, even though the connector never
            // materializes it physically.
            Table table = Table.newBuilder()
                    .setName(logicalTable)
                    .addColumns(Column.newBuilder().setName("id").setType(DataType.INT).setPrimaryKey(true).build())
                    .addColumns(Column.newBuilder().setName("data").setType(DataType.STRING).build())
                    .addColumns(Column.newBuilder().setName("_fivetran_active").setType(DataType.BOOLEAN).build())
                    .addColumns(Column.newBuilder().setName("_fivetran_start").setType(DataType.NAIVE_DATETIME).setPrimaryKey(true).build())
                    .addColumns(Column.newBuilder().setName("_fivetran_end").setType(DataType.NAIVE_DATETIME).build())
                    .addColumns(Column.newBuilder().setName("_fivetran_earliest").setType(DataType.BOOLEAN).build())
                    .build();

            WriteHistoryBatchRequest request = WriteHistoryBatchRequest.newBuilder()
                    .putAllConfiguration(confMap)
                    .setSchemaName(schemaName)
                    .setTable(table)
                    .setFileParams(FileParams.newBuilder().setNullString("NULL").setUnmodifiedString("unm").build())
                    .addUpdateFiles(updateFile.toString())
                    .build();

            AtomicReference<WriteBatchResponse> captured = new AtomicReference<>();
            new TeradataDestinationServiceImpl().writeHistoryBatch(request, new StreamObserver<WriteBatchResponse>() {
                @Override
                public void onNext(WriteBatchResponse value) {
                    captured.set(value);
                }

                @Override
                public void onError(Throwable t) {
                    throw new IllegalStateException(t);
                }

                @Override
                public void onCompleted() {
                }
            });

            WriteBatchResponse response = captured.get();
            assertNotNull(response, "writeHistoryBatch did not emit a response");
            assertTrue(response.getSuccess(),
                    "writeHistoryBatch failed (likely referencing the non-materialized _fivetran_earliest column): "
                            + response.getTask().getMessage());

            checkResult("SELECT id, data, _fivetran_active FROM " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " WHERE _fivetran_active = 1 ORDER BY id", Arrays.asList(
                    Arrays.asList("1", "f", "1")
            ));
        } finally {
            Files.deleteIfExists(updateFile);
        }
    }
}