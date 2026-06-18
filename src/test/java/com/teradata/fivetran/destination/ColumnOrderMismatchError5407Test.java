package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.writers.UpdateHistoryWriter;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.DataTypeParams;
import fivetran_sdk.v2.FileParams;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Reproduces the customer's history-mode Error 5407 ("Invalid operation for
 * DateTime or Interval") that STILL occurs after PR #36.
 *
 * <p>Root cause (different from PR #36): {@link UpdateHistoryWriter#insertNewRow}
 * emits a column-list-less statement:
 *
 * <pre>
 *   INSERT INTO t SELECT &lt;projection&gt; FROM t WHERE _fivetran_active = 1 AND "id" = ?
 * </pre>
 *
 * The projection is built by iterating the writeHistoryBatch request's
 * {@code columns} list. Because there is no explicit target column list,
 * Teradata maps the projected values <b>positionally onto the table's physical
 * column order</b>. When Fivetran sends columns in a different order than the
 * physical table was created in, values land in the wrong physical columns —
 * and a non-timestamp value ends up assigned to a {@code TIMESTAMP(6)} column,
 * which raises Error 5407.
 *
 * <p>This mirrors the customer logs exactly. The physical table is
 * {@code (id, _fivetran_synced, status, amount, last_modified, name,
 * _fivetran_start, _fivetran_end, _fivetran_active)} and {@code _fivetran_earliest}
 * is then added by {@code alterTable} just before {@code writeHistoryBatch}
 * (incremental_sync_logs.json). The request column order places
 * {@code status}/{@code amount} last, so the generated projection is:
 *
 * <pre>
 *   SELECT ?, ?, ?, ?, ?, ?, ?, ?, "status", "amount"
 * </pre>
 *
 * which is byte-for-byte the customer's failing query (8 placeholders followed
 * by the two copied columns).
 *
 * <p>PR #36 only normalized fractional-second precision in
 * {@code formatISODateTime}; it never touched this positional-mapping defect,
 * which is why rebuilding from main did not resolve the customer's issue.
 *
 * <p>The fix adds an explicit target column list to the INSERT in
 * {@code insertNewRow}, so values map by name rather than by physical position.
 * This test verifies the incremental update now succeeds and produces the
 * correct history rows even when the request column order differs from the
 * physical table order.
 */
public class ColumnOrderMismatchError5407Test extends IntegrationTestBase {

    /**
     * Builds the writeHistoryBatch request {@code columns} list in the SAME
     * order the customer's connector received from Fivetran: {@code status} and
     * {@code amount} are placed LAST, even though they are physically columns 3
     * and 4 of the table. This intentional mismatch is what triggers Error 5407.
     */
    private List<Column> requestColumnsWithStatusAmountLast() {
        List<Column> columns = new ArrayList<>();
        columns.add(stringColumn("id", true));
        columns.add(datetimeColumn("_fivetran_synced", false));
        columns.add(datetimeColumn("last_modified", false));
        columns.add(stringColumn("name", false));
        columns.add(datetimeColumn("_fivetran_start", true));
        columns.add(datetimeColumn("_fivetran_end", false));
        columns.add(booleanColumn("_fivetran_active"));
        // _fivetran_earliest is added by alterTable just before writeHistoryBatch
        // (see incremental_sync_logs.json) and is the 8th provided column, giving
        // the customer's 8 leading "?" placeholders.
        columns.add(booleanColumn("_fivetran_earliest"));
        // status / amount deliberately last — mirrors the customer's projection
        // "... ?, ?, \"status\", \"amount\""
        columns.add(stringColumn("status", false));
        columns.add(intColumn("amount"));
        return columns;
    }

    private Column stringColumn(String name, boolean pk) {
        return Column.newBuilder()
                .setName(name)
                .setType(DataType.STRING)
                .setPrimaryKey(pk)
                .setParams(DataTypeParams.newBuilder().setStringByteLength(256).build())
                .build();
    }

    private Column datetimeColumn(String name, boolean pk) {
        return Column.newBuilder()
                .setName(name)
                .setType(DataType.NAIVE_DATETIME)
                .setPrimaryKey(pk)
                .build();
    }

    private Column booleanColumn(String name) {
        return Column.newBuilder().setName(name).setType(DataType.BOOLEAN).build();
    }

    private Column intColumn(String name) {
        return Column.newBuilder().setName(name).setType(DataType.INT).build();
    }

    @Test
    public void incrementalUpdateSucceedsWithMismatchedColumnOrder() throws Exception {
        String tableName = getClass().getSimpleName() + "_users";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            // Physical table — exactly the customer's createTable column order
            // (9 columns), as produced by the initial historical sync.
            stmt.execute("CREATE MULTISET TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " (" +
                    "\"id\" VARCHAR(256) CHARACTER SET LATIN NOT NULL, " +
                    "\"_fivetran_synced\" TIMESTAMP(6), " +
                    "\"status\" VARCHAR(256) CHARACTER SET LATIN, " +
                    "\"amount\" INTEGER, " +
                    "\"last_modified\" TIMESTAMP(6), " +
                    "\"name\" VARCHAR(256) CHARACTER SET LATIN, " +
                    "\"_fivetran_start\" TIMESTAMP(6) NOT NULL, " +
                    "\"_fivetran_end\" TIMESTAMP(6), " +
                    "\"_fivetran_active\" BYTEINT, " +
                    "PRIMARY KEY (\"id\", \"_fivetran_start\"))");

            // One active historical row, as left by the initial historical sync.
            stmt.execute("INSERT INTO " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) + " VALUES(" +
                    "'1', " +                               // id
                    "TIMESTAMP '2026-06-16 09:24:00.000000', " + // _fivetran_synced
                    "'active', " +                          // status
                    "100, " +                               // amount
                    "TIMESTAMP '2026-06-15 10:00:00.000000', " + // last_modified
                    "'Alice', " +                           // name
                    "TIMESTAMP '2026-06-15 10:00:00.000000', " + // _fivetran_start
                    "TIMESTAMP '9999-12-31 23:59:59.999999', " + // _fivetran_end
                    "1)");                                  // _fivetran_active

            // alterTable adds _fivetran_earliest as the LAST physical column,
            // exactly as the incremental sync does before writeHistoryBatch.
            stmt.execute("ALTER TABLE " + TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                    " ADD \"_fivetran_earliest\" BYTEINT");

            FileParams params = FileParams.newBuilder()
                    .setNullString("NULL")
                    .setUnmodifiedString("unm")
                    .build();

            // Request columns in Fivetran's order (status/amount last), NOT the
            // physical table order — this is the crux of the reproduction.
            List<Column> requestColumns = requestColumnsWithStatusAmountLast();

            UpdateHistoryWriter writer =
                    new UpdateHistoryWriter(conn, database, tableName, requestColumns, params, null, 1);

            // Header matches the request column order. status/amount are
            // "unm" (unmodified) for this incremental update, so they become the
            // copied "status"/"amount" projection entries — exactly as the log shows.
            writer.setHeader(Arrays.asList(
                    "id", "_fivetran_synced", "last_modified", "name",
                    "_fivetran_start", "_fivetran_end", "_fivetran_active",
                    "_fivetran_earliest", "status", "amount"));

            // With the fix (explicit target column list) the update succeeds even
            // though the request column order differs from the physical order.
            assertDoesNotThrow(() ->
                    writer.writeRow(Arrays.asList(
                            "1",                       // id
                            "2026-06-16T09:25:00Z",    // _fivetran_synced
                            "2026-06-16T09:20:00Z",    // last_modified (changed)
                            "Alice",                   // name (unchanged value, still sent)
                            "2026-06-16T09:25:00Z",    // _fivetran_start (new version)
                            "9999-12-31T23:59:59Z",    // _fivetran_end
                            "1",                       // _fivetran_active
                            "0",                       // _fivetran_earliest
                            "unm",                     // status  -> copied
                            "unm")));                  // amount  -> copied
        }

        // Verify the copied columns landed in the correct physical columns:
        // status='active' and amount=100 must be carried over to the new active
        // row. If the column-order mapping were still broken these would be wrong
        // (or the statement would have failed with Error 5407 above).
        // Ordered by _fivetran_start: the older (now-inactive) row first, then the
        // new active row.
        checkResult("SELECT \"status\", \"amount\", \"_fivetran_active\" FROM " +
                        TeradataJDBCUtil.escapeTable(conf.database(), tableName) +
                        " ORDER BY \"_fivetran_start\"",
                Arrays.asList(
                        Arrays.asList("active", "100", "0"),
                        Arrays.asList("active", "100", "1")));
    }
}
