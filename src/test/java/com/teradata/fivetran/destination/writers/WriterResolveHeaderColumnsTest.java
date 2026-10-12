package com.teradata.fivetran.destination.writers;

import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pure unit tests for {@link Writer#resolveHeaderColumns}, the helper shared by both
 * {@link LoadDataWriter} and {@link FastLoadDataWriter} to resolve a CSV batch file header
 * against a table's physical columns. No live database connection is required.
 */
public class WriterResolveHeaderColumnsTest {

    private static final List<Column> COLUMNS = Arrays.asList(
            Column.newBuilder().setName("id").setType(DataType.INT).build(),
            Column.newBuilder().setName("data").setType(DataType.STRING).build(),
            Column.newBuilder().setName("extra").setType(DataType.STRING).build());

    /**
     * Regression test for IDE-26747: _fivetran_earliest must be tolerated (and skipped) even
     * when it appears in the middle of the header rather than at the end, and the row indices
     * for the surrounding columns must still line up with their original header positions.
     */
    @Test
    public void midHeaderFivetranEarliestIsSkippedWithCorrectRowIndices() {
        List<String> header = Arrays.asList("id", "_fivetran_earliest", "data", "extra");

        Writer.HeaderMapping mapping = Writer.resolveHeaderColumns(header, COLUMNS);

        assertEquals(Arrays.asList("id", "data", "extra"),
                mapping.matchedColumns.stream().map(Column::getName).toList());
        // "_fivetran_earliest" at header index 1 must be excluded; "data"/"extra" retain their
        // original header positions (2 and 3), not the compacted matchedColumns positions.
        assertEquals(Arrays.asList(0, 2, 3), mapping.rowIndices);
    }

    @Test
    public void genuinelyUnknownMidHeaderColumnThrows() {
        List<String> header = Arrays.asList("id", "some_unexpected_column", "data", "extra");

        assertThrows(IllegalStateException.class, () -> Writer.resolveHeaderColumns(header, COLUMNS));
    }
}
