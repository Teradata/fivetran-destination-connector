package com.teradata.fivetran.destination;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates Teradata database state after running 03_schema_migrations_dml.json
 * via the Fivetran SDK destination tester.
 *
 * Covers: copy_column, update_column_value, add_column_with_default_value,
 * set_column_to_null, copy_table, rename_column, rename_table, drop_table.
 */
public class SchemaMigrationDmlValidationTest extends SdkTesterValidationBase {

    @Test
    public void originalTableExists() throws Exception {
        assertTableExists("td_dml_test");
    }

    @Test
    public void droppedTableRemoved() throws Exception {
        assertTableNotExists("td_dml_drop");
    }

    @Test
    public void renamedTableExists() throws Exception {
        // td_dml_test_new was renamed to td_dml_test_renamed
        assertTableExists("td_dml_test_renamed");
    }

    @Test
    public void copiedColumnExists() throws Exception {
        // desc was copied to desc_detailed
        assertColumnExists("td_dml_test", "desc_detailed");
    }

    @Test
    public void renamedColumnExists() throws Exception {
        // amount was renamed to amount_renamed
        assertColumnExists("td_dml_test", "amount_renamed");
        assertColumnNotExists("td_dml_test", "amount");
    }

    @Test
    public void defaultValueColumnExists() throws Exception {
        assertColumnExists("td_dml_test", "operation_time");
    }

    @Test
    public void dataPreservedInOriginalTable() throws Exception {
        // 6 rows inserted, all should survive migrations
        assertRowCount("td_dml_test", 6);
    }

    @Test
    public void renamedTableHasData() throws Exception {
        // Copied from td_dml_test (6 rows) before rename
        assertRowCount("td_dml_test_renamed", 6);
    }

    @Test
    public void descColumnIsNull() throws Exception {
        // set_column_to_null was called on desc
        int nonNullCount = queryInt(
                "SELECT COUNT(*) FROM " + getFullTableRef("td_dml_test") +
                        " WHERE \"desc\" IS NOT NULL");
        assertEquals(0, nonNullCount, "All desc values should be NULL after set_column_to_null");
    }
}
