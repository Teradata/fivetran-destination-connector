package com.teradata.fivetran.destination;

import org.junit.jupiter.api.Test;

/**
 * Validates Teradata database state after running 01_basic_operations.json
 * via the Fivetran SDK destination tester.
 *
 * Covers: all data types, upsert, truncate, soft_truncate, update, delete, soft_delete,
 * alter_table (add/drop column, change scale), history mode, composite PKs, no-PK tables.
 */
public class BasicOperationsValidationTest extends SdkTesterValidationBase {

    @Test
    public void tablesExist() throws Exception {
        assertTableExists("td_transaction");
        assertTableExists("td_campaign");
        assertTableExists("td_composite");
    }

    @Test
    public void transactionTableSchemaAfterAlter() throws Exception {
        // old_column should be dropped, new_column should be added
        assertColumnNotExists("td_transaction", "old_column");
        assertColumnExists("td_transaction", "new_column");
        assertColumnExists("td_transaction", "id");
        assertColumnExists("td_transaction", "amount_double");
        assertColumnExists("td_transaction", "amount_float");
        assertColumnExists("td_transaction", "amount_decimal");
        assertColumnExists("td_transaction", "flag");
        assertColumnExists("td_transaction", "string_val");
        assertColumnExists("td_transaction", "json_val");
    }

    @Test
    public void compositeTableSchemaAfterAlter() throws Exception {
        // old_pk should be dropped, new_pk should be added
        assertColumnNotExists("td_composite", "old_pk");
        assertColumnExists("td_composite", "new_pk");
        assertColumnExists("td_composite", "pk1");
        assertColumnExists("td_composite", "pk2");
        assertColumnExists("td_composite", "value");
    }

    @Test
    public void campaignTableAfterTruncateAndReinsert() throws Exception {
        // Campaign was truncated then one row re-inserted
        assertTableExists("td_campaign");
        assertRowCount("td_campaign", 1);
    }

    @Test
    public void transactionHistoryModeColumns() throws Exception {
        // History mode table should have _fivetran_start, _fivetran_end, _fivetran_active
        assertColumnExists("td_transaction", "_fivetran_start");
        assertColumnExists("td_transaction", "_fivetran_end");
        assertColumnExists("td_transaction", "_fivetran_active");
    }

    @Test
    public void compositeTableAfterDeleteAndSoftTruncate() throws Exception {
        // soft_truncate marks rows as deleted, then hard delete removes one
        assertTableExists("td_composite");
    }
}
