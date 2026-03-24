package com.teradata.fivetran.destination;

import org.junit.jupiter.api.Test;

/**
 * Validates Teradata database state after running 05_operations_on_nonexistent_records/
 * via the Fivetran SDK destination tester.
 *
 * Covers: delete/update/soft_delete on non-existent records,
 * truncate on non-existent tables, operations after truncate empties all rows.
 * All these operations must be safely ignored by the connector.
 */
public class NonExistentRecordsValidationTest extends SdkTesterValidationBase {

    @Test
    public void deleteMissingRecordTablesExistAndEmpty() throws Exception {
        assertTableExists("td_del_missing");
        assertTableEmpty("td_del_missing");
        assertTableExists("td_del_missing_nopk");
        assertTableEmpty("td_del_missing_nopk");
    }

    @Test
    public void softDeleteMissingRecordTablesExistAndEmpty() throws Exception {
        assertTableExists("td_sdel_missing");
        assertTableEmpty("td_sdel_missing");
        assertTableExists("td_sdel_missing_nopk");
        assertTableEmpty("td_sdel_missing_nopk");
    }

    @Test
    public void updateMissingRecordTablesExistAndEmpty() throws Exception {
        assertTableExists("td_upd_missing");
        assertTableEmpty("td_upd_missing");
        assertTableExists("td_upd_missing_nopk");
        assertTableEmpty("td_upd_missing_nopk");
    }

    @Test
    public void truncateThenDeleteTablesExistAndEmpty() throws Exception {
        // Data was upserted, then truncated, then delete on empty table
        assertTableExists("td_trunc_del");
        assertTableEmpty("td_trunc_del");
        assertTableExists("td_trunc_del_nopk");
        assertTableEmpty("td_trunc_del_nopk");
    }

    @Test
    public void truncateThenSoftDeleteTablesExistAndEmpty() throws Exception {
        assertTableExists("td_trunc_sdel");
        assertTableEmpty("td_trunc_sdel");
        assertTableExists("td_trunc_sdel_nopk");
        assertTableEmpty("td_trunc_sdel_nopk");
    }

    @Test
    public void truncateThenUpdateTablesExistAndEmpty() throws Exception {
        assertTableExists("td_trunc_upd");
        assertTableEmpty("td_trunc_upd");
        assertTableExists("td_trunc_upd_nopk");
        assertTableEmpty("td_trunc_upd_nopk");
    }
}
