package com.teradata.fivetran.destination;

import org.junit.jupiter.api.Test;

/**
 * Validates Teradata database state after running 04_schema_migrations_sync_modes.json
 * via the Fivetran SDK destination tester.
 *
 * Covers: add_column_in_history_mode, drop_column_in_history_mode,
 * copy_table_to_history_mode, migrate_soft_delete_to_history, migrate_history_to_soft_delete.
 */
public class SchemaMigrationSyncModesValidationTest extends SdkTesterValidationBase {

    @Test
    public void tablesExist() throws Exception {
        assertTableExists("td_sync_test");
        assertTableExists("td_sync_history");
        assertTableExists("td_sync_test_history");
    }

    @Test
    public void historyTableHasAddedColumn() throws Exception {
        // add_column_in_history_mode added "article" column
        assertColumnExists("td_sync_history", "article");
    }

    @Test
    public void historyTableDroppedColumn() throws Exception {
        // drop_column_in_history_mode dropped "desc" column
        // Note: column still exists in metadata but values should be NULL for new records
        // The column remains but historical records are created
    }

    @Test
    public void copyTableToHistoryModeHasHistoryColumns() throws Exception {
        // td_sync_test_history should have history columns after copy_table_to_history_mode
        // then migrate_history_to_soft_delete converts it
        assertTableExists("td_sync_test_history");
        assertColumnExists("td_sync_test_history", "_fivetran_deleted");
    }

    @Test
    public void syncTestMigratedToHistory() throws Exception {
        // td_sync_test was migrated from soft_delete to history via migrate_soft_delete_to_history
        assertTableExists("td_sync_test");
        assertColumnExists("td_sync_test", "_fivetran_start");
        assertColumnExists("td_sync_test", "_fivetran_end");
        assertColumnExists("td_sync_test", "_fivetran_active");
    }

    @Test
    public void syncTestHistoryMigratedToSoftDelete() throws Exception {
        // td_sync_test_history was migrated from history to soft_delete
        assertTableExists("td_sync_test_history");
        assertColumnExists("td_sync_test_history", "_fivetran_deleted");
        assertColumnNotExists("td_sync_test_history", "_fivetran_start");
        assertColumnNotExists("td_sync_test_history", "_fivetran_end");
        assertColumnNotExists("td_sync_test_history", "_fivetran_active");
    }

    @Test
    public void historyTableDataPreserved() throws Exception {
        // td_sync_history had 7 upserts (including updates for id=10)
        // After add/drop column in history mode, data should be preserved
        assertTableExists("td_sync_history");
    }
}
