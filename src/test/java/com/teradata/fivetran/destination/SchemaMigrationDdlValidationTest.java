package com.teradata.fivetran.destination;

import org.junit.jupiter.api.Test;

/**
 * Validates Teradata database state after running 02_schema_migrations_ddl.json
 * via the Fivetran SDK destination tester.
 *
 * Covers: add_column, change_column_data_type, drop_column.
 */
public class SchemaMigrationDdlValidationTest extends SdkTesterValidationBase {

    @Test
    public void tableExists() throws Exception {
        assertTableExists("td_ddl_test");
    }

    @Test
    public void newColumnAdded() throws Exception {
        assertColumnExists("td_ddl_test", "operation_time");
    }

    @Test
    public void droppedColumnRemoved() throws Exception {
        assertColumnNotExists("td_ddl_test", "desc");
    }

    @Test
    public void dataPreserved() throws Exception {
        // 6 rows were inserted before migration; data should survive
        assertRowCount("td_ddl_test", 6);
    }

    @Test
    public void remainingColumnsIntact() throws Exception {
        assertColumnExists("td_ddl_test", "id");
        assertColumnExists("td_ddl_test", "amount");
    }
}
