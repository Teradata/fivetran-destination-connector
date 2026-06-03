package com.teradata.fivetran.destination;

import fivetran_sdk.v2.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for BLOB/CLOB primary key auto-conversion to VARBYTE.
 * Teradata does not support BLOB as a primary key column.
 * When a BINARY-typed column is a PK with size ≤ 64KB, it is auto-converted to VARBYTE.
 * When size > 64KB or unknown, the connector fails with a clear error.
 */
public class BlobPrimaryKeyTest {

    @Test
    public void binaryPkWithValidSize_convertsToVarbyte() {
        Column blobPk = Column.newBuilder()
                .setName("pk_col")
                .setType(DataType.BINARY)
                .setPrimaryKey(true)
                .setParams(DataTypeParams.newBuilder().setStringByteLength(1000).build())
                .build();

        String definition = TeradataJDBCUtil.getColumnDefinition(blobPk);

        assertTrue(definition.contains("VARBYTE(1000)"), "Expected VARBYTE(1000) but got: " + definition);
        assertTrue(definition.contains("NOT NULL"), "PK column should have NOT NULL");
    }

    @Test
    public void binaryPkWithMaxValidSize_convertsToVarbyte() {
        Column blobPk = Column.newBuilder()
                .setName("pk_col")
                .setType(DataType.BINARY)
                .setPrimaryKey(true)
                .setParams(DataTypeParams.newBuilder().setStringByteLength(64000).build())
                .build();

        String definition = TeradataJDBCUtil.getColumnDefinition(blobPk);

        assertTrue(definition.contains("VARBYTE(64000)"), "Expected VARBYTE(64000) but got: " + definition);
    }

    @Test
    public void binaryPkExceedingMaxSize_throwsError() {
        Column blobPk = Column.newBuilder()
                .setName("large_pk")
                .setType(DataType.BINARY)
                .setPrimaryKey(true)
                .setParams(DataTypeParams.newBuilder().setStringByteLength(100000).build())
                .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            TeradataJDBCUtil.getColumnDefinition(blobPk);
        });

        assertTrue(ex.getMessage().contains("large_pk"), "Error should reference column name");
        assertTrue(ex.getMessage().contains("BLOB/CLOB"), "Error should mention BLOB/CLOB");
        assertTrue(ex.getMessage().contains("64"), "Error should mention the 64KB limit");
    }

    @Test
    public void binaryPkWithUnknownSize_throwsError() {
        Column blobPk = Column.newBuilder()
                .setName("unknown_pk")
                .setType(DataType.BINARY)
                .setPrimaryKey(true)
                .setParams(DataTypeParams.newBuilder().setStringByteLength(0).build())
                .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            TeradataJDBCUtil.getColumnDefinition(blobPk);
        });

        assertTrue(ex.getMessage().contains("unknown_pk"), "Error should reference column name");
        assertTrue(ex.getMessage().contains("unknown size"), "Error should mention unknown size");
    }

    @Test
    public void binaryPkWithNoParams_throwsError() {
        Column blobPk = Column.newBuilder()
                .setName("no_params_pk")
                .setType(DataType.BINARY)
                .setPrimaryKey(true)
                .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            TeradataJDBCUtil.getColumnDefinition(blobPk);
        });

        assertTrue(ex.getMessage().contains("no_params_pk"), "Error should reference column name");
    }

    @Test
    public void binaryNonPk_remainsBlob() {
        Column blobNonPk = Column.newBuilder()
                .setName("data_col")
                .setType(DataType.BINARY)
                .setPrimaryKey(false)
                .setParams(DataTypeParams.newBuilder().setStringByteLength(1000).build())
                .build();

        String definition = TeradataJDBCUtil.getColumnDefinition(blobNonPk);

        assertTrue(definition.contains("BLOB"), "Non-PK BINARY should remain BLOB but got: " + definition);
        assertFalse(definition.contains("VARBYTE"), "Non-PK BINARY should not be VARBYTE");
        assertFalse(definition.contains("NOT NULL"), "Non-PK should not have NOT NULL");
    }

    @Test
    public void mixedPkWithBinaryAndInt_bothHandledCorrectly() {
        Column intPk = Column.newBuilder()
                .setName("id")
                .setType(DataType.INT)
                .setPrimaryKey(true)
                .build();

        Column blobPk = Column.newBuilder()
                .setName("hash_col")
                .setType(DataType.BINARY)
                .setPrimaryKey(true)
                .setParams(DataTypeParams.newBuilder().setStringByteLength(500).build())
                .build();

        List<Column> columns = Arrays.asList(intPk, blobPk);
        String columnDefs = TeradataJDBCUtil.getColumnDefinitions(columns);

        assertTrue(columnDefs.contains("INTEGER"), "INT PK should be INTEGER");
        assertTrue(columnDefs.contains("VARBYTE(500)"), "BINARY PK should be VARBYTE(500) but got: " + columnDefs);
        assertTrue(columnDefs.contains("PRIMARY KEY"), "Should have PRIMARY KEY constraint");
    }

    @Test
    public void createTableWithBinaryPkExceedingLimit_failsGracefully() {
        Table table = Table.newBuilder()
                .setName("test_table")
                .addAllColumns(Arrays.asList(
                        Column.newBuilder().setName("id").setType(DataType.INT).setPrimaryKey(true).build(),
                        Column.newBuilder().setName("blob_pk").setType(DataType.BINARY).setPrimaryKey(true)
                                .setParams(DataTypeParams.newBuilder().setStringByteLength(100000).build()).build()
                ))
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            TeradataJDBCUtil.generateCreateTableQuery("test_db", "test_table", table);
        });
    }
}
