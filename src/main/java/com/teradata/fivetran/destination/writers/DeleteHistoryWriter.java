package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.FileParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteHistoryWriter extends Writer {
    private static final Logger logger = LoggerFactory.getLogger(DeleteHistoryWriter.class);
    public DeleteHistoryWriter(Connection conn, String database, String table, List<Column> columns, FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        logger.info("DeleteHistoryWriter initialized with database: {}, table: {}, batchSize: {}", database, table, batchSize);
    }

    List<Column> headerColumns = new ArrayList<>();
    Integer fivetranEndPos;

    @Override
    public void setHeader(List<String> header) throws SQLException, IOException {
        logger.info("in DeleteHistoryWriter.setHeader");
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            nameToColumn.put(column.getName(), column);
        }

        for (int i = 0; i < header.size(); i++) {
            String name = header.get(i);
            if (name.equals("_fivetran_end")) {
                fivetranEndPos = i;
            }
            headerColumns.add(nameToColumn.get(name));
        }

        if (fivetranEndPos == null) {
            throw new IllegalArgumentException("File doesn't contain _fivetran_end column");
        }
    }

    @Override
    public void writeRow(List<String> row) throws Exception {
        logger.info("#########################DeleteHistoryWriter.writeRow#############################################################");
        StringBuilder updateQuery = new StringBuilder(String.format(
                "UPDATE %s SET _fivetran_active = 0, _fivetran_end = ? WHERE _fivetran_active = TRUE ",
                TeradataJDBCUtil.escapeTable(database, table)));

        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (c != null && c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                updateQuery.append(
                        String.format("AND %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
            }
        }

        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(updateQuery.toString())) {
            paramIndex++;
            TeradataJDBCUtil.setParameter(stmt, paramIndex, DataType.UTC_DATETIME, row.get(fivetranEndPos), params.getNullString());

            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                Column c = headerColumns.get(i);
                if (c == null || !c.getPrimaryKey() || c.getName().equals("_fivetran_start")) {
                    continue;
                }

                paramIndex++;
                TeradataJDBCUtil.setParameter(stmt, paramIndex, c.getType(), value, params.getNullString());
            }

            stmt.execute();
        }
    }

    @Override
    public void commit() {
    }
}