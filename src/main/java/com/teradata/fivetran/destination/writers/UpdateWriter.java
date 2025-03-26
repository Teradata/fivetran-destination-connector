package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.FileParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateWriter extends Writer {
    private static final Logger logger = LoggerFactory.getLogger(UpdateWriter.class);
    private List<Column> headerColumns = new ArrayList<>();

    public UpdateWriter(Connection conn, String database, String table, List<Column> columns,
                        FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        logger.info("UpdateWriter initialized with database: {}, table: {}, batchSize: {}", database, table, batchSize);
    }

    @Override
    public void setHeader(List<String> header) {
        //logger.info("Setting header with columns: {}", header);
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            nameToColumn.put(column.getName(), column);
        }

        for (String name : header) {
            headerColumns.add(nameToColumn.get(name));
        }
        logger.info("Header columns set: {}", headerColumns);
    }

    @Override
    public void writeRow(List<String> row) throws SQLException {
        logger.info("Writing row: {}", row);
        StringBuilder updateClause = new StringBuilder(
                String.format("UPDATE %s SET ", TeradataJDBCUtil.escapeTable(database, table)));
        StringBuilder whereClause = new StringBuilder("WHERE ");

        boolean firstUpdateColumn = true;
        boolean firstPKColumn = true;

        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (!row.get(i).equals(params.getUnmodifiedString())) {
                if (firstUpdateColumn) {
                    updateClause.append(
                            String.format("%s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
                    firstUpdateColumn = false;
                } else {
                    updateClause.append(
                            String.format(", %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
                }
            }

            if (c.getPrimaryKey()) {
                if (firstPKColumn) {
                    whereClause.append(
                            String.format("%s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
                    firstPKColumn = false;
                } else {
                    whereClause.append(
                            String.format("AND %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
                }
            }
        }

        if (firstUpdateColumn) {
            logger.info("No columns to update for row: {}", row);
            return;
        }

        String query = updateClause.toString() + " " + whereClause;
        logger.info("Prepared SQL update statement: {}", query);

        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                if (value.equals(params.getUnmodifiedString())) {
                    continue;
                }

                paramIndex++;
                TeradataJDBCUtil.setParameter(stmt, paramIndex, headerColumns.get(i).getType(), value,
                        params.getNullString());
                //logger.info("Set parameter at index {}: {}", paramIndex, value);
            }

            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                if (!headerColumns.get(i).getPrimaryKey()) {
                    continue;
                }
                paramIndex++;
                TeradataJDBCUtil.setParameter(stmt, paramIndex, headerColumns.get(i).getType(), value,
                        params.getNullString());
                logger.info("Set primary key parameter at index {}: {}", paramIndex, value);
            }

            stmt.execute();
            logger.info("Executed update statement for row: {}", row);
        } catch (SQLException e) {
            logger.error("Failed to execute update statement for row: {}", row, e);
            throw e;
        }
    }

    @Override
    public void commit() {
        //logger.info("Commit called, but no action required for UpdateWriter.");
    }
}