package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.FileParams;
import org.apache.commons.lang3.StringEscapeUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to handle writing updates for a table.
 */
public class UpdateWriter extends Writer {
    private List<Column> headerColumns = new ArrayList<>();

    /**
     * Constructor to initialize UpdateWriter.
     *
     * @param conn The database connection.
     * @param database The database name.
     * @param table The table name.
     * @param columns The list of columns.
     * @param params The file parameters.
     * @param secretKeys The map of secret keys.
     * @param batchSize The batch size.
     */
    public UpdateWriter(Connection conn, String database, String table, List<Column> columns,
                        FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        logMessage("INFO",String.format("UpdateWriter initialized with database: %s, table: %s, batchSize: %s", database, table, batchSize));
    }

    /**
     * Sets the header for the writer.
     *
     * @param header The list of header column names.
     */
    @Override
    public void setHeader(List<String> header) {
        logMessage("INFO","Setting header with columns: " + header);
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            nameToColumn.put(column.getName(), column);
        }

        for (String name : header) {
            headerColumns.add(nameToColumn.get(name));
        }
        logMessage("INFO","Header columns set: " + headerColumns);
    }

    /**
     * Writes a row to the writer.
     *
     * @param row The list of row values.
     * @throws SQLException If an SQL error occurs.
     */
    @Override
    public void writeRow(List<String> row) throws SQLException {
        logMessage("INFO","Writing row: " + row);
        StringBuilder updateClause = new StringBuilder(
                String.format("UPDATE %s SET ", TeradataJDBCUtil.escapeTable(database, schema, table)));
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
            logMessage("INFO","No columns to update for row: " + row);
            return;
        }

        String query = updateClause.toString() + " " + whereClause;
        logMessage("INFO","Prepared SQL update statement: " + query);

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
                logMessage("INFO",String.format("Set parameter at index %d: %s", paramIndex, value));
            }

            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                if (!headerColumns.get(i).getPrimaryKey()) {
                    continue;
                }

                paramIndex++;
                TeradataJDBCUtil.setParameter(stmt, paramIndex, headerColumns.get(i).getType(), value,
                        params.getNullString());
                logMessage("INFO",String.format("Set primary key parameter at index %d: %s", paramIndex, value));
            }

            stmt.execute();
            logMessage("INFO","Executed update statement for row: " + row);
        } catch (SQLException e) {
            logMessage("SEVERE",String.format("Failed to execute update statement for row: %s, %s", row, e.getMessage()));
            throw e;
        }
    }

    /**
     * Commits the written rows to the database.
     */
    @Override
    public void commit() {
        logMessage("INFO","Commit called, but no action required for UpdateWriter.");
    }

    private void logMessage(String level, String message) {
        message = StringEscapeUtils.escapeJava(message);
        System.out.println(String.format("{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}", level, message));
    }
}