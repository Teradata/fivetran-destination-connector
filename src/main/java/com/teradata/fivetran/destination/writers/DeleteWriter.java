package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.Logger;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.FileParams;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteWriter extends Writer {
    private final List<Integer> pkIds = new ArrayList<>();
    private final List<Column> pkColumns = new ArrayList<>();
    private final List<List<String>> rows = new ArrayList<>();

    /**
     * Constructor for DeleteWriter.
     *
     * @param conn       The database connection.
     * @param database   The database name.
     * @param table      The table name.
     * @param columns    The list of columns.
     * @param params     The file parameters.
     * @param secretKeys The map of secret keys.
     * @param batchSize  The batch size for writing rows.
     */
    public DeleteWriter(Connection conn, String database, String table, List<Column> columns,
                        FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("DeleteWriter initialized with database: %s, table: %s, batchSize: %s", database, table, batchSize));
    }

    /**
     * Sets the header for the CSV file and identifies primary key columns.
     *
     * @param header The list of header columns.
     */
    @Override
    public void setHeader(List<String> header) {
        Map<String, Column> nameToColumn = columns.stream()
                .filter(Column::getPrimaryKey)
                .collect(Collectors.toMap(Column::getName, column -> column));

        for (int i = 0; i < header.size(); i++) {
            String columnName = header.get(i);
            if (nameToColumn.containsKey(columnName)) {
                pkIds.add(i);
                pkColumns.add(nameToColumn.get(columnName));
            }
        }
    }

    /**
     * Adds a row to the list of rows to be deleted.
     *
     * @param row The list of row values.
     * @throws SQLException If an error occurs while adding the row.
     */
    @Override
    public void writeRow(List<String> row) throws SQLException {
        Logger.logMessage(Logger.debugLogLevel, "#########################DeleteWriter.writeRow#########################");
        rows.add(row);
    }

    /**
     * Commits the current batch of rows to the database by executing a DELETE statement.
     *
     * @throws SQLException If an error occurs while committing.
     */
    @Override
    public void commit() throws SQLException {
        if (rows.isEmpty()) {
            return;
        }

        String condition = pkColumns.stream()
                .map(column -> String.format("%s = ?", TeradataJDBCUtil.escapeIdentifier(column.getName())))
                .collect(Collectors.joining(" AND "));

        String query = String.format("DELETE FROM %s WHERE ", TeradataJDBCUtil.escapeTable(database, table)) +
                rows.stream().map(row -> "(" + condition + ")").collect(Collectors.joining(" OR "));

        Logger.logMessage(Logger.LogLevel.INFO,"Prepared SQL statement: " + query);

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < rows.size(); i++) {
                List<String> row = rows.get(i);
                for (int j = 0; j < pkIds.size(); j++) {
                    int paramIndex = i * pkIds.size() + j + 1;
                    String value = row.get(pkIds.get(j));
                    TeradataJDBCUtil.setParameter(stmt, paramIndex, pkColumns.get(j).getType(), value, params.getNullString());
                }
            }
            try {
                stmt.execute();
            } catch (SQLException e) {
                throw new SQLException("Failed to execute (" + query + ") on table: "
                        + TeradataJDBCUtil.escapeTable(database, table) + " with error: "
                        + e.getMessage(), e);
            }
        }

        rows.clear();
    }
}