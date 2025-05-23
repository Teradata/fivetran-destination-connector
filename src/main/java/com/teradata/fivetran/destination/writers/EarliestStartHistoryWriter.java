package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.Logger;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.FileParams;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to handle writing earliest start history for a table.
 */
public class EarliestStartHistoryWriter extends Writer {

    /**
     * Constructor to initialize EarliestStartHistoryWriter.
     *
     * @param conn The database connection.
     * @param database The database name.
     * @param table The table name.
     * @param columns The list of columns.
     * @param params The file parameters.
     * @param secretKeys The map of secret keys.
     * @param batchSize The batch size.
     */
    public EarliestStartHistoryWriter(Connection conn, String database, String table, List<Column> columns, FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("EarliestStartHistoryWriter initialized with database: %s, table: %s, batchSize: %s", database, table, batchSize));
    }

    List<Column> headerColumns = new ArrayList<>();
    Integer earliestFivetranStartPos;

    /**
     * Sets the header for the writer.
     *
     * @param header The list of header column names.
     * @throws SQLException If an SQL error occurs.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void setHeader(List<String> header) throws SQLException, IOException {
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            nameToColumn.put(column.getName(), column);
        }
        for (int i = 0; i < header.size(); i++) {
            String name = header.get(i);
            if (name.equals("_fivetran_start")) {
                earliestFivetranStartPos = i;
            }
            headerColumns.add(nameToColumn.get(name));
        }
        if (earliestFivetranStartPos == null) {
            throw new IllegalArgumentException("File doesn't contain _fivetran_start column");
        }
    }

    /**
     * Writes a delete statement for the given row.
     *
     * @param row The list of row values.
     * @throws Exception If an error occurs.
     */
    public void writeDelete(List<String> row) throws Exception {
        Logger.logMessage(Logger.debugLogLevel, "#########################EarliestStartHistoryWriter.writeDelete#############################################################");
        StringBuilder deleteQuery = new StringBuilder(String.format("DELETE FROM %s WHERE ", TeradataJDBCUtil.escapeTable(database, table)));

        boolean firstPKColumn = true;
        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (c != null && c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                if (firstPKColumn) {
                    deleteQuery.append(
                            String.format("%s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
                    firstPKColumn = false;
                } else {
                    deleteQuery.append(
                            String.format("AND %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
                }
            }
        }

        deleteQuery.append("AND _fivetran_start >= ?");
        Logger.logMessage(Logger.LogLevel.INFO,"deleteQuery is " + deleteQuery);
        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(deleteQuery.toString())) {
            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                Column c = headerColumns.get(i);
                if (c == null || !c.getPrimaryKey() || c.getName().equals("_fivetran_start")) {
                    continue;
                }

                paramIndex++;
                TeradataJDBCUtil.setParameter(stmt, paramIndex, c.getType(), value, params.getNullString());
            }

            paramIndex++;
            TeradataJDBCUtil.setParameter(stmt, paramIndex, DataType.UTC_DATETIME, row.get(earliestFivetranStartPos), params.getNullString());
            try {
                stmt.execute();
            } catch (SQLException e) {
                throw new SQLException("Failed to execute (" + deleteQuery + ") on table: "
                        + TeradataJDBCUtil.escapeTable(database, table) + " with error: "
                        + e.getMessage(), e);
            }
            Logger.logMessage(Logger.debugLogLevel,"Executed delete statement: " + stmt.toString());
        }
    }

    /**
     * Writes an update statement for the given row.
     *
     * @param row The list of row values.
     * @throws Exception If an error occurs.
     */
    public void writeUpdate(List<String> row) throws Exception {
        Logger.logMessage(Logger.debugLogLevel, "#########################EarliestStartHistoryWriter.writeUpdate#############################################################");
        StringBuilder updateQuery = new StringBuilder(String.format(
                "UPDATE %s SET _fivetran_active = 0, _fivetran_end = ? - INTERVAL '1' SECOND WHERE _fivetran_active = 1 ",
                TeradataJDBCUtil.escapeTable(database, table)));
        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (c != null && c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                updateQuery.append(
                        String.format("AND %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
            }
        }
        Logger.logMessage(Logger.LogLevel.INFO,"updateQuery is " + updateQuery);
        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(updateQuery.toString())) {
            paramIndex++;
            TeradataJDBCUtil.setParameter(stmt, paramIndex, DataType.UTC_DATETIME, row.get(earliestFivetranStartPos), params.getNullString());
            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                Column c = headerColumns.get(i);
                if (c == null || !c.getPrimaryKey() || c.getName().equals("_fivetran_start")) {
                    continue;
                }

                paramIndex++;
                TeradataJDBCUtil.setParameter(stmt, paramIndex, c.getType(), value, params.getNullString());
            }
            try {
                stmt.execute();
            } catch (SQLException e) {
                throw new SQLException("Failed to execute (" + updateQuery + ") on table: "
                        + TeradataJDBCUtil.escapeTable(database, table) + " with error: "
                        + e.getMessage(), e);
            }
            Logger.logMessage(Logger.debugLogLevel,"Executed update statement: " + stmt.toString());
        }
    }

    /**
     * Writes a row to the writer.
     *
     * @param row The list of row values.
     * @throws Exception If an error occurs.
     */
    @Override
    public void writeRow(List<String> row) throws Exception {
        Logger.logMessage(Logger.debugLogLevel,
                "#########################EarliestStartHistoryWriter.writeRow#############################################################");
        writeDelete(row);
        writeUpdate(row);
    }

    /**
     * Commits the written rows to the database.
     *
     * @throws InterruptedException If the thread is interrupted.
     * @throws IOException If an I/O error occurs.
     * @throws SQLException If an SQL error occurs.
     */
    @Override
    public void commit() throws InterruptedException, IOException, SQLException {
        // Implementation for commit
    }
}
