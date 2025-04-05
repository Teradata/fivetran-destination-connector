package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.FileParams;
import org.apache.commons.lang3.StringEscapeUtils;

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
    public EarliestStartHistoryWriter(Connection conn, String database, String schema, String table, List<Column> columns, FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        logMessage("INFO",String.format("EarliestStartHistoryWriter initialized with database: %s, table: %s, batchSize: %s", database, table, batchSize));
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
        logMessage("INFO","#########################EarliestStartHistoryWriter.writeDelete#############################################################");
        StringBuilder deleteQuery = new StringBuilder(String.format("DELETE FROM %s WHERE ", TeradataJDBCUtil.escapeTable(database, schema, table)));

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
        logMessage("INFO", "deleteQuery is " + deleteQuery);
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
            stmt.execute();
            logMessage("INFO", "Executed delete statement for row: " + row);
        }
    }

    /**
     * Writes an update statement for the given row.
     *
     * @param row The list of row values.
     * @throws Exception If an error occurs.
     */
    public void writeUpdate(List<String> row) throws Exception {
        logMessage("INFO","#########################EarliestStartHistoryWriter.writeUpdate#############################################################");
        StringBuilder updateQuery = new StringBuilder(String.format(
                "UPDATE %s SET _fivetran_active = 0, _fivetran_end = ? - INTERVAL '1' SECOND WHERE _fivetran_active = 1 ",
                TeradataJDBCUtil.escapeTable(database, schema, table)));
        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (c != null && c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                updateQuery.append(
                        String.format("AND %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
            }
        }
        logMessage("INFO", "updateQuery is " + updateQuery);
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
            stmt.execute();
            logMessage("INFO", "Executed update statement for row: " + row);
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
        logMessage("INFO", "#########################EarliestStartHistoryWriter.writeRow#############################################################");
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

    private void logMessage(String level, String message) {
        message = StringEscapeUtils.escapeJava(message);
        System.out.println(String.format("{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}", level, message));
    }
}
