package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.FileParams;
import org.apache.commons.lang3.StringEscapeUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Class to handle writing update history for a table.
 */
public class UpdateHistoryWriter extends Writer {
    private final List<List<String>> rows = new ArrayList<>();

    /**
     * Constructor to initialize UpdateHistoryWriter.
     *
     * @param conn The database connection.
     * @param database The database name.
     * @param table The table name.
     * @param columns The list of columns.
     * @param params The file parameters.
     * @param secretKeys The map of secret keys.
     * @param batchSize The batch size.
     */
    public UpdateHistoryWriter(Connection conn, String database, String table, List<Column> columns,
                               FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        logMessage("INFO", String.format("UpdateHistoryWriter initialized with database: %s, table: %s, batchSize: %s", database, table, batchSize));
    }

    private List<Column> headerColumns = new ArrayList<>();
    private Integer fivetranStartPos;
    private Map<String, Integer> nameToHeaderPos = new HashMap<>();

    /**
     * Sets the header for the writer.
     *
     * @param header The list of header column names.
     */
    @Override
    public void setHeader(List<String> header) {
        for (int i = 0; i < header.size(); i++) {
            nameToHeaderPos.put(header.get(i), i);
        }

        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            nameToColumn.put(column.getName(), column);
        }

        for (int i = 0; i < header.size(); i++) {
            String name = header.get(i);
            if (name.equals("_fivetran_start")) {
                fivetranStartPos = i;
            }
            headerColumns.add(nameToColumn.get(name));
        }

        if (fivetranStartPos == null) {
            throw new IllegalArgumentException("File doesn't contain _fivetran_start column");
        }
    }

    /**
     * Writes a row to the writer.
     *
     * @param row The list of row values.
     * @throws SQLException If an SQL error occurs.
     */
    @Override
    public void writeRow(List<String> row) throws SQLException {
        logMessage("INFO", "#########################UpdateHistoryWriter.writeRow#########################");
        rows.add(row);
        commit();
    }

    /**
     * Commits the written rows to the database.
     *
     * @throws SQLException If an SQL error occurs.
     */
    @Override
    public void commit() throws SQLException {
        logMessage("INFO", "#########################UpdateHistoryWriter.commit#########################");
        rows.sort(Comparator.comparing(row -> {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            String dateString = TeradataJDBCUtil.formatISODateTime(row.get(fivetranStartPos));

            return LocalDateTime.parse(dateString, formatter);
        }));
        for (List<String> row : rows) {
            processRow(row);
        }
        rows.clear();
    }

    /**
     * Processes a row by inserting a new row and updating the old row.
     *
     * @param row The list of row values.
     * @throws SQLException If an SQL error occurs.
     */
    private void processRow(List<String> row) throws SQLException {
        logMessage("INFO", "#########################UpdateHistoryWriter.processRow#########################");
        logMessage("INFO", "Processing row: " + row);
        insertNewRow(row);
        updateOldRow(row);
    }

    /**
     * Inserts a new row into the database.
     *
     * @param row The list of row values.
     * @throws SQLException If an SQL error occurs.
     */
    private void insertNewRow(List<String> row) throws SQLException {
        logMessage("INFO", "#########################UpdateHistoryWriter.insertNewRow#########################");
        logMessage("INFO", "Inserting new row: " + row);
        StringBuilder insertQuery = new StringBuilder(String.format(
                "INSERT INTO %s SELECT ",
                TeradataJDBCUtil.escapeTable(database, table)));

        boolean firstColumn = true;
        for (Column c : columns) {
            if (!firstColumn) {
                insertQuery.append(", ");
            }

            Integer pos = nameToHeaderPos.get(c.getName());
            if (pos == null || row.get(pos).equals(params.getUnmodifiedString())) {
                insertQuery.append(TeradataJDBCUtil.escapeIdentifier(c.getName()));
            } else {
                insertQuery.append("?");
            }

            firstColumn = false;
        }

        insertQuery.append(String.format(" FROM %s WHERE _fivetran_active = 1 ", TeradataJDBCUtil.escapeTable(database, table)));

        for (Column c : columns) {
            if (c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                insertQuery.append(String.format("AND %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
            }
        }
        logMessage("INFO", "Insert query is " + insertQuery);
        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(insertQuery.toString())) {
            for (Column c : columns) {
                Integer pos = nameToHeaderPos.get(c.getName());
                if (pos != null && !row.get(pos).equals(params.getUnmodifiedString())) {
                    paramIndex++;
                    TeradataJDBCUtil.setParameter(stmt, paramIndex, c.getType(), row.get(pos), params.getNullString());
                }
            }

            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                Column c = headerColumns.get(i);
                if (c != null && c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                    paramIndex++;
                    TeradataJDBCUtil.setParameter(stmt, paramIndex, c.getType(), value, params.getNullString());
                }
            }

            stmt.execute();
            logMessage("INFO", "Executed insert statement for row: " + row);
        }
    }

    /**
     * Updates the old row in the database.
     *
     * @param row The list of row values.
     * @throws SQLException If an SQL error occurs.
     */
    private void updateOldRow(List<String> row) throws SQLException {
        logMessage("INFO", "#########################UpdateHistoryWriter.updateOldRow#########################");
        logMessage("INFO", "Updating old row: " + row);
        StringBuilder updateQuery = new StringBuilder(String.format(
                "UPDATE %s SET _fivetran_active = 0, _fivetran_end = ? - INTERVAL '1' SECOND WHERE _fivetran_active = 1 AND _fivetran_start < ? ",
                TeradataJDBCUtil.escapeTable(database, table)));

        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (c != null && c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                updateQuery.append(
                        String.format("AND %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
            }
        }
        logMessage("INFO", "Update query is " + updateQuery);
        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(updateQuery.toString())) {
            paramIndex++;
            TeradataJDBCUtil.setParameter(stmt, paramIndex, DataType.UTC_DATETIME, row.get(fivetranStartPos), params.getNullString());
            paramIndex++;
            TeradataJDBCUtil.setParameter(stmt, paramIndex, DataType.UTC_DATETIME, row.get(fivetranStartPos), params.getNullString());

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

    private void logMessage(String level, String message) {
        message = StringEscapeUtils.escapeJava(message);
        System.out.println(String.format("{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}", level, message));
    }

}