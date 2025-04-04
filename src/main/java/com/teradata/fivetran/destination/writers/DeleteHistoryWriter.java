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
 * Class to handle writing delete history for a table.
 */
public class DeleteHistoryWriter extends Writer {

    /**
     * Constructor to initialize DeleteHistoryWriter.
     *
     * @param conn The database connection.
     * @param database The database name.
     * @param table The table name.
     * @param columns The list of columns.
     * @param params The file parameters.
     * @param secretKeys The map of secret keys.
     * @param batchSize The batch size.
     */
    public DeleteHistoryWriter(Connection conn, String database, String table, List<Column> columns, FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        logMessage("INFO",String.format("DeleteHistoryWriter initialized with database: %s, table: %s, batchSize: %s", database, table, batchSize));
    }

    List<Column> headerColumns = new ArrayList<>();
    Integer fivetranEndPos;

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
            if (name.equals("_fivetran_end")) {
                fivetranEndPos = i;
            }
            headerColumns.add(nameToColumn.get(name));
        }

        if (fivetranEndPos == null) {
            throw new IllegalArgumentException("File doesn't contain _fivetran_end column");
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
        logMessage("INFO","#########################DeleteHistoryWriter.writeRow#########################");
        StringBuilder updateQuery = new StringBuilder(String.format(
                "UPDATE %s SET _fivetran_active = 0, _fivetran_end = ? WHERE _fivetran_active = 1 ",
                TeradataJDBCUtil.escapeTable(database, table)));
        logMessage("INFO","updateQuery is " + updateQuery);

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
            logMessage("INFO","Executed update statement for row: " + row);
        }
    }

    /**
     * Commits the written rows to the database.
     */
    @Override
    public void commit() {
        // Implementation for commit
    }

    private void logMessage(String level, String message) {
        message = StringEscapeUtils.escapeJava(message);
        System.out.println(String.format("{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}", level, message));
    }
}