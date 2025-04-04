package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import com.teradata.fivetran.destination.warning_util.WarningHandler;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.DataType;
import org.apache.commons.lang3.StringEscapeUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LoadDataWriter<T> extends Writer {
    private List<Column> headerColumns;
    private PreparedStatement preparedStatement;
    private final WarningHandler<T> warningHandler;
    private int currentBatchSize = 0;
    private String temp_table;
    private List<Column> columns;
    private List<Column> matchingCols;

    /**
     * Constructor for LoadDataWriter.
     *
     * @param conn           The database connection.
     * @param database       The database name.
     * @param table          The table name.
     * @param columns        The list of columns.
     * @param params         The file parameters.
     * @param secretKeys     The map of secret keys.
     * @param batchSize      The batch size for writing rows.
     * @param warningHandler The warning handler.
     * @throws IOException If an I/O error occurs.
     */
    public LoadDataWriter(Connection conn, String database, String schema, String table, List<Column> columns,
                          FileParams params, Map<String, ByteString> secretKeys, Integer batchSize,
                          WarningHandler<T> warningHandler) throws IOException {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        this.warningHandler = warningHandler;
        this.columns = columns;
        this.batchSize=batchSize;
        logMessage("INFO", String.format("LoadDataWriter initialized with database: %s, table: %s, batchSize: %s", database, table, batchSize));
    }

    @Override
    public void setHeader(List<String> header) throws SQLException {
        logMessage("INFO","Setting header with columns: " + header);
        headerColumns = new ArrayList<>();
        matchingCols = columns.stream()
                .filter(Column::getPrimaryKey)
                .collect(Collectors.toList());
        Map<String, Column> nameToColumn = columns.stream().collect(Collectors.toMap(Column::getName, col -> col));

        for (String name : header) {
            headerColumns.add(nameToColumn.get(name));
        }

        String columnNames = headerColumns.stream()
                .map(Column::getName)
                .map(TeradataJDBCUtil::escapeIdentifier)
                .collect(Collectors.joining(", "));

        String placeholders = headerColumns.stream().map(c -> "?").collect(Collectors.joining(", "));

        temp_table = String.format("%s_%s_%d", table, "tmp_del_ins", System.currentTimeMillis());

        String createTempTable = String.format("CREATE TABLE %s AS (SELECT * FROM %s) WITH NO DATA;",
                TeradataJDBCUtil.escapeTable(database, schema, temp_table), TeradataJDBCUtil.escapeTable(database, schema, table));
        logMessage("INFO","Creating temporary table: " + createTempTable);
        try {
            dropTempTable();
            conn.createStatement().execute(createTempTable);
        } catch (SQLException e) {
            logMessage("SEVERE","Failed to create temporary table: " + e.getMessage());
            throw new SQLException("Failed to create temporary table: " + e.getMessage(), e);
        }

        String query = String.format("INSERT INTO %s (%s) VALUES (%s)",
                TeradataJDBCUtil.escapeTable(database, schema, temp_table), columnNames, placeholders);

        logMessage("INFO","Prepared SQL statement: " + query);
        preparedStatement = conn.prepareStatement(query);
    }

    @Override
    public void writeRow(List<String> row) throws Exception {
        logMessage("INFO","#########################LoadDataWriter.writeRow#########################");
        try {
            for (int i = 0; i < row.size(); i++) {
                DataType type = headerColumns.get(i).getType();
                String value = row.get(i);

                if (type == DataType.BOOLEAN) {
                    if (value.equalsIgnoreCase("true")) {
                        preparedStatement.setShort(i + 1, Short.parseShort("1"));
                    } else if (value.equalsIgnoreCase("false")) {
                        preparedStatement.setShort(i + 1, Short.parseShort("0"));
                    } else {
                        preparedStatement.setShort(i + 1, Short.parseShort(value));
                    }
                } else if (type == DataType.SHORT) {
                    preparedStatement.setShort(i + 1, Short.parseShort(value));
                } else if (type == DataType.INT) {
                    preparedStatement.setInt(i + 1, Integer.parseInt(value));
                } else if (type == DataType.LONG) {
                    preparedStatement.setLong(i + 1, Long.parseLong(value));
                } else if (type == DataType.DECIMAL) {
                    preparedStatement.setBigDecimal(i + 1, new BigDecimal(value));
                } else if (type == DataType.FLOAT) {
                    preparedStatement.setFloat(i + 1, Float.parseFloat(value));
                } else if (type == DataType.DOUBLE) {
                    preparedStatement.setDouble(i + 1, Double.parseDouble(value));
                } else if (type == DataType.NAIVE_DATETIME || type == DataType.UTC_DATETIME && !value.equals(params.getNullString())) {
                    preparedStatement.setTimestamp(i + 1, TeradataJDBCUtil.getTimestampFromObject(TeradataJDBCUtil.formatISODateTime(value)));
                } else if (type == DataType.BINARY) {
                    preparedStatement.setBytes(i + 1, Base64.getDecoder().decode(value));
                } else if (type == DataType.XML) {
                    SQLXML sqlxml =  preparedStatement.getConnection().createSQLXML();
                    sqlxml.setString(value);
                    preparedStatement.setSQLXML(i + 1, sqlxml);
                } else if (type == DataType.STRING) {
                    preparedStatement.setString(i + 1, value);
                } else {
                    preparedStatement.setObject(i + 1, value);
                }
                logMessage("INFO", String.format("Set parameter at index %d: %s", i + 1, value));
            }

            preparedStatement.addBatch();
            currentBatchSize++;
            logMessage("INFO", "Added row to batch. Current batch size: " + currentBatchSize);

            if (currentBatchSize >= batchSize) {
                logMessage("INFO", "Batch size limit reached. Committing batch.");
                commit();
            }
        } catch (Exception e) {
            warningHandler.handle("Failed to write row to batch", e);
            logMessage("SEVERE","Failed to write row to batch " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void commit() throws SQLException {
        if (currentBatchSize > 0) {
            logMessage("INFO","Committing batch of size: " + currentBatchSize);
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
            currentBatchSize = 0;
            logMessage("INFO", "Batch committed successfully.");
        } else {
            logMessage("INFO","No rows to commit.");
        }
    }

    public void deleteInsert() throws SQLException {
        String cols = matchingCols.stream()
                .map(Column::getName)
                .map(TeradataJDBCUtil::escapeIdentifier)
                .collect(Collectors.joining(", "));
        if (!cols.isEmpty()) {
            String condition = matchingCols.stream()
                    .map(Column::getName)
                    .map(col -> String.format("t.%s = tmp.%s", col, col))
                    .collect(Collectors.joining(" AND "));

            String deleteQuery = String.format(
                    "DELETE FROM %s AS t WHERE EXISTS (SELECT 1 FROM %s AS tmp WHERE %s)",
                    TeradataJDBCUtil.escapeTable(database, schema, table),
                    TeradataJDBCUtil.escapeTable(database, schema, temp_table),
                    condition
            );

            logMessage("INFO","Prepared SQL delete statement: " + deleteQuery);
            try {
                conn.createStatement().execute(deleteQuery);
                logMessage("INFO","Delete operation completed successfully.");
            } catch (SQLException e) {
                logMessage("SEVERE","Failed to execute delete statement: " + e.getMessage());
                dropTempTable();
                throw e;
            }
        }

        String insertQuery = String.format("INSERT INTO %s SELECT * FROM %s",
                TeradataJDBCUtil.escapeTable(database, schema, table), TeradataJDBCUtil.escapeTable(database, schema, temp_table));
        logMessage("INFO","Prepared SQL insert statement: " + insertQuery);
        try {
            conn.createStatement().execute(insertQuery);
            logMessage("INFO","Insert operation completed successfully.");
        } catch (SQLException e) {
            logMessage("SEVERE", "Failed to execute insert statement: " + e.getMessage());
            dropTempTable();
            throw e;
        }
        dropTempTable();
    }

    private void dropTempTable() throws SQLException {
        String deleteQuery = String.format("DELETE FROM %s", TeradataJDBCUtil.escapeTable(database, schema, temp_table));
        String dropQuery = String.format("DROP TABLE %s", TeradataJDBCUtil.escapeTable(database, schema, temp_table));
        logMessage("INFO","Prepared SQL delete statement: " + deleteQuery);
        logMessage("INFO","Prepared SQL drop statement: " + dropQuery);
        try {
            conn.createStatement().execute(deleteQuery);
            logMessage("INFO","Temporary table deleted successfully.");
        } catch (SQLException e) {
            logMessage("SEVERE","Failed to delete temporary table: " + e.getMessage());
        }
        try {
            conn.createStatement().execute(dropQuery);
            logMessage("INFO","Temporary table dropped successfully.");
        } catch (SQLException e) {
            logMessage("SEVERE","Failed to drop temporary table: " + e.getMessage());
        }
    }

    private void logMessage(String level, String message) {
        message = StringEscapeUtils.escapeJava(message);
        System.out.println(String.format("{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}", level, message));
    }
}