package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import com.teradata.fivetran.destination.warning_util.WarningHandler;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LoadDataWriter<T> extends Writer {
    private static final Logger logger = LoggerFactory.getLogger(TeradataJDBCUtil.class);
    private static final int BUFFER_SIZE = 524288;
    private List<Column> headerColumns;
    private PreparedStatement preparedStatement;
    private final WarningHandler<T> warningHandler;
    private int currentBatchSize = 0;

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
    public LoadDataWriter(Connection conn, String database, String table, List<Column> columns,
                          FileParams params, Map<String, ByteString> secretKeys, Integer batchSize,
                          WarningHandler<T> warningHandler) throws IOException {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        this.warningHandler = warningHandler;
        logger.info("LoadDataWriter initialized with database: {}, table: {}, batchSize: {}", database, table, batchSize);
    }

    @Override
    public void setHeader(List<String> header) throws SQLException {
        logger.info("Setting header with columns: {}", header);
        headerColumns = new ArrayList<>();
        Map<String, Column> nameToColumn = columns.stream().collect(Collectors.toMap(Column::getName, col -> col));

        for (String name : header) {
            headerColumns.add(nameToColumn.get(name));
        }

        String columnNames = headerColumns.stream()
                .map(Column::getName)
                .map(TeradataJDBCUtil::escapeIdentifier)
                .collect(Collectors.joining(", "));

        String placeholders = headerColumns.stream().map(c -> "?").collect(Collectors.joining(", "));

        String query = String.format("INSERT INTO %s (%s) VALUES (%s)",
                TeradataJDBCUtil.escapeTable(database, table), columnNames, placeholders);

        logger.info("Prepared SQL statement: {}", query);
        preparedStatement = conn.prepareStatement(query);
    }

    @Override
    public void writeRow(List<String> row) throws Exception {
        logger.info("Writing row: {}", row);
        try {
            for (int i = 0; i < row.size(); i++) {
                DataType type = headerColumns.get(i).getType();
                String value = row.get(i);

                if (type == DataType.BOOLEAN) {
                    preparedStatement.setBoolean(i + 1, value.equalsIgnoreCase("true"));
                } else if (type == DataType.NAIVE_DATETIME || type == DataType.UTC_DATETIME) {
                    preparedStatement.setString(i + 1, TeradataJDBCUtil.formatISODateTime(value));
                } else {
                    preparedStatement.setString(i + 1, value);
                }
            }

            preparedStatement.addBatch();
            currentBatchSize++;
            logger.info("Added row to batch. Current batch size: {}", currentBatchSize);

            if (currentBatchSize >= batchSize) {
                logger.info("Batch size limit reached. Committing batch.");
                commit();
            }
        } catch (Exception e) {
            warningHandler.handle("Failed to write row to batch", e);
            logger.error("Failed to write row to batch", e);
            throw e;
        }
    }

    @Override
    public void commit() throws SQLException {
        if (currentBatchSize > 0) {
            logger.info("Committing batch of size: {}", currentBatchSize);
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
            currentBatchSize = 0;
            logger.info("Batch committed successfully.");
        } else {
            logger.info("No rows to commit.");
        }
    }
}