package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.Logger;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import com.teradata.fivetran.destination.warning_util.WarningHandler;
import fivetran_sdk.v2.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;

public class LoadDataWriter<T> extends Writer {
    private Connection conn;
    private PreparedStatement preparedStatement;
    private String database;
    private String table;
    private String temp_table;
    private List<Column> headerColumns;
    private Map<String, ColumnMetadata> varcharColumnLengths;
    private final WarningHandler<T> warningHandler;
    private int currentBatchSize = 0;
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
    public LoadDataWriter(Connection conn, String database, String table, List<Column> columns,
                          FileParams params, Map<String, ByteString> secretKeys, Integer batchSize,
                          WarningHandler<T> warningHandler) throws IOException {
        super(conn, database, table, columns, params, secretKeys, batchSize);
        this.conn = conn;
        this.database = database;
        this.table = table;
        this.warningHandler = warningHandler;
        this.columns = columns;
        this.batchSize=batchSize;
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("LoadDataWriter initialized with database: %s, table: %s, batchSize: %s", database, table, batchSize));
    }

    @Override
    public void setHeader(List<String> header) throws SQLException {
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("Setting header with columns: %s", header));
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

        varcharColumnLengths = TeradataJDBCUtil.getVarcharColumnLengths(conn, database, table);

        temp_table = String.format("%s_%s", "td_tmp", UUID.randomUUID().toString().replace("-", "_"));

        String createTempTable = String.format("CREATE TABLE %s AS (SELECT * FROM %s) WITH NO DATA;",
                TeradataJDBCUtil.escapeTable(database, temp_table), TeradataJDBCUtil.escapeTable(database, table));
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("Creating temporary table: %s", createTempTable));
        try {
            dropTempTable();
            conn.createStatement().execute(createTempTable);
        } catch (SQLException e) {
            Logger.logMessage(Logger.LogLevel.SEVERE,
                    String.format("Failed to create temporary table: %s", e.getMessage()));
            throw new SQLException("Failed to create temporary table: " + e.getMessage() + " , with SQL: " +
                    createTempTable , e);

        }

        String query = String.format("INSERT INTO %s (%s) VALUES (%s)",
                TeradataJDBCUtil.escapeTable(database, temp_table), columnNames, placeholders);
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("Prepared SQL statement: %s", query));
        preparedStatement = conn.prepareStatement(query);
    }

    private int getSqlTypeFromDataType(DataType type) {
        switch (type) {
            case BOOLEAN:
            case SHORT:
            case INT:
                return java.sql.Types.INTEGER;
            case LONG:
                return java.sql.Types.BIGINT;
            case DECIMAL:
                return java.sql.Types.DECIMAL;
            case FLOAT:
                return java.sql.Types.FLOAT;
            case DOUBLE:
                return java.sql.Types.DOUBLE;
            case NAIVE_TIME:
                return java.sql.Types.TIME;
            case NAIVE_DATE:
                return java.sql.Types.DATE;
            case NAIVE_DATETIME:
            case UTC_DATETIME:
                return java.sql.Types.TIMESTAMP;
            case BINARY:
                return java.sql.Types.BINARY;
            case XML:
                return java.sql.Types.SQLXML;
            default:
                return java.sql.Types.VARCHAR;
        }
    }

    @Override
    public void writeRow(List<String> row) throws Exception {
        Logger.logMessage(Logger.debugLogLevel, "#########################LoadDataWriter.writeRow#########################");
        try {
            for (int i = 0; i < row.size(); i++) {
                DataType type = headerColumns.get(i).getType();
                String value = row.get(i);
                if (value == null || value.equals("null") || value.equals(params.getNullString())) {
                    preparedStatement.setNull(i + 1, getSqlTypeFromDataType(type));
                    Logger.logMessage(Logger.debugLogLevel, String.format("Set parameter at index %d to NULL", i + 1));
                    continue;
                }
                switch (type) {
                    case BOOLEAN:
                        if (value.equalsIgnoreCase("true")) {
                            preparedStatement.setInt(i + 1, 1);
                        } else if (value.equalsIgnoreCase("false")) {
                            preparedStatement.setInt(i + 1, 0);
                        }
                        break;
                    case SHORT:
                    case INT:
                        preparedStatement.setInt(i + 1, Integer.parseInt(value));
                        break;
                    case LONG:
                        preparedStatement.setLong(i + 1, Long.parseLong(value));
                        break;
                    case DECIMAL:
                        preparedStatement.setBigDecimal(i + 1, new BigDecimal(value));
                        break;
                    case FLOAT:
                        preparedStatement.setFloat(i + 1, Float.parseFloat(value));
                        break;
                    case DOUBLE:
                        preparedStatement.setDouble(i + 1, Double.parseDouble(value));
                        break;
                    case NAIVE_TIME:
                        preparedStatement.setTime(i + 1, Time.valueOf(value));
                        break;
                    case NAIVE_DATE:
                        preparedStatement.setDate(i + 1, Date.valueOf(value));
                        break;
                    case NAIVE_DATETIME:
                    case UTC_DATETIME:
                        preparedStatement.setTimestamp(i + 1, TeradataJDBCUtil.getTimestampFromObject(TeradataJDBCUtil.formatISODateTime(value)));
                        break;
                    case BINARY:
                        preparedStatement.setBytes(i + 1, Base64.getDecoder().decode(value));
                        break;
                    case XML:
                        SQLXML sqlxml = preparedStatement.getConnection().createSQLXML();
                        sqlxml.setString(value);
                        preparedStatement.setSQLXML(i + 1, sqlxml);
                        break;
                    case STRING:
                        int valueLength = value.length();
                        String columnName = headerColumns.get(i).getName();
                        ColumnMetadata meta = varcharColumnLengths.get(columnName);
                        int maxAllowed = meta.getMaxAllowedLength();
                        int currentLen = meta.getLength();
                        int safeLength = Math.min(valueLength, maxAllowed);

                        if (safeLength > currentLen && currentLen < maxAllowed) {
                            TeradataJDBCUtil.resizeVarcharColumn(conn, database, table, temp_table, columnName, currentLen, safeLength);
                            varcharColumnLengths.put(columnName, new ColumnMetadata(safeLength, meta.isUnicode() ? 2 : 1));
                        }
                        preparedStatement.setString(i + 1, value);
                        break;
                    case JSON:
                        preparedStatement.setObject(i + 1, new JSONStruct("JSON", new Object[]{value}));
                        break;
                    default:
                        preparedStatement.setObject(i + 1, value);
                        break;
                }
                Logger.logMessage(Logger.debugLogLevel, String.format("Set parameter at index %d: %s", i + 1, value));
            }

            preparedStatement.addBatch();
            currentBatchSize++;
            Logger.logMessage(Logger.debugLogLevel, String.format("Added row to batch. Current batch size: %d", currentBatchSize));

            if (currentBatchSize >= batchSize) {
                Logger.logMessage(Logger.debugLogLevel, String.format("Batch size limit reached. Committing batch of size: %d", currentBatchSize));
                commit();
            }
        } catch (BatchUpdateException bue) {
            Logger.logMessage(Logger.LogLevel.SEVERE, "Failed to write row to batch with BatchUpdateException: " + bue.getMessage());
            dropTempTable();
            throw bue;
        } catch (Exception e) {
            Logger.logMessage(Logger.LogLevel.SEVERE, "Failed to write row to batch with Exception: " +e.getMessage());
            dropTempTable();
            throw e;
        }
    }

    @Override
    public void commit() throws SQLException {
        if (currentBatchSize > 0) {
            Logger.logMessage(Logger.LogLevel.INFO, "Committing batch of size: " + currentBatchSize);
            try {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
            }
            catch (BatchUpdateException bue) {
                String actualError = "";
                if (bue.getNextException() != null) {
                    Exception nextException = bue.getNextException();
                    actualError = nextException.getMessage();
                } else {
                    actualError = bue.getMessage();
                }
                Logger.logMessage(Logger.LogLevel.SEVERE,
                        String.format("WriteBatch failed with exception %s", actualError));
                dropTempTable();
                throw bue;
            }
            catch (SQLException e) {
                Logger.logMessage(Logger.LogLevel.SEVERE,
                        "Failed to execute batch on table : "
                                + TeradataJDBCUtil.escapeTable(database, temp_table) + " with error: "
                                + e.getMessage());
                dropTempTable();
                throw e;
            }
            Logger.logMessage(Logger.LogLevel.INFO,
                    String.format("Batch of size %d committed successfully", currentBatchSize));
            currentBatchSize = 0;
        } else {
            Logger.logMessage(Logger.LogLevel.INFO,
                    String.format("No rows to commit. Current batch size: %d", currentBatchSize));
        }
    }

    public void deleteInsert() throws SQLException {
        if (matchingCols != null && !matchingCols.isEmpty()) {
            String cols = matchingCols.stream()
                    .map(Column::getName)
                    .map(TeradataJDBCUtil::escapeIdentifier)
                    .collect(Collectors.joining(", "));
            if (!cols.isEmpty()) {
                String condition = matchingCols.stream()
                        .map(Column::getName)
                        .map(col -> String.format("t.%s = tmp.%s",
                                        TeradataJDBCUtil.escapeIdentifier(col),
                                        TeradataJDBCUtil.escapeIdentifier(col)))
                        .collect(Collectors.joining(" AND "));

                String deleteQuery = String.format(
                        "DELETE FROM %s AS t WHERE EXISTS (SELECT 1 FROM %s AS tmp WHERE %s)",
                        TeradataJDBCUtil.escapeTable(database, table),
                        TeradataJDBCUtil.escapeTable(database, temp_table),
                        condition
                );
                Logger.logMessage(Logger.LogLevel.INFO, "Prepared SQL delete statement: " + deleteQuery);
                try {
                    conn.createStatement().execute(deleteQuery);
                    Logger.logMessage(Logger.LogLevel.INFO, "Delete operation completed successfully.");
                } catch (SQLException e) {
                    Logger.logMessage(Logger.LogLevel.SEVERE,
                            "Failed to execute (" + deleteQuery + ") on table: "
                                    + TeradataJDBCUtil.escapeTable(database, table) + " with error: "
                                    + e.getMessage());
                    dropTempTable();
                    throw new SQLException("Failed to execute (" + deleteQuery + ") on table: "
                            + TeradataJDBCUtil.escapeTable(database, table) + " with error: "
                            + e.getMessage(), e);
                }
            }
        }

        String insertQuery = String.format("INSERT INTO %s SELECT * FROM %s",
                TeradataJDBCUtil.escapeTable(database, table), TeradataJDBCUtil.escapeTable(database, temp_table));
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("Prepared SQL insert statement: %s", insertQuery));
        try {
            conn.createStatement().execute(insertQuery);
            Logger.logMessage(Logger.LogLevel.INFO,
                    String.format("Insert operation completed successfully."));
        } catch (SQLException e) {
            Logger.logMessage(Logger.LogLevel.SEVERE,
                    "Failed to execute (" + insertQuery + ") on table: "
                            + TeradataJDBCUtil.escapeTable(database, table) + " with error: "
                            + e.getMessage());
            dropTempTable();
            throw new SQLException("Failed to execute (" + insertQuery + ") on table: "
                    + TeradataJDBCUtil.escapeTable(database, table) + " with error: "
                    + e.getMessage(), e);
        }
        dropTempTable();
    }

    public void dropTempTable() {
        try {
            if (conn == null || conn.isClosed()) {
                Logger.logMessage(Logger.debugLogLevel,"Connection is closed. Cannot drop temporary table.");
                return;
            }
            if(database == null || temp_table == null) {
                Logger.logMessage(Logger.debugLogLevel,"Database or temporary table name is null. Cannot drop temporary table.");
                return;
            }
            String deleteQuery = String.format("DELETE FROM %s", TeradataJDBCUtil.escapeTable(database, temp_table));
            String dropQuery = String.format("DROP TABLE %s", TeradataJDBCUtil.escapeTable(database, temp_table));
            Logger.logMessage(Logger.debugLogLevel,"Prepared SQL delete statement: " + deleteQuery);
            Logger.logMessage(Logger.debugLogLevel,"Prepared SQL drop statement: " + dropQuery);

            conn.createStatement().execute(deleteQuery);
            Logger.logMessage(Logger.debugLogLevel,"Temporary table deleted successfully.");
            conn.createStatement().execute(dropQuery);
            Logger.logMessage(Logger.debugLogLevel,"Temporary table dropped successfully.");
        } catch (SQLException e) {
            if (e.getErrorCode() != 3807) {
                Logger.logMessage(Logger.LogLevel.SEVERE,"Failed to delete or drop temporary table: " + e.getMessage());
            }
        }
    }
}