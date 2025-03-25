package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.FileParams;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class UpdateHistoryWriter extends Writer {

    private final List<List<String>> rows = new ArrayList<>();

    public UpdateHistoryWriter(Connection conn, String database, String table, List<Column> columns,
                               FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
    }


    List<Column> headerColumns = new ArrayList<>();
    Integer fivetranStartPos;
    Map<String, Integer> nameToHeaderPos = new HashMap<>();


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

    @Override
    public void writeRow(List<String> row) throws SQLException {
        rows.add(row);
    }

    @Override
    public void commit() throws SQLException {
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

    private void processRow(List<String> row) throws SQLException {
        insertNewRow(row);
        updateOldRow(row);
    }

    private void insertNewRow(List<String> row) throws SQLException {
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

        insertQuery.append(String.format(" FROM %s WHERE _fivetran_active = TRUE ", TeradataJDBCUtil.escapeTable(database, table)));

        for (Column c : columns) {
            if (c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                insertQuery.append(String.format("AND %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
            }
        }

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
        }
    }

    private void updateOldRow(List<String> row) throws SQLException {
        StringBuilder updateQuery = new StringBuilder(String.format(
                "UPDATE %s SET _fivetran_active = FALSE, _fivetran_end = DATE_SUB(?,  INTERVAL 1 MICROSECOND) WHERE _fivetran_active = TRUE AND _fivetran_start < ? ",
                TeradataJDBCUtil.escapeTable(database, table)));

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
        }
    }
}