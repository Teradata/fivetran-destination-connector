package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.FileParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EarliestStartHistoryWriter extends Writer {
    private static final Logger logger = LoggerFactory.getLogger(EarliestStartHistoryWriter.class);

    public EarliestStartHistoryWriter(Connection conn, String database, String table, List<Column> columns, FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        super(conn, database, table, columns, params, secretKeys, batchSize);
    }

    List<Column> headerColumns = new ArrayList<>();
    Integer earliestFivetranStartPos;

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

    public void writeDelete(List<String> row) throws Exception {
        logger.info("#########################EarliestStartHistoryWriter.writeDelete#############################################################");
        StringBuilder deleteQuery = new StringBuilder(String.format("DELETE FROM %s WHERE ", TeradataJDBCUtil.escapeTable(database, table)));

        boolean firstPKColumn = true;
        for (int i = 0; i < row.size(); i++) {
            logger.info("Rows is " + row.get(i));
        }
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
        logger.info(String.format("deleteQuery SQL:\n %s", deleteQuery.toString()));
        int paramIndex = 0;
        try (PreparedStatement stmt = conn.prepareStatement(deleteQuery.toString())) {
            for (int i = 0; i < row.size(); i++) {
                String value = row.get(i);
                Column c = headerColumns.get(i);
                if (c == null || !c.getPrimaryKey() || c.getName().equals("_fivetran_start")) {
                    continue;
                }

                paramIndex++;
                logger.info("paramIndex is " + paramIndex);
                logger.info("value is " + value);
                TeradataJDBCUtil.setParameter(stmt, paramIndex, c.getType(), value, params.getNullString());
            }

            paramIndex++;
            TeradataJDBCUtil.setParameter(stmt, paramIndex, DataType.UTC_DATETIME, row.get(earliestFivetranStartPos), params.getNullString());

            stmt.execute();
        }
    }

    public void writeUpdate(List<String> row) throws Exception {
        logger.info("#########################EarliestStartHistoryWriter.writeUpdate#############################################################");
        StringBuilder updateQuery = new StringBuilder(String.format(
                "UPDATE %s SET _fivetran_active = 0, _fivetran_end = _fivetran_start - INTERVAL '1' SECOND WHERE _fivetran_active = 1 ",
                TeradataJDBCUtil.escapeTable(database, table)));
        for (int i = 0; i < row.size(); i++) {
            logger.info("Rows is " + row.get(i));
        }
        for (int i = 0; i < row.size(); i++) {
            Column c = headerColumns.get(i);
            if (c != null && c.getPrimaryKey() && !c.getName().equals("_fivetran_start")) {
                updateQuery.append(
                        String.format("AND %s = ? ", TeradataJDBCUtil.escapeIdentifier(c.getName())));
            }
        }
        logger.info(String.format("updateQuery SQL:\n %s", updateQuery.toString()));
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
                logger.info("paramIndex is " + paramIndex);
                logger.info("value is " + value);
                TeradataJDBCUtil.setParameter(stmt, paramIndex, c.getType(), value, params.getNullString());
            }
            stmt.execute();
        }
    }

    @Override
    public void writeRow(List<String> row) throws Exception {
        logger.info("#########################EarliestStartHistoryWriter.writeRow#############################################################");
        writeDelete(row);
        writeUpdate(row);
    }

    @Override
    public void commit() throws InterruptedException, IOException, SQLException {

    }
}