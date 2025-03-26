package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.FileParams;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WriterHistory {
    protected Connection conn;
    protected String database;
    protected String table;
    protected List<Column> columns;
    protected FileParams params;

    public WriterHistory(Connection conn, String database, String table, List<Column> columnsList, FileParams fileParams, Map<String, ByteString> keysMap, Integer integer) {
        this.conn = conn;
        this.database = database;
        this.table = table;
    }

    public void processEarliestStartFile(String file) {
        try (FileInputStream fis = new FileInputStream(file);
             InputStreamReader isr = new InputStreamReader(fis);
             BufferedReader reader = new BufferedReader(isr)) {

            String headerLine = reader.readLine(); // Read header
            if (headerLine == null) {
                return; // Empty file
            }

            List<String> headers = Arrays.asList(headerLine.split(","));
            List<Integer> pkIndices = new ArrayList<>();
            int startIndex = headers.indexOf("_fivetran_start");

            // Identify primary key indices
            for (Column column : columns) {
                if (column.getPrimaryKey()) {
                    int index = headers.indexOf(column.getName());
                    if (index != -1) {
                        pkIndices.add(index);
                    }
                }
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                String earliestStart = values[startIndex];

                // Construct WHERE clause for primary keys
                StringBuilder whereClause = new StringBuilder();
                for (int i = 0; i < pkIndices.size(); i++) {
                    if (i > 0) {
                        whereClause.append(" AND ");
                    }
                    whereClause.append(TeradataJDBCUtil.escapeIdentifier(headers.get(pkIndices.get(i))))
                            .append(" = '").append(values[pkIndices.get(i)]).append("'");
                }

                try (Statement stmt = conn.createStatement()) {
                    // Step 1: Remove overlapping records
                    String deleteQuery = String.format(
                            "DELETE FROM %s WHERE %s AND _fivetran_start >= TIMESTAMP '%s'",
                            TeradataJDBCUtil.escapeTable(database, table), whereClause, earliestStart);
                    stmt.execute(deleteQuery);

                    // Step 2: Update existing active records
                    String updateQuery = String.format(
                            "UPDATE %s SET _fivetran_active = 0, _fivetran_end = TIMESTAMP '%s' - INTERVAL '1' MILLISECOND " +
                                    "WHERE _fivetran_active = TRUE AND %s",
                            TeradataJDBCUtil.escapeTable(database, table), earliestStart, whereClause);
                    stmt.execute(updateQuery);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
