package com.teradata.fivetran.destination.writers;

import java.io.*;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.google.protobuf.ByteString;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.teradata.fivetran.destination.TeradataConfiguration;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import com.teradata.fivetran.destination.writers.util.TeradataColumnDesc;
import com.teradata.fivetran.destination.writers.util.ConnectorSchemaParser;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.Compression;
import fivetran_sdk.v2.Encryption;
import fivetran_sdk.v2.FileParams;
import com.teradata.fivetran.destination.Logger;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * FastLoadDataWriter - Handles high-performance data loading using Teradata FastLoad
 * This class manages parallel data loading from multiple files with support for
 * encryption, compression, and error handling through FastLoad sessions.
 */
public class FastLoadDataWriter {

    // ========== CONSTANT DECLARATIONS ==========

    /** SQL function to retrieve Logon Sequence Number for FastLoad sessions */
    protected static String SQL_GET_LSN = "{fn teradata_logon_sequence_number()}";

    /** SQL template for USING INSERT statement in FastLoad operations */
    protected static final String SQL_USING_INSERT_INTO_TABLE = "USING %s INSERT INTO %s ( %s ) VALUES ( %s )";

    /** SQL template for SELECT statements */
    protected static final String SQL_SELECT_FROM_SOURCE_WHERE = "SELECT %s FROM %s %s";

    /** JDBC driver class name for Teradata connections */
    protected static String jdbcDriver = "com.teradata.jdbc.TeraDriver";

    // Character constants for SQL quoting and escaping
    private static final char ESCAPE_CHAR = '\\';
    private static final char DOT_CHAR = '.';
    private static final char SINGLE_QUOTE = '\'';
    private static final char DOUBLE_QUOTE = '\"';
    private static final String REPLACE_DOUBLE_QUOTE = "\\\"";
    private static final String REPLACE_DOUBLE_QUOTE_SQL = "\"\"";

    // ========== INSTANCE VARIABLE DECLARATIONS ==========

    // Database connections
    private Connection conn;                    // Main database connection for DDL operations
    private Connection lsnConnection = null;    // LSN control session connection
    private Statement stmt = null;              // Statement object for SQL execution

    // Schema and table information
    private String database;                    // Target database name
    private String table;                       // Target table name
    private List<Column> columns;               // Column definitions from schema
    private List<Column> matchingCols;          // Primary key columns for upsert operations
    private FileParams params;                  // File parameters (compression, encryption)
    private Map<String, ByteString> secretKeys; // Encryption keys for file decryption
    private Integer batchSize;                  // Batch size for loading operations

    // Header and column information
    private List<Column> headerColumns;         // Columns extracted from CSV header
    private String columnNames;                 // Comma-separated column names for SQL

    // Connection configuration
    private String dbsHost;                     // Teradata host address
    private String username;                    // Database username
    protected String password;                  // Database password

    //Requested FastLoad sessions
    private int requestedSessions;

    // Temporary table names
    private String outputTableName;             // Temporary output table for FastLoad
    private String errorTable1;                 // First error table for FastLoad
    private String errorTable2;                 // Second error table for FastLoad

    // ========== CONSTRUCTOR ==========

    /**
     * Constructs a FastLoadDataWriter with the specified configuration and parameters.
     *
     * @param conf      Teradata configuration containing connection details
     * @param conn      Database connection for table operations
     * @param database  Target database name
     * @param table     Target table name
     * @param columns   List of column definitions from schema
     * @param params    File parameters for compression and encryption
     * @param secretKeys Map of file names to encryption keys for AES decryption
     * @param batchSize Batch size for loading operations
     */
    public FastLoadDataWriter(TeradataConfiguration conf, Connection conn, String database, String table, List<Column> columns,
                              FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {

        this.conn = conn;
        this.database = database;
        this.table = table;
        this.columns = columns;
        this.params = params;
        this.secretKeys = secretKeys;
        this.batchSize = batchSize;

        this.dbsHost = conf.host();
        this.username = conf.user();
        this.password = conf.password();
    }

    // ========== PUBLIC METHODS ==========

    /**
     * Main method to write data from multiple source files using Teradata FastLoad.
     * This method coordinates the entire FastLoad process including temporary table creation,
     * parallel file loading, and cleanup.
     *
     * @param sourceFilesList List of source file paths to be loaded
     * @throws Exception If any error occurs during the loading process
     */
    public void writeData(List<String> sourceFilesList) throws Exception {

        // Log the number of source files and requested FastLoad sessions
        Logger.logMessage(Logger.LogLevel.INFO, "Number of source files to load: " + sourceFilesList.size());
        Logger.logMessage(Logger.LogLevel.INFO, "Number of requested FastLoad sessions: " + sourceFilesList.size());
        this.requestedSessions = sourceFilesList.size();

        // Read header from first file to validate file and setup column mapping
        List<String> header = getHeader(sourceFilesList.get(0), params, secretKeys);
        if (header == null) {
            Logger.logMessage(Logger.LogLevel.SEVERE, "Source file is empty. Exiting FastLoadDataWriter.");
            return;
        }

        // Setup column mappings and identify primary key columns
        matchingCols = columns.stream()
                .filter(Column::getPrimaryKey)
                .collect(Collectors.toList());
        Map<String, Column> nameToColumn = columns.stream().collect(Collectors.toMap(Column::getName, col -> col));

        Logger.logMessage(Logger.LogLevel.INFO, "Header: " + header);
        headerColumns = new ArrayList<>();
        for (String name : header) {
            headerColumns.add(nameToColumn.get(name));
        }

        columnNames = headerColumns.stream()
                .map(Column::getName)
                .map(TeradataJDBCUtil::escapeIdentifier)
                .collect(Collectors.joining(", "));

        // Create temporary tables for FastLoad operation
        outputTableName = String.format("%s_%s", "td_tmp", UUID.randomUUID().toString().replace("-", "_"));
        errorTable1 = outputTableName + "_ERR1";
        errorTable2 = outputTableName + "_ERR2";
        Logger.logMessage(Logger.LogLevel.INFO, "Output Table Name: " + outputTableName);
        Logger.logMessage(Logger.LogLevel.INFO, "Error Table 1: " + errorTable1);
        Logger.logMessage(Logger.LogLevel.INFO, "Error Table 2: " + errorTable2);

        String columnDefinitions = TeradataJDBCUtil.getColumnDefinitions(headerColumns);
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("Column definitions for temporary table: %s", columnDefinitions));

        String createTempTableSQL = String.format("CREATE MULTISET TABLE %s (%s)",
                TeradataJDBCUtil.escapeTable(database, outputTableName), columnDefinitions);

        try {
            dropTempTable();
            dropErrorTables();
            Logger.logMessage(Logger.LogLevel.INFO,
                    String.format("Creating temporary table: %s", createTempTableSQL));
            conn.createStatement().execute(createTempTableSQL);
            Logger.logMessage(Logger.LogLevel.INFO,
                    String.format("Temporary table %s created successfully.", outputTableName));
        } catch (SQLException e) {
            Logger.logMessage(Logger.LogLevel.SEVERE,
                    String.format("Failed to create temporary table: %s", e.getMessage()));
            throw new SQLException("Failed to create temporary table: " + e.getMessage() + " , with SQL: " +
                    createTempTableSQL , e);
        }

        String beginLoading = String.format("BEGIN LOADING %s ERRORFILES %s, %s WITH INTERVAL", outputTableName, errorTable1, errorTable2);
        String endLoading = "END LOADING";

        String lsnUrl = "jdbc:teradata://" + dbsHost
                + "/LSS_TYPE=L,TMODE=TERA,CONNECT_FUNCTION=1,TSNANO=6,TNANO=0"; // Control Session
        try {
            Class.forName(jdbcDriver);
            lsnConnection = DriverManager.getConnection(lsnUrl, username, password);

            Map<String, Integer> decimalScales = new HashMap<>();

            DatabaseMetaData meta = lsnConnection.getMetaData();

            ResultSet rs = meta.getColumns(null,
                    database.toUpperCase(),  // IMPORTANT
                    table.toUpperCase(),   // IMPORTANT
                    null);
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                int dataType = rs.getInt("DATA_TYPE");
                int scale = rs.getInt("DECIMAL_DIGITS");

                if (dataType == Types.DECIMAL || dataType == Types.NUMERIC) {
                    decimalScales.put(colName.toLowerCase(), scale);
                }
                Logger.logMessage(Logger.LogLevel.INFO, "Col=" + colName +
                        ", Type=" + dataType +
                        ", Scale=" + scale);
            }
            rs.close();

            String lsnNumber = lsnConnection.nativeSQL(SQL_GET_LSN);
            Logger.logMessage(Logger.LogLevel.INFO,"FastLoad LSN: " + lsnNumber);
            String fastLoadURL = "jdbc:teradata://" + dbsHost
                    + "/LSS_TYPE=L,PARTITION=FASTLOAD,CONNECT_FUNCTION=2,TSNANO=6,TNANO=0,LOGON_SEQUENCE_NUMBER="
                    + lsnNumber; // FastLoad Session

            // Step 1: Get TASM FastLoad session limit from configuration
            int tasmLimit = getTASMFastLoadLimit();

            // Step 2: Check system-level session limits
            int systemLimit = getSystemSessionLimit();

            // Step 3: Check workload governance (for comparison)
            int checkWorkloadLimit = getGovernedSessionCount();

            // Step 4: Use the most restrictive limit
            int numSessions = Math.min(Math.min(systemLimit, tasmLimit), checkWorkloadLimit);

            Logger.logMessage(Logger.LogLevel.INFO,"=== Session Count Summary ===");
            Logger.logMessage(Logger.LogLevel.INFO,"Requested:              " + requestedSessions);
            Logger.logMessage(Logger.LogLevel.INFO,"System Limit:           " + systemLimit);
            Logger.logMessage(Logger.LogLevel.INFO,"TASM Config Limit:      " + tasmLimit);
            Logger.logMessage(Logger.LogLevel.INFO,"CHECK WORKLOAD Limit:   " + checkWorkloadLimit);
            Logger.logMessage(Logger.LogLevel.INFO,"Final (Most Restrictive): " + numSessions);
            Logger.logMessage(Logger.LogLevel.INFO,"=============================");

            // Creating FastLoad Connections
            FastLoad fastLoad[] = new FastLoad[numSessions];
            for (int i = 0; i < numSessions; i++) {
                fastLoad[i] = new FastLoad();
            }
            Logger.logMessage(Logger.LogLevel.INFO,"fastLoadURL: " + fastLoadURL);

            for (int i = 0; i < numSessions; i++) {
                Logger.logMessage(Logger.LogLevel.INFO,"calling createFastLoadConnection() for instance : " + i + 1);
                fastLoad[i].createFastLoadConnection(i + 1, fastLoadURL, username, password, batchSize, decimalScales);
            }

            // Submitting beginLoading
            stmt = lsnConnection.createStatement();
            stmt.executeUpdate("SET SESSION DateForm = IntegerDate");
            lsnConnection.setAutoCommit(true);
            stmt.execute(beginLoading);

            String usingInsertSQL = getusingInsertSQL(lsnConnection, database, outputTableName, header);
            Logger.logMessage(Logger.LogLevel.INFO,"usingInsertSQL: " + usingInsertSQL);
            // submitting usingInsertSQL
            lsnConnection.setAutoCommit(false);
            stmt.executeUpdate(usingInsertSQL);

            // Distribute files to sessions
            List<List<String>> fileBatches = new ArrayList<>();
            for (int i = 0; i < numSessions; i++) {
                fileBatches.add(new ArrayList<>());
            }
            for (int i = 0; i < sourceFilesList.size(); i++) {
                fileBatches.get(i % numSessions).add(sourceFilesList.get(i));
            }


            FastLoadThread fastLoadThread[] = new FastLoadThread[numSessions];
            for (int i = 0; i < numSessions; i++) {
                fastLoadThread[i] = new FastLoadThread(fastLoad[i], fileBatches.get(i), columns, params, secretKeys);
                Logger.logMessage(Logger.LogLevel.INFO,"Starting Thread: " + i);
                fastLoadThread[i].start();
            }
            int count = 0;
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                }
                for (int i = 0; i < numSessions; i++) {
                    if (fastLoad[i].getLoadCompleted()) {
                        count++;
                    }
                }
                if (count == numSessions) {
                    break;
                }
                count = 0;
            }

            stmt.executeUpdate("CHECKPOINT LOADING END");
            lsnConnection.commit();

            // submitting endLoading
            stmt.executeUpdate(endLoading);
            lsnConnection.commit();

            lsnConnection.setAutoCommit(true);
            stmt.close();

            for (int i = 0; i < numSessions; i++) {
                fastLoad[i].closeFastLoadConnection();
            }

            lsnConnection.close();
        } catch (SQLException ex) {
            try {
                stmt.executeUpdate(endLoading);
                lsnConnection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int getTASMFastLoadLimit() {
        int tasmLimit = requestedSessions;

        try {
            Statement stmt = lsnConnection.createStatement();

             Logger.logMessage(Logger.LogLevel.INFO,"\n=== TASM Ruleset Configuration ===");

            // Query TASM ruleset for FastLoad session limit
            String query = "SELECT rulename, configName, UtilSessions " +
                    "FROM TDWM.Configurations TC, TDWM.RuleDefs TD " +
                    "WHERE TC.configID = TD.configID " +
                    "AND TD.RuleName LIKE '%fastloadlimit%' " +
                    "AND utilsessions > 0";

             Logger.logMessage(Logger.LogLevel.INFO,"Query: " + query);

            ResultSet rs = stmt.executeQuery(query);

            boolean found = false;
            while (rs.next()) {
                found = true;
                String ruleName = rs.getString(1);
                String rulesetName = rs.getString(2);
                Integer flLimit = (Integer) rs.getObject(3);

                 Logger.logMessage(Logger.LogLevel.INFO,"\nRuleset Name: " + rulesetName);
                 Logger.logMessage(Logger.LogLevel.INFO,"Rule Name: " + ruleName);
                 Logger.logMessage(Logger.LogLevel.INFO,"FastLoad Session Limit: " +
                        (flLimit != null ? flLimit : "Not Set"));

                if (flLimit != null && flLimit > 0 && flLimit < tasmLimit) {
                    tasmLimit = flLimit;
                     Logger.logMessage(Logger.LogLevel.INFO,"*** Using TASM configured limit: " + tasmLimit + " ***");
                }
            }

            if (!found) {
                 Logger.logMessage(Logger.LogLevel.INFO,"No TASM rulesets with FastLoad limits found");
            }

             Logger.logMessage(Logger.LogLevel.INFO,"==================================\n");

            rs.close();
            stmt.close();

        } catch (SQLException e) {
             Logger.logMessage(Logger.LogLevel.INFO,"Could not query TASM ruleset info: " + e.getMessage());
        }

        return tasmLimit;
    }

    /**
     * Query system-level FastLoad session limits from DBS Control
     */
    private int getSystemSessionLimit() throws SQLException {
        int systemLimit = requestedSessions;

        try {
            Statement stmt = lsnConnection.createStatement();

            // Query DBS Control for FastLoad session limits
            String query = "SELECT InfoData FROM DBC.DBCInfoV WHERE InfoKey = 'MAXIMUM LOAD TASKS'";

             Logger.logMessage(Logger.LogLevel.INFO,"\nQuerying system-level FastLoad limits...");
             Logger.logMessage(Logger.LogLevel.INFO,"Query: " + query);

            ResultSet rs = stmt.executeQuery(query);

            if (rs.next()) {
                String maxLoadTasks = rs.getString(1);
                if (maxLoadTasks != null && !maxLoadTasks.trim().isEmpty()) {
                    try {
                        int maxTasks = Integer.parseInt(maxLoadTasks.trim());
                         Logger.logMessage(Logger.LogLevel.INFO,"System Maximum Load Tasks: " + maxTasks);

                        if (maxTasks > 0) {
                            systemLimit = Math.min(requestedSessions, maxTasks);
                             Logger.logMessage(Logger.LogLevel.INFO,"Adjusted session limit: " + systemLimit);
                        }
                    } catch (NumberFormatException e) {
                         Logger.logMessage(Logger.LogLevel.INFO,"Warning: Could not parse max load tasks: " + maxLoadTasks);
                    }
                }
            } else {
                 Logger.logMessage(Logger.LogLevel.INFO,"No system limit found in DBC.DBCInfoV");
            }

            rs.close();
            stmt.close();

        } catch (SQLException e) {
             Logger.logMessage(Logger.LogLevel.INFO,"Warning: Could not query system limits: " + e.getMessage());
             Logger.logMessage(Logger.LogLevel.INFO,"Will attempt with requested sessions: " + requestedSessions);
        }

        return systemLimit;
    }

    /**
     * Get TASM-governed session count using CHECK WORKLOAD
     */
    private int getGovernedSessionCount() throws SQLException {
        int governedSessions = requestedSessions;

        String CHECK_WORKLOAD = "CHECK WORKLOAD FOR ";
        String CHECK_WORKLOAD_END = "CHECK WORKLOAD END";
        String SQL_BEGIN_LOADING = "BEGIN LOADING";

        try {
            // Check if connection is governed by TASM
            String governedValue = lsnConnection.nativeSQL("{fn teradata_provide(governed)}");
             Logger.logMessage(Logger.LogLevel.INFO,"\nTASM Governed: " + governedValue);
            boolean isGoverned = "true".equals(governedValue);

            // Build check workload statement
            String checkWorkload = CHECK_WORKLOAD + SQL_BEGIN_LOADING + " " + outputTableName
                    + " ERRORFILES " + errorTable1 + ", " + errorTable2;

            String checkWorkloadEnd = (!isGoverned ? "{fn teradata_failfast}" : "") + CHECK_WORKLOAD_END;

             Logger.logMessage(Logger.LogLevel.INFO,"Checking TASM workload rules...");
             Logger.logMessage(Logger.LogLevel.INFO,"Query: " + checkWorkload);

            Statement stmt = lsnConnection.createStatement();
            stmt.executeUpdate(checkWorkload);

            ResultSet rs = stmt.executeQuery(checkWorkloadEnd);

            ResultSetMetaData rsmd = rs.getMetaData();
             Logger.logMessage(Logger.LogLevel.INFO,"Result set columns: " + rsmd.getColumnCount());

            if (rs.next() && rsmd.getColumnCount() >= 2) {
                String tasmFlag = rs.getString(1);
                int suggestedCount = rs.getInt(2);
                String rulesetName = rsmd.getColumnCount() >= 3 ? rs.getString(3) : null;

                 Logger.logMessage(Logger.LogLevel.INFO,"TASM Flag: " + (tasmFlag != null ? tasmFlag.trim() : "NULL"));
                 Logger.logMessage(Logger.LogLevel.INFO,"TASM Suggested Count: " + suggestedCount);
                 Logger.logMessage(Logger.LogLevel.INFO,"Ruleset Name: " + (rulesetName != null ? rulesetName : "Not Available"));

                // Use suggested count if it's valid and less than requested
                // The count is returned regardless of Y/N flag
                if (suggestedCount > 0 && suggestedCount < requestedSessions) {
                    governedSessions = suggestedCount;

                    if (tasmFlag != null && tasmFlag.trim().equalsIgnoreCase("Y")) {
                         Logger.logMessage(Logger.LogLevel.INFO,"*** TASM WORKLOAD CLASSIFICATION ACTIVE ***");
                         Logger.logMessage(Logger.LogLevel.INFO,"Active workload rule is limiting sessions");
                    } else {
                         Logger.logMessage(Logger.LogLevel.INFO,"*** TASM RULESET LIMIT ACTIVE ***");
                         Logger.logMessage(Logger.LogLevel.INFO,"System-level TASM ruleset is limiting sessions");
                    }
                     Logger.logMessage(Logger.LogLevel.INFO,"DBS will use: " + governedSessions + " sessions");
                } else if (suggestedCount > 0) {
                     Logger.logMessage(Logger.LogLevel.INFO,"TASM allows up to " + suggestedCount + " sessions");
                     Logger.logMessage(Logger.LogLevel.INFO,"Using requested: " + requestedSessions + " sessions");
                } else {
                     Logger.logMessage(Logger.LogLevel.INFO,"Warning: Invalid session count returned: " + suggestedCount);
                }
            } else {
                 Logger.logMessage(Logger.LogLevel.INFO,"Warning: Unrecognized result set structure");
            }

            rs.close();
            stmt.close();

        } catch (SQLException e) {
             Logger.logMessage(Logger.LogLevel.INFO,"Warning: Could not check TASM governance: " + e.getMessage());
        }

        return governedSessions;
    }

    /**
     * Performs delete-insert operation (upsert) from temporary table to target table.
     * Deletes existing records based on primary key matches and inserts all records from temporary table.
     *
     * @throws SQLException If any SQL operation fails during delete or insert
     */
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
                        TeradataJDBCUtil.escapeTable(database, outputTableName),
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
                    dropErrorTables();
                    throw new SQLException("Failed to execute (" + deleteQuery + ") on table: "
                            + TeradataJDBCUtil.escapeTable(database, table) + " with error: "
                            + e.getMessage(), e);
                }
            }
        }

        String insertQuery = String.format("INSERT INTO %s (%s) SELECT %s FROM %s",
                TeradataJDBCUtil.escapeTable(database, table),
                columnNames,
                columnNames,
                TeradataJDBCUtil.escapeTable(database, outputTableName));
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
            dropErrorTables();
            throw new SQLException("Failed to execute (" + insertQuery + ") on table: "
                    + TeradataJDBCUtil.escapeTable(database, table) + " with error: "
                    + e.getMessage(), e);
        }
        dropTempTable();
        dropErrorTables();
    }

    /**
     * Drops the temporary output table if it exists.
     * Handles SQLException gracefully for cases where table doesn't exist.
     */
    public void dropTempTable() {
        try {
            if (conn == null || conn.isClosed()) {
                Logger.logMessage(Logger.debugLogLevel,"Connection is closed. Cannot drop temporary table.");
                return;
            }
            if(database == null || outputTableName == null) {
                Logger.logMessage(Logger.debugLogLevel,"Database or temporary table name is null. Cannot drop temporary table.");
                return;
            }
            String deleteQuery = String.format("DELETE FROM %s", TeradataJDBCUtil.escapeTable(database, outputTableName));
            String dropQuery = String.format("DROP TABLE %s", TeradataJDBCUtil.escapeTable(database, outputTableName));
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

    /**
     * Drops the error tables if they exist and are empty.
     * Throws exception if error tables contain rows that need analysis.
     */
    public void dropErrorTables() {
        try {
            if (conn == null || conn.isClosed()) {
                Logger.logMessage(Logger.debugLogLevel, "Connection is closed. Cannot drop error tables.");
                return;
            }

            if (errorTable1 == null && errorTable2 == null) {
                Logger.logMessage(Logger.debugLogLevel, "Database or error table names are null. Cannot drop error tables.");
                return;
            }

            String[] errorTables = {errorTable1, errorTable2};

            for (String errorTable : errorTables) {
                if (errorTable == null || errorTable.isEmpty()) {
                    continue; // skip null/empty
                }

                String escapedTable = TeradataJDBCUtil.escapeTable(database, errorTable);

                try (Statement stmt = conn.createStatement()) {
                    // Check if error table exists
                    String checkExistQuery = String.format(
                            "SELECT COUNT(*) FROM DBC.TablesV WHERE DatabaseName = '%s' AND TableName = '%s';",
                            database, errorTable
                    );
                    Logger.logMessage(Logger.debugLogLevel, "Checking existence of error table: " + errorTable);

                    ResultSet rs = stmt.executeQuery(checkExistQuery);
                    rs.next();
                    int exists = rs.getInt(1);

                    if (exists == 0) {
                        Logger.logMessage(Logger.debugLogLevel, "Error table " + errorTable + " does not exist. Skipping.");
                        continue;
                    }

                    // Count rows in error table
                    String countQuery = "SELECT COUNT(*) FROM " + escapedTable + ";";
                    Logger.logMessage(Logger.debugLogLevel, "Checking row count for: " + errorTable);

                    rs = stmt.executeQuery(countQuery);
                    rs.next();
                    int rowCount = rs.getInt(1);

                    if (rowCount == 0) {
                        // Drop empty error table
                        String deleteQuery = String.format("DELETE FROM %s;", escapedTable);
                        String dropQuery = String.format("DROP TABLE %s;", escapedTable);

                        Logger.logMessage(Logger.debugLogLevel, "Deleting and dropping empty error table: " + errorTable);
                        stmt.execute(deleteQuery);
                        stmt.execute(dropQuery);
                        Logger.logMessage(Logger.debugLogLevel, "Error table " + errorTable + " dropped successfully.");
                    } else {
                        // Throw exception if table has rows
                        String message = String.format(
                                "Error table %s contains %d error rows. Aborting cleanup for analysis.",
                                errorTable, rowCount
                        );
                        Logger.logMessage(Logger.LogLevel.SEVERE, message);
                        throw new SQLException(message);
                    }

                } catch (SQLException e) {
                    if (e.getErrorCode() != 3807) { // 3807 = table not found
                        throw new SQLException("Failed to drop error table " + errorTable + ": " + e.getMessage(), e);
                    } else {
                        Logger.logMessage(Logger.debugLogLevel, "Error table " + errorTable + " not found (Error 3807).");
                    }
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error table cleanup failed: " + e.getMessage(), e);
        }
    }

    // ========== STATIC UTILITY METHODS ==========

    /**
     * Reads the header row from a CSV file with support for encryption and compression.
     *
     * @param file      Path to the CSV file
     * @param params    File parameters specifying compression and encryption
     * @param secretKeys Map of file names to encryption keys
     * @return List of column names from the header, or null if file is empty
     * @throws Exception If file reading, decryption, or decompression fails
     */
    public static List<String> getHeader(String file, FileParams params, Map<String, ByteString> secretKeys) throws Exception {
        FileInputStream is = new FileInputStream(file);
        InputStream decoded = is ;
        if (params.getEncryption() == Encryption.AES) {
            decoded = decodeAES(is, secretKeys.get(file).toByteArray(), file);
        }

        InputStream uncompressed = decoded;
        if (params.getCompression() == Compression.ZSTD) {
            uncompressed = new ZstdInputStream(decoded);
        } else if (params.getCompression() == Compression.GZIP) {
            uncompressed = new GZIPInputStream(decoded);
        }

        try (CSVReader csvReader = new CSVReaderBuilder(new BufferedReader(new InputStreamReader(uncompressed)))
                .withCSVParser(new CSVParserBuilder().withEscapeChar('\0').build())
                .build()) {
            String[] headerString = csvReader.readNext();
            if (headerString == null) {
                // Finish if file is empty
                return null;
            }
            List<String> header = new ArrayList<>(Arrays.asList(headerString));
            return header;
        }
    }

    /**
     * Generates the USING INSERT SQL statement for FastLoad operations.
     * This method queries the database to get column metadata and constructs the appropriate USING clause.
     *
     * @param con            Database connection
     * @param database       Database name
     * @param outputTableName Temporary table name
     * @param header         List of column names from header
     * @return USING INSERT SQL statement, or null if no valid columns found
     */
    private static String getusingInsertSQL(Connection con, String database, String outputTableName, List<String> header) {
        try {
            Statement stmt = con.createStatement();
            Logger.logMessage(Logger.LogLevel.INFO,"Query: " + "select count(*) from dbc.ColumnsV where tablename='" + database + "." + outputTableName + "';");
            ResultSet res = stmt
                    .executeQuery("select count(*) from dbc.columns where tablename='" + database + "." + outputTableName + "';");
            res.next();
            Logger.logMessage(Logger.LogLevel.INFO,"Total columns in table " + outputTableName + ": " + res.getInt(1));

            // First, get all available columns from the database
            res = null;
            Logger.logMessage(Logger.LogLevel.INFO,"Query: " + "select columnName from dbc.columns where databasename = '"+ database + "' and tablename='" + outputTableName + "';");
            res = stmt.executeQuery("select columnName from dbc.ColumnsV where databasename = '"+ database + "' and tablename='" + outputTableName + "';");

            List<String> availableColumns = new ArrayList<>();
            while (res.next()) {
                Logger.logMessage(Logger.debugLogLevel, "Found column in table: " + res.getString("columnName"));
                availableColumns.add(res.getString("columnName").trim());
            }

            if (availableColumns.isEmpty()) {
                Logger.logMessage(Logger.debugLogLevel, "No columns found in table: " + outputTableName);
                Thread.sleep(10000); // wait for 10 seconds before retrying
                res = stmt.executeQuery("select columnName from dbc.columns where tablename='" + outputTableName + "';");
                while (res.next()) {
                    Logger.logMessage(Logger.LogLevel.INFO, "Found column in table after wait: " + res.getString("columnName"));
                    availableColumns.add(res.getString("columnName").trim());
                }
            }

            Logger.logMessage(Logger.LogLevel.INFO,"Available columns in table " + outputTableName + ": " + availableColumns);

            // Filter and order columns based on header, while ensuring they exist in the table
            List<String> orderedColumns = new ArrayList<>();
            for (String headerCol : header) {
                Logger.logMessage(Logger.debugLogLevel,"Processing header column: " + headerCol);
                String trimmedHeader = headerCol.trim();
                Logger.logMessage(Logger.debugLogLevel,"Trimmed header column: " + trimmedHeader);
                // Check if this column exists in the database
                if (availableColumns.contains(trimmedHeader)) {
                    orderedColumns.add(trimmedHeader);
                } else {
                    Logger.logMessage(Logger.LogLevel.WARNING, "Column '" + trimmedHeader + "' from header not found in table '" + outputTableName + "'");
                }
            }

            // Check if we found any valid columns
            if (orderedColumns.isEmpty()) {
                Logger.logMessage(Logger.LogLevel.SEVERE, "No valid columns found matching header for table: " + outputTableName);
                return null;
            }

            // Convert to quoted column names array
            String[] colNames = new String[orderedColumns.size()];
            for (int i = 0; i < orderedColumns.size(); i++) {
                colNames[i] = "\"" + orderedColumns.get(i) + "\"";
            }

            TeradataColumnDesc[] fieldDescs = getColumnDesc(outputTableName, colNames, con);
            String[] fieldTypes4Using = new String[fieldDescs.length];
            String[] fieldNames = new String[fieldDescs.length];

            int index;

            /*
             * Determine the lowest timestamp precision (scale) to set for all
             * time/timestamp columns in USING statement
             */
            int lowestScaleForTime = TeradataColumnDesc.TIME_SCALE_DEFAULT;
            int lowestScaleForTimeStamp = TeradataColumnDesc.TIME_SCALE_DEFAULT;
            for (index = 0; index < fieldDescs.length; index++) {
                TeradataColumnDesc fieldDesc = fieldDescs[index];
                if (fieldDesc.getType() == Types.TIME) {
                    if (fieldDesc.getScale() < lowestScaleForTime) {
                        lowestScaleForTime = fieldDesc.getScale();
                    }
                } else if (fieldDesc.getType() == Types.TIMESTAMP) {
                    if (fieldDesc.getScale() < lowestScaleForTimeStamp) {
                        lowestScaleForTimeStamp = fieldDesc.getScale();
                    }
                }
            }

            /*
             * Loop through all fields and get type string; if this is a TIME or TIMESTAMP
             * field, use the lowest scale calculated above
             */
            for (index = 0; index < fieldDescs.length; index++) {
                TeradataColumnDesc fieldDesc = fieldDescs[index];
                fieldNames[index] = fieldDesc.getName();
                fieldTypes4Using[index] = fieldDesc.getTypeString4Using("UTF-8", lowestScaleForTime,
                        lowestScaleForTimeStamp);
            }

            Logger.logMessage(Logger.LogLevel.INFO,"Field Names: " + Arrays.toString(fieldNames));
            Logger.logMessage(Logger.LogLevel.INFO,"Field Types: " + Arrays.toString(fieldTypes4Using));
            return getUsingSQL(outputTableName, fieldNames, fieldTypes4Using, "UTF-8");
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * Constructs a USING SQL statement for FastLoad operations.
     *
     * @param tableName    Target table name
     * @param columns      Array of column names
     * @param types4Using  Array of column type definitions for USING clause
     * @param charset      Character set for the data
     * @return Complete USING INSERT SQL statement
     */
    public static String getUsingSQL(String tableName, String[] columns, String[] types4Using, String charset) {

        StringBuilder targetColExpBuilder = new StringBuilder();
        StringBuilder usingColExpBuilder = new StringBuilder();
        StringBuilder usingValExpBuilder = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                targetColExpBuilder.append(", ");
                usingColExpBuilder.append(", ");
                usingValExpBuilder.append(", ");
            }
            String quotedUsingCol = getQuotedName(columns[i]);
            String quotedCol = getQuotedName(columns[i]);
            targetColExpBuilder.append(quotedCol);
            //TDCH-2003 - Start
            if(types4Using[i].contains("LONG VARCHAR")) {
                types4Using[i] = "LONG VARCHAR";
            }
            //TDCH-2003 - End
            usingColExpBuilder.append(quotedUsingCol).append(" (").append(types4Using[i]).append(')');
            usingValExpBuilder.append(':').append(quotedUsingCol);
        }

        return String.format(
                SQL_USING_INSERT_INTO_TABLE,
                usingColExpBuilder.toString(),
                tableName,
                targetColExpBuilder.toString(),
                usingValExpBuilder.toString());
    }

    /**
     * Generates a SELECT SQL statement for the specified table and columns.
     *
     * @param tableName Table name to select from
     * @param columns   Array of column names to select, or null for all columns
     * @return Complete SELECT SQL statement
     */
    public static String getSelectSQL(String tableName, String[] columns) {
        StringBuilder colExpBuilder = new StringBuilder();

        if (columns != null && columns.length != 0) {
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) {
                    colExpBuilder.append(", ");
                }
                colExpBuilder.append(columns[i]);
            }
        } else {
            colExpBuilder.append('*');
        }
        return String.format(SQL_SELECT_FROM_SOURCE_WHERE, colExpBuilder.toString(), tableName, "");
    }

    /**
     * Extracts column descriptions from ResultSet metadata.
     *
     * @param metadata ResultSet metadata containing column information
     * @return Array of TeradataColumnDesc objects describing each column
     * @throws SQLException If metadata access fails
     */
    protected static TeradataColumnDesc[] getColumnDescs(ResultSetMetaData metadata) throws SQLException {
        int columnCount = metadata.getColumnCount();

        TeradataColumnDesc[] columns = new TeradataColumnDesc[columnCount];

        for (int i = 1; i <= columnCount; i++) {

            TeradataColumnDesc column = new TeradataColumnDesc();

            column.setName(metadata.getColumnName(i));
            column.setType(metadata.getColumnType(i));
            column.setTypeName(metadata.getColumnTypeName(i));
            column.setClassName(metadata.getColumnClassName(i));
            column.setNullable(metadata.isNullable(i) > 0);

            /* Only valid for types that have length */
            column.setLength(metadata.getColumnDisplaySize(i));

            /* Only valid for String (non-CLOB types) */
            column.setCaseSensitive(metadata.isCaseSensitive(i));

            /* Only valid for numeric/decimal types */
            column.setPrecision(metadata.getPrecision(i));
            column.setScale(metadata.getScale(i));

            columns[i - 1] = column;
        }
        return columns;
    }

    /**
     * Gets column descriptions for a specific SQL query.
     *
     * @param sql        SQL query to analyze
     * @param connection Database connection
     * @return Array of TeradataColumnDesc objects, or null if connection is null
     * @throws SQLException If SQL preparation or metadata extraction fails
     */
    public static TeradataColumnDesc[] getColumnDescsForSQL(String sql, Connection connection) throws SQLException {
        if (connection != null) {
            PreparedStatement stmt = connection.prepareStatement(sql);
            ResultSetMetaData metadata = stmt.getMetaData();
            TeradataColumnDesc[] desc = getColumnDescs(metadata);
            stmt.close();
            return desc;
        }

        return null;
    }

    /**
     * Gets column descriptions for a specific table and field names.
     *
     * @param tableName   Table name
     * @param fieldNames  Array of field names to get descriptions for
     * @param connection  Database connection
     * @return Array of TeradataColumnDesc objects for the specified fields
     * @throws SQLException If SQL execution or metadata extraction fails
     */
    public static TeradataColumnDesc[] getColumnDesc(String tableName, String[] fieldNames,
                                                     Connection connection) throws SQLException {
        return getColumnDescsForSQL(getSelectSQL(tableName, fieldNames),
                connection);
    }

    // ========== QUOTING AND ESCAPING METHODS ==========

    /**
     * Quotes a field name for use in SQL statements with proper escaping.
     *
     * @param fieldName Field name to quote
     * @return Properly quoted field name for SQL
     */
    public static String quoteFieldNameForSql(String fieldName) {
        return quoteFieldName(fieldName, REPLACE_DOUBLE_QUOTE_SQL);
    }

    /**
     * Quotes a field name with specified quote replacement string.
     *
     * @param fieldName          Field name to quote
     * @param replaceQuoteString String to replace quotes with
     * @return Properly quoted and escaped field name
     */
    public static String quoteFieldName(String fieldName, String replaceQuoteString) {
        if (fieldName == null || fieldName.isEmpty()) {
            return String.format("%c%c", DOUBLE_QUOTE, DOUBLE_QUOTE);
        }

        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar(DOT_CHAR);
        parser.setEscapeChar(ESCAPE_CHAR);

        List<String> tokens = parser.tokenize(fieldName);
        StringBuilder builder = new StringBuilder();

        for (String token : tokens) {
            if (token.length() > 1) {
                char begin = token.charAt(0);
                char end = token.charAt(token.length() - 1);
                if ((begin == SINGLE_QUOTE || begin == DOUBLE_QUOTE) && begin == end) {
                    /*
                     * already quoted, assume what's inside is correct but
                     * requote it
                     */
                    token = token.substring(1, token.length() - 1);
                }
            }
            /*
             * For Teradata SQL, double the double quote
             */
            builder.append(DOUBLE_QUOTE).append(token.replace(String.valueOf(DOUBLE_QUOTE), replaceQuoteString))
                    .append(DOUBLE_QUOTE).append(DOT_CHAR);
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    /**
     * Returns a quoted name for use in SQL statements.
     *
     * @param name Name to quote
     * @return Quoted name suitable for SQL
     */
    public static String getQuotedName(String name) {
        return quoteFieldNameForSql(name);
    }

    // ========== ENCRYPTION/DECRYPTION METHODS ==========

    /**
     * Decodes an AES-encrypted input stream using the provided secret key.
     *
     * @param is             Input stream containing encrypted data
     * @param secretKeyBytes AES secret key bytes
     * @param file           File name for error reporting
     * @return Decrypted input stream
     * @throws Exception If decryption setup or execution fails
     */
    private static InputStream decodeAES(InputStream is, byte[] secretKeyBytes, String file) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec iv = readIV(is, file);
        SecretKey secretKey = new SecretKeySpec(secretKeyBytes, "AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, iv);
        return new CipherInputStream(is, cipher);
    }

    /**
     * Reads the initialization vector (IV) from the input stream.
     *
     * @param is   Input stream containing the IV
     * @param file File name for error reporting
     * @return IV parameter specification for AES decryption
     * @throws Exception If IV reading fails or insufficient bytes are available
     */
    private static IvParameterSpec readIV(InputStream is, String file) throws Exception {
        byte[] ivBytes = new byte[16];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(ivBytes); // Generate a random IV
        int bytesRead = 0;
        while (bytesRead < ivBytes.length) {
            int read = is.read(ivBytes, bytesRead, ivBytes.length - bytesRead);
            if (read == -1) {
                throw new Exception(String.format("Failed to read initialization vector. File '%s' has only %d bytes", file, bytesRead));
            }
            bytesRead += read;
        }
        return new IvParameterSpec(ivBytes);
    }
}