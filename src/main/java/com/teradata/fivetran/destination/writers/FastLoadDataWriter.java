package com.teradata.fivetran.destination.writers;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.Logger;
import com.teradata.fivetran.destination.TeradataConfiguration;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.Compression;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.Encryption;
import fivetran_sdk.v2.FileParams;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.SecureRandom;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import com.github.luben.zstd.ZstdInputStream;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

/**
 * Handles data loading into Teradata using the FastLoad protocol.
 * This class manages the entire lifecycle of a FastLoad operation including:
 * - Creating temporary and error tables
 * - Managing FastLoad sessions
 * - Distributing files across multiple threads
 * - Handling data loading and error checking
 * - Performing final merge (upsert) operations
 */
public class FastLoadDataWriter {
    // SQL constants for Teradata interactions
    private static final String SQL_GET_LSN = "{fn teradata_lsn}";

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
    private Connection conn; // Main database connection for DDL operations
    private Connection lsnConnection = null; // LSN control session connection
    private Statement stmt = null; // Statement object for SQL execution

    // Schema and table information
    private String database; // Target database name
    private String table; // Target table name
    private List<Column> columns; // Column definitions from schema
    private List<Column> matchingCols; // Primary key columns for upsert operations
    private FileParams params; // File parameters (compression, encryption)
    private Map<String, ByteString> secretKeys; // Encryption keys for file decryption
    private Integer batchSize; // Batch size for loading operations

    // Header and column information
    private List<Column> headerColumns; // Columns extracted from CSV header
    private String columnNames; // Comma-separated column names for SQL

    // Connection configuration
    private String dbsHost; // Teradata host address
    private String username; // Database username
    protected String password; // Database password

    // Temporary table names
    private String outputTableName; // Temporary output table for FastLoad
    private String errorTable1; // First error table for FastLoad
    private String errorTable2; // Second error table for FastLoad

    // ========== CONSTRUCTOR ==========

    /**
     * Constructs a FastLoadDataWriter with the specified configuration and
     * parameters.
     *
     * @param conf       Teradata configuration containing connection details
     * @param conn       Database connection for table operations
     * @param database   Target database name
     * @param table      Target table name
     * @param columns    List of column definitions from schema
     * @param params     File parameters for compression and encryption
     * @param secretKeys Map of file names to encryption keys for AES decryption
     * @param batchSize  Batch size for loading operations
     */
    public FastLoadDataWriter(TeradataConfiguration conf, Connection conn, String database, String table,
            List<Column> columns,
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
     * This method coordinates the entire FastLoad process including temporary table
     * creation,
     * parallel file loading, and cleanup.
     *
     * @param sourceFilesList List of source file paths to be loaded
     * @throws Exception If any error occurs during the loading process
     */
    public void writeData(List<String> sourceFilesList) throws Exception {
        Logger.logMessage(Logger.LogLevel.INFO, "Getting header from file: " + sourceFilesList.get(0));

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
                    createTempTableSQL, e);
        }

        String beginLoading = String.format("BEGIN LOADING %s ERRORFILES %s, %s WITH INTERVAL", outputTableName,
                errorTable1, errorTable2);
        String endLoading = "END LOADING";

        String lsnUrl = "jdbc:teradata://" + dbsHost
                + "/LSS_TYPE=L,TMODE=TERA,CONNECT_FUNCTION=1,TSNANO=6,TNANO=0"; // Control Session
        try {
            Class.forName(jdbcDriver);
            lsnConnection = DriverManager.getConnection(lsnUrl, username, password);

            // Determine number of sessions based on workload
            // int maxSessions = getFastloadSessionCount(sourceFilesList.size());
            int maxSessions = 1;
            int numSessions = Math.min(sourceFilesList.size(), maxSessions);
            Logger.logMessage(Logger.LogLevel.INFO,
                    "Using " + numSessions + " FastLoad sessions for " + sourceFilesList.size() + " files.");

            String lsnNumber = lsnConnection.nativeSQL(SQL_GET_LSN);
            Logger.logMessage(Logger.LogLevel.INFO, "FastLoad LSN: " + lsnNumber);
            String fastLoadURL = "jdbc:teradata://" + dbsHost
                    + "/LSS_TYPE=L,PARTITION=FASTLOAD,CONNECT_FUNCTION=2,TSNANO=6,TNANO=0,LOGON_SEQUENCE_NUMBER="
                    + lsnNumber; // FastLoad Session

            // Creating FastLoad Connections
            FastLoad fastLoad[] = new FastLoad[numSessions];
            for (int i = 0; i < numSessions; i++) {
                fastLoad[i] = new FastLoad();
            }
            Logger.logMessage(Logger.LogLevel.INFO, "fastLoadURL: " + fastLoadURL);

            for (int i = 0; i < numSessions; i++) {
                Logger.logMessage(Logger.LogLevel.INFO, "calling createFastLoadConnection() for instance : " + (i + 1));
                fastLoad[i].createFastLoadConnection(i + 1, fastLoadURL, username, password, batchSize);
            }

            // Submitting beginLoading
            stmt = lsnConnection.createStatement();
            stmt.executeUpdate("SET SESSION DateForm = IntegerDate");
            lsnConnection.setAutoCommit(true);
            stmt.execute(beginLoading);

            String usingInsertSQL = getusingInsertSQL(lsnConnection, database, outputTableName, header);
            Logger.logMessage(Logger.LogLevel.INFO, "usingInsertSQL: " + usingInsertSQL);
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
                Logger.logMessage(Logger.LogLevel.INFO, "Starting Thread: " + i);
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
                if (stmt != null && !stmt.isClosed()) {
                    stmt.executeUpdate(endLoading);
                    lsnConnection.commit();
                }
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

    private int getFastloadSessionCount(int requestedSessions) throws SQLException {
        int numSugSessn = requestedSessions;
        String CHECK_WORKLOAD = "CHECK WORKLOAD FOR ";
        String CHECK_WORKLOAD_END = "CHECK WORKLOAD END";
        String SQL_BEGIN_LOADING = "BEGIN LOADING";

        boolean isGoverned = "true".equals(lsnConnection.nativeSQL("{fn teradata_provide(governed)}"));
        String check_workload_end = (!isGoverned ? "{fn teradata_failfast}" : "") + CHECK_WORKLOAD_END;

        String checkWorkLoad = CHECK_WORKLOAD + SQL_BEGIN_LOADING + " " + outputTableName
                + " ERRORFILES " + errorTable1 + ", " + errorTable2 + " WITH INTERVAL";

        Logger.logMessage(Logger.debugLogLevel,
                "FastLoadDataWriter.getFastloadSessionCount(): create Statement object from lsnConnection");
        Statement controlStmt = lsnConnection.createStatement();
        Logger.logMessage(Logger.debugLogLevel,
                "FastLoadDataWriter.getFastloadSessionCount(): executing checkWorkLoad");
        controlStmt.executeUpdate(checkWorkLoad);
        Logger.logMessage(Logger.debugLogLevel,
                "FastLoadDataWriter.getFastloadSessionCount(): executing check_workload_end");
        ResultSet rs = controlStmt.executeQuery(check_workload_end);

        ResultSetMetaData rsmd = rs.getMetaData();
        if (rs.next() && rsmd.getColumnCount() >= 2 && rs.getString(1) != null) {
            if (rs.getString(1).trim().equalsIgnoreCase("Y")) {
                int count = rs.getInt(2);
                if (count > 0) {
                    numSugSessn = Math.min(requestedSessions, count);

                    if (numSugSessn < requestedSessions) {
                        Logger.logMessage(Logger.LogLevel.INFO, "User provided number of sessions [" +
                                requestedSessions + "] is overridden by [" + numSugSessn +
                                "] returned from DBS");
                    } else {
                        Logger.logMessage(Logger.LogLevel.INFO,
                                "User provided number of sessions is NOT overridden by [" + count + "] DBS.");
                    }
                    return numSugSessn;
                } else {
                    Logger.logMessage(Logger.LogLevel.INFO, "invalid number " + count + " returned from DBS");
                }
            } else {
                Logger.logMessage(Logger.LogLevel.INFO, "returned TASM-flag is N");
            }
        } else {
            Logger.logMessage(Logger.LogLevel.INFO, "unrecognized column returned");
        }

        rs.close();
        controlStmt.close();
        return numSugSessn;
    }

    /**
     * Performs delete-insert operation (upsert) from temporary table to target
     * table.
     * Deletes existing records based on primary key matches and inserts all records
     * from temporary table.
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
                        condition);
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

    public void dropTempTable() {
        try {
            if (conn == null || conn.isClosed()) {
                Logger.logMessage(Logger.debugLogLevel, "Connection is closed. Cannot drop temporary table.");
                return;
            }
            if (database == null || outputTableName == null) {
                Logger.logMessage(Logger.debugLogLevel,
                        "Database or temporary table name is null. Cannot drop temporary table.");
                return;
            }
            String deleteQuery = String.format("DELETE FROM %s",
                    TeradataJDBCUtil.escapeTable(database, outputTableName));
            String dropQuery = String.format("DROP TABLE %s", TeradataJDBCUtil.escapeTable(database, outputTableName));
            Logger.logMessage(Logger.debugLogLevel, "Prepared SQL delete statement: " + deleteQuery);
            Logger.logMessage(Logger.debugLogLevel, "Prepared SQL drop statement: " + dropQuery);

            conn.createStatement().execute(deleteQuery);
            Logger.logMessage(Logger.debugLogLevel, "Temporary table deleted successfully.");
            conn.createStatement().execute(dropQuery);
            Logger.logMessage(Logger.debugLogLevel, "Temporary table dropped successfully.");
        } catch (SQLException e) {
            if (e.getErrorCode() != 3807) {
                Logger.logMessage(Logger.LogLevel.SEVERE,
                        "Failed to delete or drop temporary table: " + e.getMessage());
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
                Logger.logMessage(Logger.debugLogLevel,
                        "Database or error table names are null. Cannot drop error tables.");
                return;
            }

            String[] errorTables = { errorTable1, errorTable2 };

            for (String errorTable : errorTables) {
                if (errorTable == null || errorTable.isEmpty()) {
                    continue; // skip null/empty
                }

                String escapedTable = TeradataJDBCUtil.escapeTable(database, errorTable);

                try (Statement stmt = conn.createStatement()) {
                    // Check if error table exists
                    String checkExistQuery = String.format(
                            "SELECT COUNT(*) FROM DBC.TablesV WHERE DatabaseName = '%s' AND TableName = '%s';",
                            database, errorTable);
                    Logger.logMessage(Logger.debugLogLevel, "Checking existence of error table: " + errorTable);

                    ResultSet rs = stmt.executeQuery(checkExistQuery);
                    rs.next();
                    int exists = rs.getInt(1);

                    if (exists == 0) {
                        Logger.logMessage(Logger.debugLogLevel, "Error table " + errorTable + " does not exist.");
                        continue;
                    }

                    // Check if error table has rows
                    String countQuery = String.format("SELECT COUNT(*) FROM %s;", escapedTable);
                    Logger.logMessage(Logger.debugLogLevel, "Checking row count for: " + errorTable);

                    rs = stmt.executeQuery(countQuery);
                    rs.next();
                    int rowCount = rs.getInt(1);

                    if (rowCount == 0) {
                        // Drop empty error table
                        String deleteQuery = String.format("DELETE FROM %s;", escapedTable);
                        String dropQuery = String.format("DROP TABLE %s;", escapedTable);

                        Logger.logMessage(Logger.debugLogLevel,
                                "Error table " + errorTable + " is empty. Dropping it.");
                        stmt.execute(deleteQuery);
                        stmt.execute(dropQuery);
                        Logger.logMessage(Logger.debugLogLevel, "Error table " + errorTable + " dropped successfully.");
                    } else {
                        // Error table has rows - this indicates data loading issues
                        String message = String.format(
                                "Error table %s contains %d rows. Please check the table for loading errors.",
                                errorTable, rowCount);
                        Logger.logMessage(Logger.LogLevel.SEVERE, message);
                        throw new SQLException(message);
                    }
                } catch (SQLException e) {
                    if (e.getErrorCode() != 3807) { // Ignore "Table does not exist" error if it happens during drop
                        Logger.logMessage(Logger.LogLevel.WARNING,
                                "Failed to drop error table " + errorTable + ": " + e.getMessage());
                    } else {
                        Logger.logMessage(Logger.debugLogLevel, "Error table " + errorTable + " already dropped.");
                    }
                }
            }
        } catch (SQLException e) {
            Logger.logMessage(Logger.LogLevel.WARNING, "Error checking/dropping error tables: " + e.getMessage());
        }
    }

    public static List<String> getHeader(String file, FileParams params, Map<String, ByteString> secretKeys)
            throws Exception {
        FileInputStream is = new FileInputStream(file);
        InputStream decoded = is;
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
            String[] header = csvReader.readNext();
            if (header == null) {
                return null;
            }
            List<String> headerList = new ArrayList<>();
            for (String s : header) {
                headerList.add(s);
            }
            return headerList;
        }
    }

    private static InputStream decodeAES(InputStream is, byte[] secretKeyBytes, String file) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec iv = readIV(is, file);
        SecretKey secretKey = new SecretKeySpec(secretKeyBytes, "AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, iv);
        return new CipherInputStream(is, cipher);
    }

    private static IvParameterSpec readIV(InputStream is, String file) throws Exception {
        byte[] ivBytes = new byte[16];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(ivBytes); // Generate a random IV
        int bytesRead = 0;
        while (bytesRead < ivBytes.length) {
            int read = is.read(ivBytes, bytesRead, ivBytes.length - bytesRead);
            if (read == -1) {
                throw new Exception(String.format("Failed to read initialization vector. File '%s' has only %d bytes",
                        file, bytesRead));
            }
            bytesRead += read;
        }
        return new IvParameterSpec(ivBytes);
    }

    private String getusingInsertSQL(Connection lsnConnection, String database, String outputTableName,
            List<String> header) throws SQLException {
        String usingInsertSQL = "INSERT INTO " + TeradataJDBCUtil.escapeTable(database, outputTableName) + " (";
        String valuesSQL = "VALUES (";
        for (int i = 0; i < header.size(); i++) {
            usingInsertSQL += TeradataJDBCUtil.escapeIdentifier(header.get(i));
            valuesSQL += ":" + TeradataJDBCUtil.escapeIdentifier(header.get(i));
            if (i < header.size() - 1) {
                usingInsertSQL += ",";
                valuesSQL += ",";
            }
        }
        usingInsertSQL += ") " + valuesSQL + ")";
        return usingInsertSQL;
    }
}