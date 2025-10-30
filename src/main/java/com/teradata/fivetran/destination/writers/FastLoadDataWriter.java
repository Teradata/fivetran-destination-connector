package com.teradata.fivetran.destination.writers;

import java.io.*;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.google.protobuf.ByteString;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.teradata.fivetran.destination.TeradataConfiguration;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.Compression;
import fivetran_sdk.v2.Encryption;
import fivetran_sdk.v2.FileParams;
import com.teradata.fivetran.destination.Logger;

import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import jdk.jpackage.internal.Log;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class FastLoadDataWriter {
    protected static String SQL_GET_LSN = "{fn teradata_logon_sequence_number()}";
    protected static String jdbcDriver = "com.teradata.jdbc.TeraDriver";


    /**
     * Constructor for Writer.
     *
     * @param conn       The database connection.
     * @param database   The database name.
     * @param table      The table name.
     * @param columns    The list of columns.
     * @param params     The file parameters.
     * @param secretKeys The map of secret keys.
     * @param batchSize  The batch size for writing rows.
     */

    public FastLoadDataWriter(TeradataConfiguration conf, Connection conn, String database, String table, List<Column> columns,
                              FileParams params, Map<String, ByteString> secretKeys, Integer batchSize, List<String> sourceFilesList) {

        String dbsHost = conf.host();
        String username = conf.user();
        String password = conf.password();
        String outputTableName = table;
        //String outputTableName = String.format("%s_%s", "td_tmp", UUID.randomUUID().toString().replace("-", "_"));
        String errorTable1 = outputTableName + "_ERR1";
        String errorTable2 = outputTableName + "_ERR2";
        Logger.logMessage(Logger.LogLevel.INFO,"Output Table Name: " + outputTableName);
        Logger.logMessage(Logger.LogLevel.INFO,"Error Table 1: " + errorTable1);
        Logger.logMessage(Logger.LogLevel.INFO,"Error Table 2: " + errorTable2);

        int instances = sourceFilesList.size();

        String endLoading = "END LOADING";

        Connection lsnConnection = null;
        Statement stmt = null;

        String lsnUrl = "jdbc:teradata://" + dbsHost
                + "/LSS_TYPE=L,TMODE=TERA,CONNECT_FUNCTION=1,TSNANO=6,TNANO=0"; // Control Session
        try {
            Class.forName(jdbcDriver);
            lsnConnection = DriverManager.getConnection(lsnUrl, username, password);

            String lsnNumber = lsnConnection.nativeSQL(SQL_GET_LSN);
            Logger.logMessage(Logger.LogLevel.INFO,"FastLoad LSN: " + lsnNumber);
            String fastLoadURL = "jdbc:teradata://" + dbsHost
                    + "/LSS_TYPE=L,PARTITION=FASTLOAD,CONNECT_FUNCTION=2,TSNANO=6,TNANO=0,LOGON_SEQUENCE_NUMBER="
                    + lsnNumber; // FastLoad Session
            // Session
            String beginLoading = "BEGIN LOADING " + outputTableName + " ERRORFILES " + errorTable1 + ", " + errorTable2
                    + " WITH INTERVAL";

            // Creating FastLoad Connections
            FastLoad fastLoad[] = new FastLoad[instances];
            for (int i = 0; i < instances; i++) {
                fastLoad[i] = new FastLoad();
            }
            Logger.logMessage(Logger.LogLevel.INFO,"fastLoadURL: " + fastLoadURL);
            Logger.logMessage(Logger.LogLevel.INFO,"username: " + username);
            Logger.logMessage(Logger.LogLevel.INFO,"password: " + password);

            for (int i = 0; i < instances; i++) {
                Logger.logMessage(Logger.LogLevel.INFO,"calling createFastLoadConnection() for instance : " + i);
                fastLoad[i].createFastLoadConnection(i + 1, fastLoadURL, username, password, batchSize);
            }

            Logger.logMessage(Logger.LogLevel.INFO, "Getting header from file: " + sourceFilesList.get(0));
            List<String> header = getHeader(sourceFilesList.get(0), params, secretKeys); // to check if file is empty
            if (header == null) {
                Logger.logMessage(Logger.LogLevel.SEVERE, "Source file is empty. Exiting FastLoadDataWriter.");
                return;
            }
            Logger.logMessage(Logger.LogLevel.INFO, "Header: " + header);

            // Submitting beginLoading
            stmt = lsnConnection.createStatement();
            stmt.executeUpdate("SET SESSION DateForm = IntegerDate");
            lsnConnection.setAutoCommit(true);
            stmt.execute(beginLoading);

            String usingInsertSQL = getusingInsertSQL(lsnConnection, outputTableName, header);
            Logger.logMessage(Logger.LogLevel.INFO,"usingInsertSQL: " + usingInsertSQL);
            // submitting usingInsertSQL
            lsnConnection.setAutoCommit(false);
            stmt.executeUpdate(usingInsertSQL);

            FastLoadThread fastLoadThread[] = new FastLoadThread[instances];
            for (int i = 0; i < instances; i++) {
                fastLoadThread[i] = new FastLoadThread(fastLoad[i], sourceFilesList.get(i), columns, params, secretKeys);
                Logger.logMessage(Logger.LogLevel.INFO,"Starting Thread: " + i);
                fastLoadThread[i].start();
            }
            int count = 0;
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                }
                for (int i = 0; i < instances; i++) {
                    if (fastLoad[i].getLoadCompleted()) {
                        count++;
                    }
                }
                if (count == instances) {
                    break;
                }
                count = 0;
            }

            /*
             * // Passing data to each session for (int i = 0; i < instances; i++) {
             * fastLoad[i].loadData(datasetList.get(i), structFields); }
             */

            // Load left over rows
            for (int i = 0; i < instances; i++) {
                fastLoad[i].loadLeftOverRows();
            }

            stmt.executeUpdate("CHECKPOINT LOADING END");
            lsnConnection.commit();

            // submitting endLoading
            stmt.executeUpdate(endLoading);
            lsnConnection.commit();

            lsnConnection.setAutoCommit(true);
            stmt.close();

            for (int i = 0; i < instances; i++) {
                fastLoad[i].closeFastLoadConnection();
            }

            lsnConnection.close();
        } catch (SQLException ex) {
            try {
                stmt.executeUpdate(endLoading);
                lsnConnection.commit();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getusingInsertSQL(Connection con, String outputTableName, List<String> header) {
        try {
            Statement stmt = con.createStatement();
            ResultSet res = stmt
                    .executeQuery("select count(*) from dbc.columns where tablename='" + outputTableName + "';");
            res.next();
            Logger.logMessage(Logger.LogLevel.INFO,"Total columns in table " + outputTableName + ": " + res.getInt(1));

            // First, get all available columns from the database
            res = null;
            res = stmt.executeQuery("select columnName from dbc.columns where tablename='" + outputTableName + "';");

            List<String> availableColumns = new ArrayList<>();
            while (res.next()) {
                Logger.logMessage(Logger.LogLevel.INFO, "Found column in table: " + res.getString("columnName"));
                availableColumns.add(res.getString("columnName").trim());
            }
            Logger.logMessage(Logger.LogLevel.INFO,"Available columns in table " + outputTableName + ": " + availableColumns);

            // Filter and order columns based on header, while ensuring they exist in the table
            List<String> orderedColumns = new ArrayList<>();
            for (String headerCol : header) {
                Logger.logMessage(Logger.LogLevel.INFO,"Processing header column: " + headerCol);
                String trimmedHeader = headerCol.trim();
                Logger.logMessage(Logger.LogLevel.INFO,"Trimmed header column: " + trimmedHeader);
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
                if (fieldDesc.getType() == java.sql.Types.TIME) {
                    if (fieldDesc.getScale() < lowestScaleForTime) {
                        lowestScaleForTime = fieldDesc.getScale();
                    }
                } else if (fieldDesc.getType() == java.sql.Types.TIMESTAMP) {
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
            return TeradataConnection.getUsingSQL(outputTableName, fieldNames, fieldTypes4Using, "UTF-8");
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        return null;
    }

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
        String SQL_SELECT_FROM_SOURCE_WHERE = "SELECT %s FROM %s %s";
        return String.format(SQL_SELECT_FROM_SOURCE_WHERE, colExpBuilder.toString(), tableName, "");
    }

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

    public static TeradataColumnDesc[] getColumnDesc(String tableName, String[] fieldNames,
                                                     Connection connection) throws SQLException {
        return getColumnDescsForSQL(getSelectSQL(tableName, fieldNames),
                connection);
    }

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
                throw new Exception(String.format("Failed to read initialization vector. File '%s' has only %d bytes", file, bytesRead));
            }
            bytesRead += read;
        }
        return new IvParameterSpec(ivBytes);
    }
}