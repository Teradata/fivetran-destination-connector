package com.teradata.fivetran.destination.writers;

import com.github.luben.zstd.ZstdInputStream;
import com.google.protobuf.ByteString;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.teradata.fivetran.destination.Logger;
import com.teradata.fivetran.destination.TeradataJDBCUtil;
import com.teradata.fivetran.destination.writers.util.JSONStruct;
import fivetran_sdk.v2.*;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.CipherInputStream;
import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;


public class FastLoad {
    Connection fastLoadConnection;
    PreparedStatement preparedStatement;
    int instanceNumber;
    int batchCount;
    int batchSize;
    boolean loadCompleteStatus = false;
    private List<Column> headerColumns;
    private List<Integer> headerColumnRowIndices;
    private String columnNames;
    private List<Column> matchingCols;
    private List<Column> columns;
    Map<String, Integer> decimalScales;
    FileParams params;

    String url;
    String username;
    String password;

    Object[] nullDefaultValues = null;
    int nullDefaultValueCount = 0;
    int[] nullJdbcTypes = null;
    int[] nullJdbcScales = null;

    public boolean createFastLoadConnection(int instanceNumber, String url, String username, String password, int batchSize, Map<String, Integer> decimalScales) {
        Logger.logMessage(Logger.LogLevel.INFO,"in createFastLoadConnection()");
        this.url = url;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
        this.decimalScales = decimalScales;

        this.instanceNumber = instanceNumber;
        try {
            Class.forName("com.teradata.jdbc.TeraDriver");
            fastLoadConnection = DriverManager.getConnection(url, username, password);
            Logger.logMessage(Logger.LogLevel.INFO,"fastLoadConnection done, session no: "
                    + fastLoadConnection.nativeSQL("{fn teradata_session_number}"));
            preparedStatement = fastLoadConnection.prepareStatement(null);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
        Logger.logMessage(Logger.LogLevel.INFO,"Created FastLoad connection with session: " + instanceNumber);
        return true;
    }

    public boolean closeFastLoadConnection() {
        Logger.logMessage(Logger.LogLevel.INFO,"in closeFastLoadConnection()");
        try {
            preparedStatement.close();
            fastLoadConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
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

    public void setHeader(List<String> header) throws SQLException {
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("Setting header with columns: %s", header));
        matchingCols = columns.stream()
                .filter(Column::getPrimaryKey)
                .collect(Collectors.toList());

        // Header names not found among the physical columns (e.g. the transient
        // _fivetran_earliest marker, see IDE-26747) are skipped here; rowIndices
        // lets writeRow still read each surviving value from its original
        // position in the CSV row.
        Writer.HeaderMapping mapping = Writer.resolveHeaderColumns(header, columns);
        headerColumns = mapping.matchedColumns;
        headerColumnRowIndices = mapping.rowIndices;

        columnNames = headerColumns.stream()
                .map(Column::getName)
                .map(TeradataJDBCUtil::escapeIdentifier)
                .collect(Collectors.joining(", "));

        String placeholders = headerColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
    }

    public void writeRow(List<String> row) throws Exception {
        Logger.logMessage(Logger.debugLogLevel, "#########################LoadDataWriter.writeRow#########################");
        try {
            for (int j = 0; j < headerColumns.size(); j++) {
                int i = headerColumnRowIndices.get(j);
                String columnName = headerColumns.get(j).getName();
                DataType type = headerColumns.get(j).getType();
                String value = row.get(i);
                int paramIndex = j + 1;

                if (value == null || value.equals("null") || value.equals(params.getNullString())) {
                    preparedStatement.setNull(paramIndex, getSqlTypeFromDataType(type));
                    Logger.logMessage(Logger.debugLogLevel,
                            String.format("Set parameter at index %d to NULL (DataType: %s)", paramIndex, type));
                    continue;
                }

                switch (type) {
                    case BOOLEAN:
                        if (value.equalsIgnoreCase("true")) {
                            preparedStatement.setByte(paramIndex, (byte) 1);
                            Logger.logMessage(Logger.debugLogLevel,
                                    String.format("Set BOOLEAN parameter at index %d: %s (as byte 1)", paramIndex, value));
                        } else if (value.equalsIgnoreCase("false")) {
                            preparedStatement.setByte(paramIndex, (byte) 0);
                            Logger.logMessage(Logger.debugLogLevel,
                                    String.format("Set BOOLEAN parameter at index %d: %s (as byte 0)", paramIndex, value));
                        }
                        break;

                    case SHORT:
                        preparedStatement.setShort(paramIndex, Short.parseShort(value));
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: SHORT) at index %d: %s", paramIndex, value));
                        break;
                    case INT:
                        preparedStatement.setInt(paramIndex, Integer.parseInt(value));
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: INT) at index %d: %s", paramIndex, value));
                        break;

                    case LONG:
                        preparedStatement.setLong(paramIndex, Long.parseLong(value));
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: LONG) at index %d: %s", paramIndex, value));
                        break;

                    case DECIMAL:
                        int scale = decimalScales.get(columnName.toLowerCase());
                        BigDecimal bd = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
                        preparedStatement.setBigDecimal(paramIndex, bd);
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: DECIMAL) at index %d: %s", paramIndex, bd.toString()));
                        break;

                    case FLOAT:
                        preparedStatement.setFloat(paramIndex, Float.parseFloat(value));
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: FLOAT) at index %d: %s", paramIndex, value));
                        break;

                    case DOUBLE:
                        preparedStatement.setDouble(paramIndex, Double.parseDouble(value));
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: DOUBLE) at index %d: %s", paramIndex, value));
                        break;

                    case NAIVE_TIME:
                        preparedStatement.setTime(paramIndex, Time.valueOf(value));
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: NAIVE_TIME) at index %d: %s", paramIndex, value));
                        break;

                    case NAIVE_DATE:
                        preparedStatement.setDate(paramIndex, Date.valueOf(value));
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: NAIVE_DATE) at index %d: %s", paramIndex, value));
                        break;

                    case NAIVE_DATETIME:
                    case UTC_DATETIME:
                        Timestamp ts = TeradataJDBCUtil.getTimestampFromObject(
                                TeradataJDBCUtil.formatISODateTime(value));
                        preparedStatement.setTimestamp(paramIndex, ts);
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: NAIVE_DATETIME/UTC_DATETIME) at index %d: %s (Formatted: %s)",
                                        paramIndex, value, ts));
                        break;

                    case BINARY:
                        preparedStatement.setBytes(paramIndex, Base64.getDecoder().decode(value));
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: BINARY) at index %d (Base64-decoded)", paramIndex));
                        break;

                    case XML:
                        SQLXML sqlxml = preparedStatement.getConnection().createSQLXML();
                        sqlxml.setString(value);
                        preparedStatement.setSQLXML(paramIndex, sqlxml);
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: XML) at index %d", paramIndex));
                        break;

                    case STRING:
                        preparedStatement.setString(paramIndex, value);
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: STRING) at index %d: %s", paramIndex, value));
                        break;

                    case JSON:
                        preparedStatement.setObject(paramIndex, new JSONStruct("JSON", new Object[]{value}));
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: JSON) at index %d: %s", paramIndex, value));
                        break;

                    default:
                        preparedStatement.setObject(paramIndex, value);
                        Logger.logMessage(Logger.debugLogLevel,
                                String.format("Set parameter (DataType: %s) at index %d: %s", type, paramIndex, value));
                        break;
                }
            }

            preparedStatement.addBatch();
            batchCount++;
            Logger.logMessage(Logger.debugLogLevel, "Added row to batch. Current batch count: " + batchCount);

        } catch (SQLException e) {
            throw new Exception("Failed to write row to FastLoad: " + e.getMessage(), e);
        }
    }


    public void loadData(String file, List<Column> columns, FileParams params, Map<String, ByteString> secretKeys) throws Exception {
        Logger.logMessage(Logger.LogLevel.INFO,"Instance[" + instanceNumber + "] in loadData() for file: " + file);
        this.params = params;
        this.columns = columns;
        batchCount = 0;
        FileInputStream is = new FileInputStream(Writer.validateBatchFilePath(file).toFile());
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
                return;
            }
            List<String> header = new ArrayList<>(Arrays.asList(headerString));
            setHeader(header);

            String[] tokens;
            while ((tokens = csvReader.readNext()) != null) {
                List<String> row = new ArrayList<>(Arrays.asList(tokens));
                writeRow(row);
                Logger.logMessage(Logger.debugLogLevel,"batch size: " + batchSize);
                Logger.logMessage(Logger.debugLogLevel, "Current batch count after writing row: " + batchCount);
                if (batchCount >= batchSize) {
                    try {
                        preparedStatement.executeBatch();
                        Logger.logMessage(Logger.LogLevel.INFO,"Instance[" + instanceNumber + "] inserted " + batchCount + " rows in DBS");
                        batchCount = 0;
                    } catch (BatchUpdateException bue) {
                        String actualError = "";
                        if (bue.getNextException() != null) {
                            Exception nextException = bue.getNextException();
                            actualError = nextException.getMessage();
                        } else {
                            actualError = bue.getMessage();
                        }
                        Logger.logMessage(Logger.LogLevel.SEVERE,
                                String.format("WriteBatch failed with exception %s", actualError));
                    }
                    catch (SQLException e) {
                        throw new Exception("Failed to execute batch: " + e.getMessage(), e);
                    }
                }
            }
        }
        Logger.logMessage(Logger.LogLevel.INFO,"Instance[" + instanceNumber + "] loadData completed for file: " + file);
    }

    private void setLoadCompleted(boolean loadCompleteStatus) {
        this.loadCompleteStatus = loadCompleteStatus;
    }

    public void markLoadCompleted() {
        this.loadCompleteStatus = true;
    }

    public boolean getLoadCompleted() {
        return loadCompleteStatus;
    }

    public void loadLeftOverRows() {
        if (batchCount > 0) {
            try {
                preparedStatement.executeBatch();
                Logger.logMessage(Logger.LogLevel.INFO,"Instance[" + instanceNumber + "] inserted " + batchCount + " rows in DBS from loadLeftOverRows()");
                batchCount = 0;
            } catch (BatchUpdateException bue) {
                String actualError = "";
                if (bue.getNextException() != null) {
                    Exception nextException = bue.getNextException();
                    actualError = nextException.getMessage();
                } else {
                    actualError = bue.getMessage();
                }
                Logger.logMessage(Logger.LogLevel.SEVERE,
                        String.format("WriteBatch failed with exception %s", actualError));
            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private InputStream decodeAES(InputStream is, byte[] secretKeyBytes, String file) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec iv = readIV(is, file);
        SecretKey secretKey = new SecretKeySpec(secretKeyBytes, "AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, iv);
        return new CipherInputStream(is, cipher);
    }

    private IvParameterSpec readIV(InputStream is, String file) throws Exception {
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