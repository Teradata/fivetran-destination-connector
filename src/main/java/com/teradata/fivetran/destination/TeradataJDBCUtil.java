package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.warning_util.WarningHandler;
import com.teradata.fivetran.destination.writers.ColumnMetadata;
import com.teradata.fivetran.destination.writers.JSONStruct;
import fivetran_sdk.v2.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;
import java.util.function.Function;

public class TeradataJDBCUtil {


    /**
     * Creates a connection to the Teradata database using the provided configuration.
     *
     * @param conf The Teradata configuration.
     * @return A connection to the Teradata database.
     * @throws SQLException If a database access error occurs.
     * @throws ClassNotFoundException If the JDBC driver class is not found.
     */
    static Connection createConnection(TeradataConfiguration conf) throws Exception {
        Properties connectionProps = new Properties();

        connectionProps.put("logmech", conf.logmech());
        if (!conf.logmech().equals("BROWSER")) {
            connectionProps.put("user", conf.user());
            if (conf.password() != null) {
                connectionProps.put("password", conf.password());
            }
        }

        connectionProps.put("tmode", conf.tmode());
        connectionProps.put("FLATTEN","ON");
        connectionProps.put("sslMode", conf.sslMode());
        Set<String> CaModes = new HashSet<>(Arrays.asList("DISABLE", "ALLOW", "PREFER", "REQUIRE"));
        if (!CaModes.contains(conf.sslMode())) {
            /*
            String[] sslCertPaths = writeSslCertToFile(conf.sslServerCert());
            connectionProps.put("sslcapath", sslCertPaths[0]);
            connectionProps.put("sslca", sslCertPaths[1]);
            Logger.logMessage(Logger.LogLevel.INFO, "SSLCAPATH: " + sslCertPaths[0]);
            Logger.logMessage(Logger.LogLevel.INFO, "SSLCA: " + sslCertPaths[1]);
             */

            connectionProps.put("SSLBASE64", conf.sslServerCert());
        }

        String driverParameters = conf.driverParameters();
        if (driverParameters != null) {
            for (String parameter : driverParameters.split(",")) {
                String[] keyValue = parameter.split("=");
                if (keyValue.length != 2) {
                    throw new Exception("Invalid value of 'driverParameters' configuration");
                }
                putIfNotEmpty(connectionProps, keyValue[0], keyValue[1]);
            }
        }

        String queryBandText = handleQueryBand(conf.queryBand());

        String url = String.format("jdbc:teradata://%s", conf.host());
        Class.forName("com.teradata.jdbc.TeraDriver");
        Connection conn = DriverManager.getConnection(url, connectionProps);
        Statement stmt = conn.createStatement();

        try{
            stmt.execute(String.format("SET QUERY_BAND = '%s' FOR SESSION;", queryBandText));
        } catch (SQLException e) {
            Logger.logMessage(Logger.LogLevel.SEVERE, "Failed to set query band, please check the format for setting query band: " + e.getMessage());
        }

        return conn;
    }

    /**
     * Puts a key-value pair in a properties object if the key and value are not empty.
     *
     * @param props The properties object.
     * @param key The key.
     * @param value The value.
     */
    private static void putIfNotEmpty(Properties props, String key, String value) {
        if (key != null && !key.trim().isEmpty() && value != null && !value.trim().isEmpty()) {
            props.put(key.trim(), value.trim());
        }
    }

    /**
     *
     * @param sslCert: The SSL certificate as raw String
     * @return the array of string with ssl certificate file's paths
     * @throws IOException
     */
    /*
    private static String[] writeSslCertToFile(String sslCert) throws IOException {
        File tempFile = File.createTempFile("sslCert", ".pem");
        tempFile.deleteOnExit();
        try (FileWriter writer = new FileWriter(tempFile)) {
            sslCert = sslCert.replace("\\n", "\n" );
            writer.write(sslCert);
        }
        return new String[]{tempFile.getParent(), tempFile.getAbsolutePath()};
    }
     */

    /**
     * Handles and validates the user-defined query band text.
     *
     * @return The validated query band text, ensuring required parameters are presented in required
     * format.
     */
    private static String handleQueryBand(String queryBand) {
        // Split the queryBand into key-value pairs
        String[] pairs = queryBand.split(";");
        Map<String, String> queryBandMap = new HashMap<>();

        // Populate the map with key-value pairs
        for (String pair : pairs) {
            if (!pair.trim().isEmpty()) {
                String[] keyValue = pair.split("=");
                if (keyValue.length == 2) {
                    queryBandMap.put(keyValue[0].trim(), keyValue[1].trim());
                }
            }
        }

        // Check if "org" key exists, if not add it
        if (!queryBandMap.containsKey("org")) {
            queryBandMap.put("org", "teradata-internal-telem;");
        }

        // Check if "appname" key exists
        if (queryBandMap.containsKey("appname")) {
            // Check if "airflow" exists in the value of "appname" key
            String appnameValue = queryBandMap.get("appname").toLowerCase();
            if (!appnameValue.contains("fivetran")) {
                queryBandMap.put("appname", queryBandMap.get("appname") + "_fivetran;");
            }
        } else {
            // Add "appname=fivetran;" if "appname" key does not exist
            queryBandMap.put("appname", "fivetran;");
        }

        // Reconstruct the queryBand string from the map
        StringBuilder updatedQueryBand = new StringBuilder();
        for (Map.Entry<String, String> entry : queryBandMap.entrySet()) {
            updatedQueryBand.append(entry.getKey()).append("=").append(entry.getValue());
            if (!entry.getValue().endsWith(";")) {
                updatedQueryBand.append(";");
            }
        }

        return updatedQueryBand.toString();
    }


    /**
     * Checks if a table exists in the specified database.
     *
     * @param stmt The SQL statement.
     * @param database The database name.
     * @param table The table name.
     * @return True if the table exists, false otherwise.
     */
    static boolean checkTableExists(Statement stmt, String database, String table) {
        try {
            stmt.executeQuery(String.format("SELECT * FROM %s WHERE 1=0", escapeTable(database, table)));
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Checks if a database exists.
     *
     * @param stmt The SQL statement.
     * @param database The database name.
     * @return True if the database exists, false otherwise.
     * @throws SQLException If a database access error occurs.
     */
    static boolean checkDatabaseExists(Statement stmt, String database) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(String.format("SELECT DatabaseName FROM DBC.DatabasesV WHERE DatabaseName LIKE %s", escapeString(database)))) {
            return rs.next();
        }
    }

    /**
     * Retrieves the table metadata from the Teradata database.
     *
     * @param conf The Teradata configuration.
     * @param database The database name.
     * @param table The table name.
     * @return The table metadata.
     * @throws SQLException If a database access error occurs.
     * @throws ClassNotFoundException If the JDBC driver class is not found.
     * @throws TableNotExistException If the table does not exist.
     */
    static <T> Table getTable(TeradataConfiguration conf, String database, String table,
                              String originalTableName, WarningHandler<T> warningHandler) throws TableNotExistException {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf)) {
            DatabaseMetaData metadata = conn.getMetaData();

            try (ResultSet tables = metadata.getTables(null, database, table, null)) {
                if (!tables.next()) {
                    throw new TableNotExistException(TeradataJDBCUtil.escapeTable(database,table));
                }
                if (tables.next()) {
                    warningHandler.handle(String.format("Found several tables that match %s name",
                            TeradataJDBCUtil.escapeTable(database, table)));
                }
            }

            Set<String> primaryKeys = new HashSet<>();
            try (ResultSet primaryKeysRS = metadata.getPrimaryKeys(database, null, table)) {
                while (primaryKeysRS.next()) {
                    primaryKeys.add(primaryKeysRS.getString("COLUMN_NAME"));
                }
            }

            List<Column> columns = new ArrayList<>();
            try (ResultSet columnsRS = metadata.getColumns(null, database, table, null)) {
                while (columnsRS.next()) {
                    Column.Builder c = Column.newBuilder()
                            .setName(columnsRS.getString("COLUMN_NAME"))
                            .setType(TeradataJDBCUtil.mapDataTypes(columnsRS.getInt("DATA_TYPE"),
                                    columnsRS.getString("TYPE_NAME")))
                            .setPrimaryKey(
                                    primaryKeys.contains(columnsRS.getString("COLUMN_NAME")));
                    if (c.getType() == DataType.DECIMAL) {
                        c.setParams(DataTypeParams.newBuilder()
                                .setDecimal(DecimalParams.newBuilder()
                                        .setScale(columnsRS.getInt("DECIMAL_DIGITS"))
                                        .setPrecision(columnsRS.getInt("COLUMN_SIZE")).build())
                                .build());
                    }
                    if (c.getType() == DataType.STRING) {
                        c.setParams(DataTypeParams.newBuilder()
                                .setStringByteLength(columnsRS.getInt("CHAR_OCTET_LENGTH"))
                                .build());
                    }
                    columns.add(c.build());
                }
            }

            return Table.newBuilder().setName(originalTableName).addAllColumns(columns).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Maps SQL data types to custom data types.
     *
     * @param dataType The SQL data type.
     * @param typeName The SQL type name.
     * @return The custom data type.
     */
    static DataType mapDataTypes(Integer dataType, String typeName) {
        switch (typeName) {
            case "BYTEINT":
                return DataType.BOOLEAN;
            case "SMALLINT":
                return DataType.SHORT;
            case "INTEGER":
                return DataType.INT;
            case "BIGINT":
                return DataType.LONG;
            case "DECIMAL":
                return DataType.DECIMAL;
            case "FLOAT":
            case "DOUBLE PRECISION":
                return DataType.FLOAT;
            case "TIME":
                return DataType.NAIVE_TIME;
            case "DATE":
                return DataType.NAIVE_DATE;
            case "DATETIME":
            case "TIMESTAMP":
                return DataType.NAIVE_DATETIME;
            case "BIT":
            case "BINARY":
            case "VARBINARY":
            case "TINYBLOB":
            case "MEDIUMBLOB":
            case "BLOB":
            case "LONGBLOB":
                return DataType.BINARY;
            case "XML":
                return DataType.XML;
            case "CHAR":
            case "VARCHAR":
            case "TINYTEXT":
            case "MEDIUMTEXT":
            case "TEXT":
            case "LONGTEXT":
            case "GEOGRAPHYPOINT":
            case "GEOGRAPHY":
                return DataType.STRING;
            case "JSON":
                return DataType.JSON;
            default:
                return DataType.UNSPECIFIED;
        }
    }

    /**
     * Generates a SQL query to create a table.
     *
     * @param database The database name.
     * @param tableName The table name.
     * @param table The table metadata.
     * @return The SQL query to create the table.
     */
    static String generateCreateTableQuery(String database, String tableName, Table table) {
        String columnDefinitions = getColumnDefinitions(table.getColumnsList());
        return String.format("CREATE MULTISET TABLE %s (%s)", escapeTable(database, tableName), columnDefinitions);
    }

    /**
     * Generates a SQL query to create a table, including database creation if necessary.
     *
     * @param conf The Teradata configuration.
     * @param stmt The SQL statement.
     * @param request The create table request.
     * @return The SQL query to create the table.
     * @throws SQLException If a database access error occurs.
     */
    static String generateCreateTableQuery(TeradataConfiguration conf, Statement stmt, CreateTableRequest request) throws SQLException {
        String database = getDatabaseName(conf, request.getSchemaName());
        String table = getTableName(request.getSchemaName(), request.getTable().getName());

        if(database != null && table != null) {
            return generateCreateTableQuery(database, table, request.getTable());
        } else {
            return null;
        }
    }

    /**
     * Generates the column definitions for a create table query.
     *
     * @param columns The list of columns.
     * @return The column definitions.
     */
    public static String getColumnDefinitions(List<Column> columns) {
        List<String> columnsDefinitions = columns.stream().map(TeradataJDBCUtil::getColumnDefinition).collect(Collectors.toList());

        List<String> primaryKeyColumns = columns.stream().filter(Column::getPrimaryKey)
                .map(column -> escapeIdentifier(column.getName())).collect(Collectors.toList());

        if (!primaryKeyColumns.isEmpty()) {
            columnsDefinitions.add(String.format("PRIMARY KEY (%s)", String.join(", ", primaryKeyColumns)));
        }

        return String.join(",\n", columnsDefinitions);
    }

    /**
     * Generates the column definition for a single column.
     *
     * @param col The column.
     * @return The column definition.
     */
    static String getColumnDefinition(Column col) {
        String definition = String.format("%s %s", escapeIdentifier(col.getName()), mapDataTypes(col.getType(), col.getParams()));

        if (col.getPrimaryKey()) {
            definition += " NOT NULL";
        }
        return definition;
    }

    /**
     * Maps custom data types to SQL data types.
     *
     * @param type The custom data type.
     * @param params The data type parameters.
     * @return The SQL data type.
     */
    static String mapDataTypes(DataType type, DataTypeParams params) {
        switch (type) {
            case BOOLEAN:
                return "BYTEINT";
            case SHORT:
                return "SMALLINT";
            case INT:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case DECIMAL:
                if (params != null) {
                    return String.format("DECIMAL (%d, %d)", params.getDecimal().getPrecision(), Math.min(30, params.getDecimal().getScale()));
                }
                return "DECIMAL";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case NAIVE_TIME:
                return "TIME(0)";
            case NAIVE_DATE:
                return "DATE FORMAT 'YYYY-MM-DD'";
            case NAIVE_DATETIME:
            case UTC_DATETIME:
                return "TIMESTAMP(6)";
            case BINARY:
                return "BLOB";
            case JSON:
                return "JSON";
            case XML:
                return "XML";
            case UNSPECIFIED:
            case STRING:
            default:
                String varcharCharacterSet = Optional.ofNullable(TeradataConfiguration.varcharCharacterSet())
                    .map(String::toUpperCase)
                    .filter(cs -> cs.equals("LATIN") || cs.equals("UNICODE"))
                    .orElseGet(() -> {
                        String cs = TeradataConfiguration.varcharCharacterSet();
                        if (cs != null && !cs.isEmpty()) {

                            Logger.logMessage(Logger.LogLevel.WARNING, "Unsupported VARCHAR character set: " + cs + ". Defaulting to LATIN.");
                        }
                        return "LATIN";
                    });

                int defaultVarcharSize = TeradataConfiguration.defaultVarcharSize();

                if (params != null && params.getStringByteLength() != 0) {
                    int stringByteLength = params.getStringByteLength();
                    if (stringByteLength <= 256) {
                        return "VARCHAR(" + stringByteLength + ") CHARACTER SET " + varcharCharacterSet + " NOT CASESPECIFIC";
                    }
                    else {
                        return "VARCHAR(" + defaultVarcharSize + ") CHARACTER SET " + varcharCharacterSet + " NOT CASESPECIFIC";
                    }
                }
                return "VARCHAR(" + defaultVarcharSize + ") CHARACTER SET " + varcharCharacterSet + " NOT CASESPECIFIC";
        }
    }

    /**
     * Formats an ISO date-time string for use in SQL queries.
     *
     * @param dateTime The ISO date-time string.
     * @return The formatted date-time string.
     */
    public static String formatISODateTime(String dateTime) {
        dateTime = dateTime.replace("T", " ").replace("Z", "");
        int dotPos = dateTime.indexOf(".", 0);
        if (dotPos != -1 && dotPos + 6 < dateTime.length()) {
            return dateTime.substring(0, dotPos + 6 + 1);
        }
        return dateTime;
    }

    public static Timestamp getTimestampFromObject(Object object) {
        Timestamp obj = null;
        if (object == null)
            return obj;
        if (object instanceof java.sql.Timestamp) {
            obj = (Timestamp) object;
        } else if (object instanceof String) {
            obj = Timestamp.valueOf((String) object);
        }
        return obj;
    }

    public static final Object getLongFromTimestamp(Object object) {
        if (object == null) {
            return null;
        } else
            return (getTimestampFromObject(object)).getTime();
    }

    /**
     * Sets a parameter in a prepared statement.
     *
     * @param stmt The prepared statement.
     * @param id The parameter index.
     * @param type The data type of the parameter.
     * @param value The value of the parameter.
     * @param nullStr The string representing a null value.
     * @throws SQLException If a database access error occurs.
     */
    public static void setParameter(PreparedStatement stmt, Integer id, DataType type, String value,
                                    String nullStr) throws SQLException {
        if (value.equals(nullStr)) {
            stmt.setNull(id, Types.NULL);
        } else {
            switch (type) {
                case BOOLEAN:
                    if (value.equalsIgnoreCase("true")) {
                        stmt.setShort(id, Short.parseShort("1"));
                    } else if (value.equalsIgnoreCase("false")) {
                        stmt.setShort(id, Short.parseShort("0"));
                    } else {
                        stmt.setShort(id, Short.parseShort(value));
                    }
                    break;
                case SHORT:
                    stmt.setShort(id, Short.parseShort(value));
                    break;
                case INT:
                    stmt.setInt(id, Integer.parseInt(value));
                    break;
                case LONG:
                    stmt.setLong(id, Long.parseLong(value));
                    break;
                case DECIMAL:
                    stmt.setBigDecimal(id, new BigDecimal(value));
                case FLOAT:
                    stmt.setFloat(id, Float.parseFloat(value));
                    break;
                case DOUBLE:
                    stmt.setDouble(id, Double.parseDouble(value));
                    break;
                case NAIVE_TIME:
                    stmt.setTime(id, Time.valueOf(value));
                    break;
                case NAIVE_DATE:
                    stmt.setDate(id, Date.valueOf(value));
                    break;
                case NAIVE_DATETIME:
                case UTC_DATETIME:
                    stmt.setTimestamp(id, TeradataJDBCUtil.getTimestampFromObject(formatISODateTime(value)));
                    break;
                case BINARY:
                    stmt.setBytes(id, Base64.getDecoder().decode(value));
                    break;
                case XML:
                    SQLXML sqlxml =  stmt.getConnection().createSQLXML();
                    sqlxml.setString(value);
                    stmt.setSQLXML(id, sqlxml);
                    break;
                case STRING:
                    stmt.setString(id, value);
                    break;
                case JSON:
                    stmt.setObject(id, new JSONStruct("JSON", new Object[] {value}));
                case UNSPECIFIED:
                default:
                    stmt.setString(id, value);
                    break;
            }
        }
    }

    static private Set<String> pkColumnNames(Table table) {
        return table.getColumnsList().stream().filter(column -> column.getPrimaryKey())
                .map(column -> column.getName()).collect(Collectors.toSet());
    }

    static private boolean pkEquals(Table t1, Table t2) {
        return pkColumnNames(t1).equals(pkColumnNames(t2));
    }

    static <T> String generateAlterTableQuery(AlterTableRequest request, WarningHandler<T> warningHandler) throws Exception {
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());

        String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = TeradataJDBCUtil.getTableName(request.getSchemaName(), request.getTable().getName());

        Table oldTable = getTable(conf, database, table, request.getTable().getName(), warningHandler);
        Table newTable = request.getTable();
        boolean pkChanged = false;

        if (!pkEquals(oldTable, newTable)) {
            pkChanged = true;
        }

        Map<String, Column> oldColumns = oldTable.getColumnsList().stream()
                .collect(Collectors.toMap(Column::getName, Function.identity()));

        List<Column> columnsToAdd = new ArrayList<>();
        List<Column> columnsToChange = new ArrayList<>();
        List<Column> commonColumns = new ArrayList<>();

        for (Column column : newTable.getColumnsList()) {
            Column oldColumn = oldColumns.get(column.getName());
            if (oldColumn == null) {
                columnsToAdd.add(column);
            } else {
                commonColumns.add(column);
                String oldType = mapDataTypes(oldColumn.getType(), oldColumn.getParams());
                String newType = mapDataTypes(column.getType(), column.getParams());
                if (!oldType.equals(newType)) {
                    if (oldColumn.getPrimaryKey()) {
                        pkChanged = true;
                        continue;
                    }
                    columnsToChange.add(column);
                }
            }
        }

        if (pkChanged) {
            Logger.logMessage(Logger.LogLevel.INFO, "Alter table changes the key of the table. This operation is not supported by Teradata. The table will be recreated from scratch.");

            return generateRecreateTableQuery(database, table, newTable, commonColumns);
        } else {
            return generateAlterTableQuery(database, table, columnsToAdd, columnsToChange);
        }
    }

    static String generateRecreateTableQuery(String database, String tableName, Table table,
                                             List<Column> commonColumns) {
        String tmpTableName = tableName + "_alter_tmp";
        String columns = commonColumns.stream().map(column -> escapeIdentifier(column.getName()))
                .collect(Collectors.joining(", "));

        String createTable = generateCreateTableQuery(database, tmpTableName, table);
        String insertData = String.format("INSERT INTO %s (%s) SELECT %s FROM %s",
                escapeTable(database, tmpTableName), columns, columns,
                escapeTable(database, tableName));
        String dropTable = String.format("DROP TABLE %s", escapeTable(database, tableName));
        String renameTable = String.format("RENAME TABLE %s TO %s",
                escapeTable(database, tmpTableName), escapeTable(database, tableName));
        String join = String.join("; ", createTable, insertData, dropTable, renameTable);
        Logger.logMessage(Logger.LogLevel.INFO, "Prepared SQL statement: " + join);
        return join;
    }

    static String generateAlterTableQuery(String database, String table, List<Column> columnsToAdd,
                                          List<Column> columnsToChange) {
        if (columnsToAdd.isEmpty() && columnsToChange.isEmpty()) {
            return null;
        }

        StringBuilder query = new StringBuilder();

        for (Column column : columnsToChange) {
            String tmpColName = column.getName() + "_alter_tmp";
            query.append(String.format("ALTER TABLE %s ADD %s %s; ",
                    escapeTable(database, table), escapeIdentifier(tmpColName),
                    mapDataTypes(column.getType(), column.getParams())));
            query.append(
                    String.format("UPDATE %s SET %s = %s; ", escapeTable(database, table),
                            escapeIdentifier(tmpColName), escapeIdentifier(column.getName())));
            query.append(String.format("ALTER TABLE %s DROP %s; ", escapeTable(database, table),
                    escapeIdentifier(column.getName())));
            query.append(String.format("ALTER TABLE %s RENAME %s TO %s; ",
                    escapeTable(database, table), tmpColName, escapeIdentifier(column.getName())));
        }

        if (!columnsToAdd.isEmpty()) {
            List<String> addOperations = new ArrayList<>();

            columnsToAdd.forEach(column -> addOperations
                    .add(String.format("ADD %s", getColumnDefinition(column))));

            query.append(String.format("ALTER TABLE %s %s; ", escapeTable(database, table),
                    String.join(", ", addOperations)));
        }
        Logger.logMessage(Logger.LogLevel.INFO, "Prepared SQL statement: " + query.toString());
        return query.toString();
    }

    static String generateTruncateTableQuery(String database, String table,
                                             TruncateRequest request, String utcDeleteBefore) {
        String query;

        if (request.hasSoft()) {
            query = String.format("UPDATE %s SET %s = 1 ", escapeTable(database, table),
                    escapeIdentifier(request.getSoft().getDeletedColumn()));
        } else {
            query = String.format("DELETE FROM %s ", escapeTable(database, table));
        }

      query += String.format("WHERE %s < TO_TIMESTAMP('%s')",
          escapeIdentifier(request.getSyncedColumn()),
          utcDeleteBefore);
        Logger.logMessage(Logger.LogLevel.INFO, "Prepared SQL statement: " + query);
        return query;
    }

    public static Map<String, ColumnMetadata> getVarcharColumnLengths(Connection conn, String dbName, String tableName) {
        Map<String, ColumnMetadata> map = new HashMap<>();

        String query = "SELECT ColumnName, ColumnLength, CharType FROM DBC.ColumnsV " +
                "WHERE UPPER(DatabaseName) = UPPER(?) AND UPPER(TableName) = UPPER(?) AND ColumnType = 'CV'";
        Logger.logMessage(Logger.LogLevel.INFO,
                String.format("Prepared SQL statement for getting VarcharColumnLengths: %s", query));

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, dbName);
            pstmt.setString(2, tableName);

            try (ResultSet rs = pstmt.executeQuery()) {
                Logger.logMessage(Logger.LogLevel.INFO,
                        "--------------------------------------------------------------------------");
                while (rs.next()) {
                    String columnName = rs.getString("ColumnName");
                    int length = rs.getInt("ColumnLength");
                    int charType = rs.getInt("CharType");
                    if (charType == 2) { // If CharType is 2, it means the column is a Unicode character type
                        length /= 2; // Convert to byte length
                    }
                    Logger.logMessage(Logger.LogLevel.INFO,
                            String.format("Column: %s, Length: %d, CharType: %d", columnName, length, charType));

                    map.put(columnName, new ColumnMetadata(length, charType));
                }
                Logger.logMessage(Logger.LogLevel.INFO,
                        "--------------------------------------------------------------------------");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return map;
    }

    public static void resizeVarcharColumn(Connection conn,
                                           String database,
                                           String table,
                                           String temp_table,
                                           String columnName,
                                           int currentLength,
                                           int newLength) throws SQLException {
        String[] tables;
        if (temp_table == null) {
            tables = new String[]{table};
        } else {
            tables = new String[]{table, temp_table};
        }
        for (String tableName : tables) {
            String tmpColumn = columnName + "_tmp";
            try (Statement stmt = conn.createStatement()) {
                // 1. Add new temp column
                String addCol = String.format("ALTER TABLE %s.%s ADD %s VARCHAR(%d)", database, tableName, tmpColumn, newLength);
                stmt.executeUpdate(addCol);
                conn.commit();  // commit after DDL

                // 2. Copy data to temp column
                String copyData = String.format("UPDATE %s.%s SET %s = %s", database, tableName, tmpColumn, columnName);
                stmt.executeUpdate(copyData);

                // 3. Drop old column
                String dropOld = String.format("ALTER TABLE %s.%s DROP %s", database, tableName, columnName);
                stmt.executeUpdate(dropOld);
                conn.commit();  // commit after DDL

                // 4. Rename temp column to original name
                String renameCol = String.format("ALTER TABLE %s.%s RENAME %s TO %s", database, tableName, tmpColumn, columnName);
                stmt.executeUpdate(renameCol);
                conn.commit();  // commit after DDL
                Logger.logMessage(Logger.LogLevel.INFO,
                        String.format("Column '%s' in table '%s' resized to VARCHAR(%d) successfully.%n",
                                columnName, tableName, newLength));
            }
        }
    }

    /**
     * Escapes an identifier (e.g., table or column name) for use in SQL queries.
     *
     * @param ident The identifier.
     * @return The escaped identifier.
     */
    public static String escapeIdentifier(String ident) {
        return String.format("\"%s\"", ident.replace("`", "``"));
    }

    /**
     * Escapes a string for use in SQL queries.
     *
     * @param ident The string.
     * @return The escaped string.
     */
    public static String escapeString(String ident) {
        return String.format("'%s'", ident.replace("'", "''"));
    }

    /**
     * Escapes a table name for use in SQL queries.
     *
     * @param database The database name.
     * @param table The table name.
     * @return The escaped table name.
     */
    public static String escapeTable(String database, String table) {
        return escapeIdentifier(database) + "." + escapeIdentifier(table);
    }

    public static String getSingleValue(ResultSet resultSet) throws SQLException {
        if (resultSet.next()) {
            return resultSet.getString(1);
        }
        return null;
    }

    /**
     * Retrieves the database name from the configuration or schema.
     *
     * @param conf The Teradata configuration.
     * @param schema The schema name.
     * @return The database name.
     */
    public static String getDatabaseName(TeradataConfiguration conf, String schema) {
        return conf.database() != null ? conf.database() : schema;
    }

    /**
     * Retrieves the table name from the configuration or schema.
     *
     * @param schema The schema name.
     * @param table The table name.
     * @return The table name.
     */
    public static String getTableName(String schema, String table) {
        return schema + "_" + table;
    }
}