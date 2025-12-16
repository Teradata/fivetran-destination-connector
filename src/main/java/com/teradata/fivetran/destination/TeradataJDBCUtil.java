package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.warning_util.WarningHandler;
import com.teradata.fivetran.destination.writers.ColumnMetadata;
import com.teradata.fivetran.destination.writers.JSONStruct;
import fivetran_sdk.v2.*;

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

    public static class QueryWithCleanup {
        private final String query;
        private final String cleanupQuery; // nullable
        private final String warningMessage;
        private final List<String> parameterValues = new ArrayList<>();
        private final List<DataType> parameterTypes = new ArrayList<>();


        public QueryWithCleanup(String query, String cleanupQuery, String warningMessage) {
            this.query = query;
            this.cleanupQuery = cleanupQuery;
            this.warningMessage = warningMessage;
        }

        public QueryWithCleanup addParameter(String value, DataType type) {
            parameterValues.add(value.toString());
            parameterTypes.add(type);
            return this;
        }

        public String getQuery() {
            return query;
        }

        public String getCleanupQuery() {
            return cleanupQuery;
        }

        public String getWarningMessage() {
            return warningMessage;
        }

        public void execute(Connection conn) throws SQLException {
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                for (int i = 0; i < parameterTypes.size(); i++) {
                    String value = parameterValues.get(i);
                    DataType type = parameterTypes.get(i);
                    TeradataJDBCUtil.setParameter(stmt, i + 1, type, value, "NULL");
                }
                stmt.execute();
            }
        }
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
                              String originalTableName, WarningHandler warningHandler) throws Exception {
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
                        return "VARCHAR(" + stringByteLength + ") CHARACTER SET " + varcharCharacterSet;
                    }
                    else {
                        return "VARCHAR(" + defaultVarcharSize + ") CHARACTER SET " + varcharCharacterSet;
                    }
                }
                return "VARCHAR(" + defaultVarcharSize + ") CHARACTER SET " + varcharCharacterSet;
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

    static <T> List generateAlterTableQuery(AlterTableRequest request, WarningHandler warningHandler) throws Exception {
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
        List<Column> columnsToDrop = new ArrayList<>();
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

        if (request.getDropColumns()) {
            Set<String> newColumnNames = newTable.getColumnsList().stream()
                    .map(Column::getName).collect(Collectors.toSet());

            for (Column column : oldTable.getColumnsList()) {
                if (!newColumnNames.contains(column.getName())) {
                    columnsToDrop.add(column);
                    if (column.getPrimaryKey()) {
                        pkChanged = true;
                    }
                }
            }
        }

        if (pkChanged) {
            Logger.logMessage(Logger.LogLevel.INFO, "Alter table changes the key of the table. This operation is not supported by Teradata. The table will be recreated from scratch.");

            return generateRecreateTableQuery(database, table, newTable, commonColumns);
        } else {
            return generateAlterTableQuery(database, table, columnsToAdd, columnsToChange, columnsToDrop);
        }
    }

    static <T> List generateRecreateTableQuery(String database, String tableName, Table table,
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

        return Arrays.asList(
                new QueryWithCleanup(createTable, null, null),
                new QueryWithCleanup(insertData, null, null),
                new QueryWithCleanup(dropTable, null, null),
                // The original table has been dropped; all data now resides in the temporary table.
                new QueryWithCleanup(renameTable, null,
                        String.format("Failed to recreate table %s with the new schema. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, tableName),
                                escapeTable(database, tmpTableName),
                                escapeTable(database, tmpTableName),
                                escapeTable(database, tableName)))
        );
    }

    static List<QueryWithCleanup> generateAlterTableQuery(
            String database,
            String table,
            List<Column> columnsToAdd,
            List<Column> columnsToChange,
            List<Column> columnsToDrop) {

        if (columnsToAdd.isEmpty() && columnsToChange.isEmpty()  && columnsToDrop.isEmpty()) {
            return null;
        }

        List<QueryWithCleanup> queries = new ArrayList<>();

        for (Column column : columnsToChange) {
            String tmpColName = column.getName() + "_alter_tmp";

            String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s %s",
                    escapeTable(database, table),
                    escapeIdentifier(tmpColName),
                    mapDataTypes(column.getType(), column.getParams())
            );

            String copyDataQuery = String.format(
                    "UPDATE %s SET %s = %s",
                    escapeTable(database, table),
                    escapeIdentifier(tmpColName),
                    escapeIdentifier(column.getName())
            );

            String dropColumnQuery = String.format(
                    "ALTER TABLE %s DROP %s",
                    escapeTable(database, table),
                    escapeIdentifier(column.getName())
            );

            String renameColumnQuery = String.format(
                    "ALTER TABLE %s RENAME %s TO %s",
                    escapeTable(database, table),
                    tmpColName,
                    escapeIdentifier(column.getName())
            );

            String cleanupTmpColumnQuery = String.format(
                    "ALTER TABLE %s DROP %s",
                    escapeTable(database, table),
                    escapeIdentifier(tmpColName)
            );

            // Order preserved exactly as original code
            queries.add(new QueryWithCleanup(addColumnQuery, null, null));
            queries.add(new QueryWithCleanup(copyDataQuery, cleanupTmpColumnQuery, null));
            queries.add(new QueryWithCleanup(dropColumnQuery, cleanupTmpColumnQuery, null));
            queries.add(new QueryWithCleanup(renameColumnQuery, null, null));
        }

        if (!columnsToAdd.isEmpty()) {
            List<String> addOperations = new ArrayList<>();

            columnsToAdd.forEach(column ->
                    addOperations.add(String.format("ADD %s", getColumnDefinition(column)))
            );

            String addColumnsQuery = String.format(
                    "ALTER TABLE %s %s",
                    escapeTable(database, table),
                    String.join(", ", addOperations)
            );

            queries.add(new QueryWithCleanup(addColumnsQuery, null, null));
        }

        if (!columnsToDrop.isEmpty()) {
            List<String> dropOperations = new ArrayList<>();

            columnsToDrop.forEach(column -> dropOperations
                    .add(String.format("DROP %s", escapeIdentifier(column.getName()))));

            String dropColumnsQuery = String.format("ALTER TABLE %s %s; ",
                    escapeTable(database, table), String.join(", ", dropOperations));
            queries.add(new QueryWithCleanup(dropColumnsQuery, null, null));
        }

        return queries;
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

    private static String getTempName(String originalName) {
        return originalName + "_tmp_" + Integer.toHexString(new Random().nextInt(0x1000000));
    }

    private static boolean checkTableNonEmpty(
            TeradataConfiguration conf, String database, String table) throws Exception {

        String query = String.format(
                "SELECT 1 FROM %s SAMPLE 1",
                escapeTable(database, table)
        );

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            return rs.next();
        }
    }

    static List<QueryWithCleanup> generateMigrateQueries(MigrateRequest request, WarningHandler warningHandler) throws Exception {
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());

        MigrationDetails details = request.getDetails();
        String database = getDatabaseName(conf, details.getSchema());
        String table = getTableName(details.getSchema(), details.getTable());

        Table t;
        switch (details.getOperationCase()) {
            case DROP:
                DropOperation drop = details.getDrop();
                switch (drop.getEntityCase()) {
                    case DROP_TABLE:
                        return generateMigrateDropQueries(table, database);
                    case DROP_COLUMN_IN_HISTORY_MODE:
                        DropColumnInHistoryMode dropColumnInHistoryMode = drop.getDropColumnInHistoryMode();

                        if (!checkTableNonEmpty(conf, database, table)) {
                            return new ArrayList<>();
                        }

                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateDropColumnInHistoryMode(drop.getDropColumnInHistoryMode(), t, database, table);
                    default:
                        throw new IllegalArgumentException("Unsupported drop operation");
                }
            case COPY:
                CopyOperation copy = details.getCopy();
                switch (copy.getEntityCase()) {
                    case COPY_TABLE:
                        CopyTable renameTableMigration = copy.getCopyTable();
                        String tableFrom =
                                getTableName(details.getSchema(), renameTableMigration.getFromTable());
                        String tableTo =
                                getTableName(details.getSchema(), renameTableMigration.getToTable());

                        return generateMigrateCopyTable(tableFrom, tableTo, database);
                    case COPY_COLUMN:
                        CopyColumn migration = copy.getCopyColumn();
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        Column c = t.getColumnsList().stream()
                                .filter(column -> column.getName().equals(migration.getFromColumn()))
                                .findFirst()
                                .orElseThrow(() -> new IllegalArgumentException("Source column doesn't exist"));

                        return generateMigrateCopyColumn(copy.getCopyColumn(), database, table, c);
                    case COPY_TABLE_TO_HISTORY_MODE:
                        CopyTableToHistoryMode copyTableToHistoryModeMigration = copy.getCopyTableToHistoryMode();
                        String tableFromHM =
                                getTableName(details.getSchema(), copyTableToHistoryModeMigration.getFromTable());
                        String tableToHM =
                                getTableName(details.getSchema(), copyTableToHistoryModeMigration.getToTable());

                        t = getTable(conf, database, tableFromHM, copyTableToHistoryModeMigration.getFromTable(), warningHandler);

                        return generateMigrateCopyTableToHistoryMode(t,
                                database, tableFromHM, tableToHM, copyTableToHistoryModeMigration.getSoftDeletedColumn());
                    default:
                        throw new IllegalArgumentException("Unsupported copy operation");
                }
            case RENAME:
                RenameOperation rename = details.getRename();
                switch (rename.getEntityCase()) {
                    case RENAME_TABLE:
                        RenameTable renameTableMigration = rename.getRenameTable();
                        String tableFrom =
                                getTableName(details.getSchema(), renameTableMigration.getFromTable());
                        String tableTo =
                                getTableName(details.getSchema(), renameTableMigration.getToTable());

                        return generateMigrateRenameTable(tableFrom, tableTo, database);
                    case RENAME_COLUMN:
                        return generateMigrateRenameColumn(rename.getRenameColumn(), database, table);
                    default:
                        throw new IllegalArgumentException("Unsupported rename operation");
                }
            case ADD:
                AddOperation add = details.getAdd();
                switch (add.getEntityCase()) {
                    case ADD_COLUMN_IN_HISTORY_MODE:
                        AddColumnInHistoryMode addColumnInHistoryMode = add.getAddColumnInHistoryMode();

                        boolean isEmpty = !checkTableNonEmpty(conf, database, table);

                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateAddColumnInHistoryMode(addColumnInHistoryMode, t, database, table, isEmpty);
                    case ADD_COLUMN_WITH_DEFAULT_VALUE:
                        return generateMigrateAddColumnWithDefaultValue(add.getAddColumnWithDefaultValue(), table, database);
                    default:
                        throw new IllegalArgumentException("Unsupported add operation");
                }
            case UPDATE_COLUMN_VALUE:
                UpdateColumnValueOperation updateColumnValue = details.getUpdateColumnValue();
                t = getTable(conf, database, table, details.getTable(), warningHandler);
                Column c = t.getColumnsList().stream()
                        .filter(column -> column.getName().equals(updateColumnValue.getColumn()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("Source column doesn't exist"));

                return generateMigrateUpdateColumnValueOperation(updateColumnValue, database, table, c.getType());
            case TABLE_SYNC_MODE_MIGRATION:
                TableSyncModeMigrationOperation tableSyncModeMigration = details.getTableSyncModeMigration();
                TableSyncModeMigrationType type = tableSyncModeMigration.getType();
                String softDeleteColumn = tableSyncModeMigration.getSoftDeletedColumn();
                Boolean keepDeletedRows = tableSyncModeMigration.getKeepDeletedRows();
                switch (type) {
                    case SOFT_DELETE_TO_LIVE:
                        return generateMigrateSoftDeleteToLive(database, table, softDeleteColumn);
                    case SOFT_DELETE_TO_HISTORY:
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateMigrateSoftDeleteToHistory(t, database, table, softDeleteColumn);
                    case HISTORY_TO_SOFT_DELETE:
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateMigrateHistoryToSoftDelete(t, database, table, softDeleteColumn);
                    case HISTORY_TO_LIVE:
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateMigrateHistoryToLive(t, database, table, keepDeletedRows);
                    case LIVE_TO_HISTORY:
                        t = getTable(conf, database, table, details.getTable(), warningHandler);
                        return generateMigrateLiveToHistory(t, database, table);
                    case LIVE_TO_SOFT_DELETE:
                        return generateMigrateLiveToSoftDelete(database, table, softDeleteColumn);
                    default:
                        throw new IllegalArgumentException("Unsupported table sync mode migration operation");
                }
            default:
                throw new IllegalArgumentException("Unsupported migration operation");
        }
    }

    static List<QueryWithCleanup> generateMigrateDropQueries(String table, String database) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateDropQueries: table=%s, database=%s", table, database));
        String query = String.format("DROP TABLE %s", escapeTable(database, table));
        return Collections.singletonList(new QueryWithCleanup(query, null, null));
    }

    static List<QueryWithCleanup> generateMigrateAddColumnWithDefaultValue(AddColumnWithDefaultValue migration, String table, String database) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateAddColumnWithDefaultValue: column=%s, table=%s, database=%s", migration.getColumn(), table, database));
        String column = migration.getColumn();
        DataType type = migration.getColumnType();
        String defaultValue = migration.getDefaultValue();
        String addColumnOnly  = String.format("ALTER TABLE %s ADD %s %s",
                escapeTable(database, table), escapeIdentifier(column),
                mapDataTypes(type, null));
        String updateValues = String.format("UPDATE %s SET %s = ?",
                escapeTable(database, table), escapeIdentifier(column), escapeIdentifier(column));

        QueryWithCleanup addOnly = new QueryWithCleanup(addColumnOnly, null, null);
        QueryWithCleanup update = new QueryWithCleanup(updateValues, null, null);

        update.addParameter(defaultValue, type);
        return Arrays.asList(addOnly, update);
    }

    static List<QueryWithCleanup> generateMigrateRenameTable(String tableFrom, String tableTo, String database) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateRenameTable: tableFrom=%s, tableTo=%s, database=%s", tableFrom, tableTo, database));
        String query = String.format("RENAME TABLE %s to %s", escapeTable(database, tableFrom), escapeTable(database, tableTo));
        return Collections.singletonList(new QueryWithCleanup(query, null, null));
    }

    static List<QueryWithCleanup> generateMigrateRenameColumn(RenameColumn migration, String database, String table) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateRenameColumn: fromColumn=%s, toColumn=%s, table=%s, database=%s", migration.getFromColumn(), migration.getToColumn(), table, database));
        String query = String.format(
                "ALTER TABLE %s RENAME %s TO %s",
                escapeTable(database, table),
                escapeIdentifier(migration.getFromColumn()),
                escapeIdentifier(migration.getToColumn())
        );
        return Collections.singletonList(new QueryWithCleanup(query, null, null));
    }

    static List<QueryWithCleanup> generateMigrateCopyColumn(CopyColumn migration,
                                                            String database,
                                                            String table,
                                                            Column c) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateCopyColumn: fromColumn=%s, toColumn=%s, table=%s, database=%s", migration.getFromColumn(), migration.getToColumn(), table, database));
        String fromColumn = migration.getFromColumn();
        String toColumn = migration.getToColumn();

        String addColumnQuery = String.format("ALTER TABLE %s ADD %s %s",
                escapeTable(database, table),
                escapeIdentifier(toColumn),
                mapDataTypes(c.getType(), c.getParams())
        );
        String copyDataQuery = String.format("UPDATE %s SET %s = %s",
                escapeTable(database, table),
                escapeIdentifier(toColumn),
                escapeIdentifier(fromColumn)
        );
        String dropColumnQuery = String.format("ALTER TABLE %s DROP %s",
                escapeTable(database, table),
                escapeIdentifier(toColumn)
        );

        return Arrays.asList(new QueryWithCleanup(addColumnQuery, null, null),
                new QueryWithCleanup(copyDataQuery, dropColumnQuery, null));
    }

    static List<QueryWithCleanup> generateMigrateCopyTable(String tableFrom, String tableTo, String database) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateCopyTable: tableFrom=%s, tableTo=%s, database=%s", tableFrom, tableTo, database));
        String query = String.format("CREATE TABLE %s AS (SELECT * FROM %s) WITH DATA", escapeTable(database, tableTo), escapeTable(database, tableFrom));
        return Collections.singletonList(new QueryWithCleanup(query, null, null));
    }

    static List<QueryWithCleanup> generateMigrateUpdateColumnValueOperation(UpdateColumnValueOperation migration, String database, String table, DataType type) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateUpdateColumnValueOperation: column=%s, value=%s, table=%s, database=%s", migration.getColumn(), migration.getValue(), table, database));
        String sql = String.format("UPDATE %s SET %s = ?",
                escapeTable(database, table),
                escapeIdentifier(migration.getColumn()));

        QueryWithCleanup query = new QueryWithCleanup(sql, null, null);
        query.addParameter(migration.getValue(), type);
        return Collections.singletonList(query);
    }

    static List<QueryWithCleanup> generateMigrateLiveToSoftDelete(String database,
                                                                  String table,
                                                                  String softDeleteColumn) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateLiveToSoftDelete: table=%s, softDeleteColumn=%s, database=%s", table, softDeleteColumn, database));
        String addColumnQuery = String.format("ALTER TABLE %s ADD %s BYTEINT",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn)
        );
        String copyDataQuery = String.format("UPDATE %s SET %s = 0",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn),
                escapeIdentifier(softDeleteColumn)
        );
        String dropColumnQuery = String.format("ALTER TABLE %s DROP %s",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn)
        );

        return Arrays.asList(new QueryWithCleanup(addColumnQuery, null, null),
                new QueryWithCleanup(copyDataQuery, dropColumnQuery, null));
    }

    static List<QueryWithCleanup> generateMigrateLiveToHistory(Table t,
                                                               String database,
                                                               String table) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateLiveToHistory: table=%s, database=%s", table, database));
        // SingleStore doesn't support adding PK columns, so the table needs to be recreated from scratch.
        String tempTableName = getTempName(table);
        Table tempTable = t.toBuilder()
                .setName(tempTableName)
                .addColumns(
                        Column.newBuilder()
                                .setName("_fivetran_start")
                                .setType(DataType.NAIVE_DATETIME)
                                .setPrimaryKey(true)
                )
                .addColumns(
                        Column.newBuilder()
                                .setName("_fivetran_end")
                                .setType(DataType.NAIVE_DATETIME)
                )
                .addColumns(
                        Column.newBuilder()
                                .setName("_fivetran_active")
                                .setType(DataType.BOOLEAN)
                ).build();
        String createTableQuery = generateCreateTableQuery(database, tempTableName, tempTable);
        String populateDataQuery = String.format("INSERT INTO %s SELECT src.*, CURRENT_TIMESTAMP(6) AS \"_fivetran_start\", TIMESTAMP '9999-12-31 23:59:59.999999' AS \"_fivetran_end\", 1 AS \"_fivetran_active\" FROM %s AS src;",
                escapeTable(database, tempTableName), escapeTable(database, table));
        String dropTableQuery = String.format("DROP TABLE %s", escapeTable(database, table));
        String renameTableQuery = String.format("RENAME TABLE %s TO %s", escapeTable(database, tempTableName), escapeIdentifier(table));

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(dropTableQuery, String.format("DROP TABLE %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(renameTableQuery, null,
                        String.format("Failed to migrate table %s to history mode. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, table),
                                escapeTable(database, tempTableName),
                                escapeTable(database, tempTableName),
                                escapeTable(database, table)))
        );
    }

    static List<QueryWithCleanup> generateMigrateSoftDeleteToHistory(Table t,
                                                                     String database,
                                                                     String table,
                                                                     String softDeleteColumn) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateSoftDeleteToHistory: table=%s, softDeleteColumn=%s, database=%s", table, softDeleteColumn, database));
        // SingleStore doesn't support adding PK columns, so the table needs to be recreated from scratch.
        List<Column> tempTableColumns = t.getColumnsList().stream()
                .filter(c -> !c.getName().equals(softDeleteColumn))
                .collect(Collectors.toList());
        tempTableColumns.add(
                Column.newBuilder()
                        .setName("_fivetran_start")
                        .setType(DataType.NAIVE_DATETIME)
                        .setPrimaryKey(true)
                        .build()
        );
        tempTableColumns.add(
                Column.newBuilder()
                        .setName("_fivetran_end")
                        .setType(DataType.NAIVE_DATETIME)
                        .build()
        );
        tempTableColumns.add(
                Column.newBuilder()
                        .setName("_fivetran_active")
                        .setType(DataType.BOOLEAN)
                        .build()
        );

        String tempTableName = getTempName(table);
        Table tempTable = Table.newBuilder()
                .setName(tempTableName)
                .addAllColumns(tempTableColumns)
                .build();
        String createTableQuery = generateCreateTableQuery(database, tempTableName, tempTable);
        String populateDataQuery = String.format(
                "INSERT INTO %s " +
                        "WITH _last_sync AS (SELECT MAX(_fivetran_synced) AS _last_sync FROM %s) " +
                        "SELECT %s, " +
                        "CASE WHEN %s = 1 THEN TIMESTAMP '1000-01-01 00:00:00.000000' " +
                        "     ELSE (SELECT _last_sync FROM _last_sync) END AS \"_fivetran_start\", " +
                        "CASE WHEN %s = 1 THEN TIMESTAMP '1000-01-01 00:00:00.000000' " +
                        "     ELSE TIMESTAMP '9999-12-31 23:59:59.999999' END AS \"_fivetran_end\", " +
                        "CASE WHEN %s = 1 THEN 0 ELSE 1 END AS \"_fivetran_active\" " +
                        "FROM %s",

                escapeTable(database, tempTableName),
                escapeTable(database, table),
                t.getColumnsList().stream()
                        .filter(c -> !c.getName().equals(softDeleteColumn))
                        .map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                escapeIdentifier(softDeleteColumn),
                escapeIdentifier(softDeleteColumn),
                escapeIdentifier(softDeleteColumn),
                escapeTable(database, table)
        );

        String dropTableQuery = String.format("DROP TABLE %s", escapeTable(database, table));
        String renameTableQuery = String.format("RENAME TABLE %s TO %s", escapeTable(database, tempTableName), escapeIdentifier(table));

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(dropTableQuery, String.format("DROP TABLE %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(renameTableQuery, null,
                        String.format("Failed to migrate table %s to soft delete mode. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, table),
                                escapeTable(database, tempTableName),
                                escapeTable(database, tempTableName),
                                escapeTable(database, table)))
        );
    }

    static List<QueryWithCleanup> generateMigrateSoftDeleteToLive(String database,
                                                                  String table,
                                                                  String softDeleteColumn) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateSoftDeleteToLive: table=%s, softDeleteColumn=%s, database=%s", table, softDeleteColumn, database));
        String deleteRows = String.format("DELETE FROM %s WHERE %s = 1",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn)
        );
        String dropColumnQuery = String.format("ALTER TABLE %s DROP %s",
                escapeTable(database, table),
                escapeIdentifier(softDeleteColumn)
        );

        return Arrays.asList(new QueryWithCleanup(deleteRows, null, null),
                new QueryWithCleanup(dropColumnQuery, null, null));
    }

    static List<QueryWithCleanup> generateMigrateHistoryToSoftDelete(Table t,
                                                                     String database,
                                                                     String table,
                                                                     String softDeletedColumn) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateHistoryToSoftDelete: table=%s, softDeletedColumn=%s, database=%s", table, softDeletedColumn, database));
        // SingleStore doesn't support adding PK columns, so the table needs to be recreated from scratch.
        String tempTableName = getTempName(table);
        List<Column> tempTableColumns = t.getColumnsList().stream()
                .filter(c ->
                        !c.getName().equals("_fivetran_start") &&
                                !c.getName().equals("_fivetran_end") &&
                                !c.getName().equals("_fivetran_active")
                )
                .collect(Collectors.toList());
        tempTableColumns.add(Column.newBuilder()
                .setName(softDeletedColumn)
                .setType(DataType.BOOLEAN)
                .build()
        );

        Table tempTable = Table.newBuilder()
                .setName(tempTableName)
                .addAllColumns(tempTableColumns).build();

        String createTableQuery = generateCreateTableQuery(database, tempTableName, tempTable);
        String populateDataQuery;
        if (tempTableColumns.stream().noneMatch(Column::getPrimaryKey)) {
            populateDataQuery = String.format(
                    "INSERT INTO %s " +
                            "SELECT %s, " +
                            "CASE WHEN _fivetran_active = 1 THEN 0 ELSE 1 END AS %s " +
                            "FROM %s",
                    escapeTable(database, tempTableName),
                    tempTableColumns.stream().filter(c -> !c.getName().equals(softDeletedColumn))
                            .map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                    escapeIdentifier(softDeletedColumn),
                    escapeTable(database, table)
            );
        } else {
            populateDataQuery = String.format(
                    "INSERT INTO %s " +
                    "SELECT ranked.%s, " +
                            "CASE WHEN ranked._fivetran_active = 1 THEN 0 ELSE 1 END AS %s " +
                            "FROM ( " +
                            "  SELECT t.*, " +
                            "         ROW_NUMBER() OVER (PARTITION BY t.%s ORDER BY t._fivetran_start DESC) AS rn " +
                            "  FROM %s AS t " +
                            ") AS ranked " +
                            "WHERE ranked.rn = 1",
                    escapeTable(database, tempTableName),
                    tempTableColumns.stream().filter(c -> !c.getName().equals(softDeletedColumn))
                            .map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                    escapeIdentifier(softDeletedColumn),
                    tempTableColumns.stream().filter(Column::getPrimaryKey)
                            .map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                    escapeTable(database, table)
            );
        }
        String dropTableQuery = String.format("DROP TABLE %s", escapeTable(database, table));
        String renameTableQuery = String.format("RENAME TABLE %s TO %s", escapeTable(database, tempTableName), escapeIdentifier(table));

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(dropTableQuery, String.format("DROP TABLE %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(renameTableQuery, null,
                        String.format("Failed to migrate table %s to soft delete mode. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, table),
                                escapeTable(database, tempTableName),
                                escapeTable(database, tempTableName),
                                escapeTable(database, table)))
        );
    }

    static List<QueryWithCleanup> generateMigrateHistoryToLive(Table t,
                                                               String database,
                                                               String table,
                                                               Boolean keep_deleted_rows) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateHistoryToLive: table=%s, keep_deleted_rows=%s, database=%s", table, keep_deleted_rows, database));
        // SingleStore doesn't support adding PK columns, so the table needs to be recreated from scratch.
        String tempTableName = getTempName(table);
        Table tempTable = Table.newBuilder()
                .setName(tempTableName)
                .addAllColumns(
                        t.getColumnsList().stream()
                                .filter(c ->
                                        !c.getName().equals("_fivetran_start") &&
                                                !c.getName().equals("_fivetran_end") &&
                                                !c.getName().equals("_fivetran_active")
                                )
                                .collect(Collectors.toList())
                ).build();

        String createTableQuery = generateCreateTableQuery(database, tempTableName, tempTable);
        String populateDataQuery = String.format("INSERT INTO %s " +
                        "SELECT %s " +
                        "FROM %s" +
                        "%s",
                escapeTable(database, tempTableName),
                tempTable.getColumnsList().stream().map(c -> escapeIdentifier(c.getName())).collect(Collectors.joining(", ")),
                escapeTable(database, table),
                keep_deleted_rows ? "" : " WHERE _fivetran_active = 1"
        );
        String dropTableQuery = String.format("DROP TABLE %s", escapeTable(database, table));
        String renameTableQuery = String.format("RENAME TABLE %s TO %s", escapeTable(database, tempTableName), escapeIdentifier(table));

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(dropTableQuery, String.format("DROP TABLE %s", escapeTable(database, tempTableName)), null),
                new QueryWithCleanup(renameTableQuery, null,
                        String.format("Failed to migrate table %s to live mode. All data has been preserved in the temporary table %s. To avoid data loss, please rename %s back to %s.",
                                escapeTable(database, table),
                                escapeTable(database, tempTableName),
                                escapeTable(database, tempTableName),
                                escapeTable(database, table)))
        );
    }

    static List<QueryWithCleanup> generateMigrateCopyTableToHistoryMode(Table t,
                                                                        String database,
                                                                        String fromTable,
                                                                        String toTable,
                                                                        String softDeleteColumn) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateMigrateCopyTableToHistoryMode: fromTable=%s, toTable=%s, softDeleteColumn=%s, database=%s", fromTable, toTable, softDeleteColumn, database));
        List<Column> newTableColumns = new ArrayList<>(t.getColumnsList());
        if (softDeleteColumn != null && !softDeleteColumn.isEmpty()) {
            newTableColumns = newTableColumns.stream()
                    .filter(c -> !c.getName().equals(softDeleteColumn))
                    .collect(Collectors.toList());
        }
        newTableColumns.add(
                Column.newBuilder()
                        .setName("_fivetran_start")
                        .setType(DataType.NAIVE_DATETIME)
                        .setPrimaryKey(true)
                        .build()
        );
        newTableColumns.add(
                Column.newBuilder()
                        .setName("_fivetran_end")
                        .setType(DataType.NAIVE_DATETIME)
                        .build()
        );
        newTableColumns.add(
                Column.newBuilder()
                        .setName("_fivetran_active")
                        .setType(DataType.BOOLEAN)
                        .build()
        );

        Table newTable = Table.newBuilder()
                .setName(toTable)
                .addAllColumns(newTableColumns)
                .build();

        String createTableQuery = generateCreateTableQuery(database, toTable, newTable);
        String populateDataQuery;
        if (softDeleteColumn == null || softDeleteColumn.isEmpty()) {
            populateDataQuery = String.format(
                    "INSERT INTO %s " +
                            "SELECT src.*, " +
                            "CURRENT_TIMESTAMP(6) AS \"%s\", " +
                            "TIMESTAMP '9999-12-31 23:59:59.999999' AS \"%s\", " +
                            "1 AS \"%s\" " +
                            "FROM %s AS src",
                    escapeTable(database, toTable),
                    "_fivetran_start",
                    "_fivetran_end",
                    "_fivetran_active",
                    escapeTable(database, fromTable)
            );

        } else {
            populateDataQuery = String.format(
                    "INSERT INTO %s " +
                            "WITH _last_sync AS (SELECT MAX(_fivetran_synced) AS _last_sync FROM %s) " +
                            "SELECT %s, " +
                            "CASE WHEN %s = 1 THEN TIMESTAMP '1000-01-01 00:00:00.000000' " +
                            "     ELSE (SELECT _last_sync FROM _last_sync) END AS \"%s\", " +
                            "CASE WHEN %s = 1 THEN TIMESTAMP '1000-01-01 00:00:00.000000' " +
                            "     ELSE TIMESTAMP '9999-12-31 23:59:59.999999' END AS \"%s\", " +
                            "CASE WHEN %s = 1 THEN 0 ELSE 1 END AS \"%s\" " +
                            "FROM %s",
                    escapeTable(database, toTable),
                    escapeTable(database, fromTable),
                    // SELECT column list excluding the soft delete column
                    t.getColumnsList().stream()
                            .filter(c -> !c.getName().equals(softDeleteColumn))
                            .map(c -> escapeIdentifier(c.getName()))
                            .collect(Collectors.joining(", ")),
                    // soft delete column comparison
                    escapeIdentifier(softDeleteColumn),
                    "_fivetran_start",
                    escapeIdentifier(softDeleteColumn),
                    "_fivetran_end",
                    escapeIdentifier(softDeleteColumn),
                    "_fivetran_active",
                    escapeTable(database, fromTable)
            );
        }

        return Arrays.asList(
                new QueryWithCleanup(createTableQuery, null, null),
                new QueryWithCleanup(populateDataQuery, String.format("DROP TABLE %s", escapeTable(database, toTable)), null)
        );
    }

    static List<QueryWithCleanup> generateDropColumnInHistoryMode(DropColumnInHistoryMode migration, Table t, String database, String table) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateDropColumnInHistoryMode: column=%s, table=%s, database=%s", migration.getColumn(), table, database));
        String column = migration.getColumn();
        String operationTimestamp = migration.getOperationTimestamp();

        QueryWithCleanup deleteQuery = new QueryWithCleanup(String.format("DELETE FROM %s WHERE _fivetran_start > ? AND _fivetran_active = 1",
                escapeTable(database, table)
        ), null, null)
                .addParameter(operationTimestamp, DataType.NAIVE_DATETIME);

        QueryWithCleanup insertQuery = new QueryWithCleanup(String.format("INSERT INTO %s (%s, %s, _fivetran_start) " +
                        "SELECT %s, NULL as %s, ? AS _fivetran_start " +
                        "FROM %s " +
                        "WHERE _fivetran_active = 1 AND %s IS NOT NULL AND _fivetran_start < ?",
                escapeTable(database, table),
                t.getColumnsList().stream()
                        .filter(c -> !c.getName().equals(column) && !c.getName().equals("_fivetran_start"))
                        .map(c -> escapeIdentifier(c.getName()))
                        .collect(Collectors.joining(", ")),
                escapeIdentifier(column),
                t.getColumnsList().stream()
                        .filter(c -> !c.getName().equals(column) && !c.getName().equals("_fivetran_start"))
                        .map(c -> escapeIdentifier(c.getName()))
                        .collect(Collectors.joining(", ")),
                escapeIdentifier(column),
                escapeTable(database, table),
                escapeIdentifier(column)
        ), null, null)
                .addParameter(operationTimestamp, DataType.NAIVE_DATETIME)
                .addParameter(operationTimestamp, DataType.NAIVE_DATETIME);

        QueryWithCleanup updateQuery = new QueryWithCleanup(String.format("UPDATE %s " +
                        "SET _fivetran_end = (CAST(? AS TIMESTAMP(6)) - INTERVAL '0.000001' SECOND), _fivetran_active = 0 " +
                        "WHERE _fivetran_active =1 AND %s IS NOT NULL AND _fivetran_start < ?",
                escapeTable(database, table),
                escapeIdentifier(column)
        ), null, null);
        updateQuery.addParameter(operationTimestamp, DataType.NAIVE_DATETIME);
        updateQuery.addParameter(operationTimestamp, DataType.NAIVE_DATETIME);

        return Arrays.asList(deleteQuery, insertQuery, updateQuery);
    }

    static List<QueryWithCleanup> generateAddColumnInHistoryMode(AddColumnInHistoryMode migration, Table t, String database, String table, boolean isEmptyTable) {
        Logger.logMessage(Logger.LogLevel.INFO, String.format("In generateAddColumnInHistoryMode: column=%s, table=%s, database=%s, isEmptyTable=%s", migration.getColumn(), table, database, isEmptyTable));
        String column = migration.getColumn();
        DataType columnType = migration.getColumnType();
        String defaultValue = migration.getDefaultValue();
        String operationTimestamp = migration.getOperationTimestamp();

        QueryWithCleanup alterTableQuery = new QueryWithCleanup(
                String.format("ALTER TABLE %s ADD %s %s",
                        escapeTable(database, table),
                        escapeIdentifier(column),
                        mapDataTypes(columnType, null)
                ),
                null, null);

        if (isEmptyTable) {
            return Collections.singletonList(alterTableQuery);
        }

        String dropColumnCleanup = String.format("ALTER TABLE %s DROP %s", escapeTable(database, table), escapeIdentifier(column));

        QueryWithCleanup insertQuery = new QueryWithCleanup(String.format("INSERT INTO %s (%s, %s, _fivetran_start) " +
                        "SELECT %s, CAST(? AS %s) AS %s, ? AS _fivetran_start " +
                        "FROM %s " +
                        "WHERE _fivetran_active = 1 " +
                        "AND _fivetran_start < ?",
                escapeTable(database, table),
                t.getColumnsList().stream()
                        .filter(c -> !c.getName().equals(column) && !c.getName().equals("_fivetran_start"))
                        .map(c -> escapeIdentifier(c.getName()))
                        .collect(Collectors.joining(", ")),
                escapeIdentifier(column),
                t.getColumnsList().stream()
                        .filter(c -> !c.getName().equals(column) && !c.getName().equals("_fivetran_start"))
                        .map(c -> escapeIdentifier(c.getName()))
                        .collect(Collectors.joining(", ")),
                mapDataTypes(columnType, null),
                escapeIdentifier(column),
                escapeTable(database, table)
        ), dropColumnCleanup, null)
                .addParameter(defaultValue, columnType)
                .addParameter(operationTimestamp, DataType.NAIVE_DATETIME)
                .addParameter(operationTimestamp, DataType.NAIVE_DATETIME);

        QueryWithCleanup updateQuery = new QueryWithCleanup(
                String.format(
                        "UPDATE %s " +
                                "SET _fivetran_end = " +
                                "(CAST(? AS TIMESTAMP(6)) - INTERVAL '0.000001' SECOND), " +
                                "_fivetran_active = 0 " +
                                "WHERE _fivetran_active = 1 " +
                                "AND _fivetran_start < ?",
                        escapeTable(database, table)
                ),
                dropColumnCleanup,
                null
        );
        updateQuery.addParameter(operationTimestamp, DataType.NAIVE_DATETIME);
        updateQuery.addParameter(operationTimestamp, DataType.NAIVE_DATETIME);

        return Arrays.asList(alterTableQuery, insertQuery, updateQuery);
    }

}

