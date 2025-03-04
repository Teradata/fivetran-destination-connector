package com.teradata.fivetran.destination;

import fivetran_sdk.v2.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class TeradataJDBCUtil {
    private static final Logger logger = LoggerFactory.getLogger(TeradataJDBCUtil.class);

    /**
     * Creates a connection to the Teradata database using the provided configuration.
     *
     * @param conf The Teradata configuration.
     * @return A connection to the Teradata database.
     * @throws SQLException If a database access error occurs.
     * @throws ClassNotFoundException If the JDBC driver class is not found.
     */
    static Connection createConnection(TeradataConfiguration conf) throws SQLException, ClassNotFoundException {
        Properties connectionProps = new Properties();
        connectionProps.put("user", conf.user());
        if (conf.password() != null) {
            connectionProps.put("password", conf.password());
        }

        String url = String.format("jdbc:teradata://%s", conf.host());
        Class.forName("com.teradata.jdbc.TeraDriver");
        return DriverManager.getConnection(url, connectionProps);
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
     * @param originalTableName The original table name.
     * @return The table metadata.
     * @throws SQLException If a database access error occurs.
     * @throws ClassNotFoundException If the JDBC driver class is not found.
     * @throws TableNotExistException If the table does not exist.
     */
    static Table getTable(TeradataConfiguration conf, String database, String table, String originalTableName) throws SQLException, ClassNotFoundException, TableNotExistException {
        try (Connection conn = createConnection(conf)) {
            DatabaseMetaData metadata = conn.getMetaData();

            if (!tableExists(metadata, database, table)) {
                throw new TableNotExistException();
            }

            Set<String> primaryKeys = getPrimaryKeys(metadata, database, table);
            List<Column> columns = getColumns(metadata, database, table, primaryKeys);

            return Table.newBuilder().setName(originalTableName).addAllColumns(columns).build();
        }
    }

    /**
     * Checks if a table exists in the database metadata.
     *
     * @param metadata The database metadata.
     * @param database The database name.
     * @param table The table name.
     * @return True if the table exists, false otherwise.
     * @throws SQLException If a database access error occurs.
     */
    private static boolean tableExists(DatabaseMetaData metadata, String database, String table) throws SQLException {
        try (ResultSet tables = metadata.getTables(database, null, table, null)) {
            if (!tables.next()) {
                return false;
            }
            if (tables.next()) {
                logger.warn(String.format("Found several tables that match %s name", escapeTable(database, table)));
            }
            return true;
        }
    }

    /**
     * Retrieves the primary keys of a table.
     *
     * @param metadata The database metadata.
     * @param database The database name.
     * @param table The table name.
     * @return A set of primary key column names.
     * @throws SQLException If a database access error occurs.
     */
    private static Set<String> getPrimaryKeys(DatabaseMetaData metadata, String database, String table) throws SQLException {
        Set<String> primaryKeys = new HashSet<>();
        try (ResultSet primaryKeysRS = metadata.getPrimaryKeys(database, null, table)) {
            while (primaryKeysRS.next()) {
                primaryKeys.add(primaryKeysRS.getString("COLUMN_NAME"));
            }
        }
        return primaryKeys;
    }

    /**
     * Retrieves the columns of a table.
     *
     * @param metadata The database metadata.
     * @param database The database name.
     * @param table The table name.
     * @param primaryKeys The set of primary key column names.
     * @return A list of columns.
     * @throws SQLException If a database access error occurs.
     */
    private static List<Column> getColumns(DatabaseMetaData metadata, String database, String table, Set<String> primaryKeys) throws SQLException {
        List<Column> columns = new ArrayList<>();
        try (ResultSet columnsRS = metadata.getColumns(database, null, table, null)) {
            while (columnsRS.next()) {
                Column.Builder c = Column.newBuilder()
                        .setName(columnsRS.getString("COLUMN_NAME"))
                        .setType(mapDataTypes(columnsRS.getInt("DATA_TYPE"), columnsRS.getString("TYPE_NAME")))
                        .setPrimaryKey(primaryKeys.contains(columnsRS.getString("COLUMN_NAME")));
                if (c.getType() == DataType.DECIMAL) {
                    c.setParams(DataTypeParams.newBuilder()
                            .setDecimal(DecimalParams.newBuilder()
                                    .setScale(columnsRS.getInt("DECIMAL_DIGITS"))
                                    .setPrecision(columnsRS.getInt("COLUMN_SIZE"))
                                    .build())
                            .build());
                }
                columns.add(c.build());
            }
        }
        return columns;
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
            case "BOOLEAN":
                return DataType.BOOLEAN;
            case "SMALLINT":
                return DataType.SHORT;
            case "MEDIUMINT":
            case "INT":
                return DataType.INT;
            case "BIGINT":
                return DataType.LONG;
            case "FLOAT":
                return DataType.FLOAT;
            case "DOUBLE":
                return DataType.DOUBLE;
            case "DECIMAL":
                return DataType.DECIMAL;
            case "DATE":
            case "YEAR":
                return DataType.NAIVE_DATE;
            case "DATETIME":
            case "TIME":
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
        return String.format("CREATE Multiset TABLE %s (%s)", escapeTable(database, tableName), columnDefinitions);
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
        String table = getTableName(conf, request.getSchemaName(), request.getTable().getName());
        String createTableQuery = generateCreateTableQuery(database, table, request.getTable());

        if (!checkDatabaseExists(stmt, database)) {
            return String.format("CREATE DATABASE %s AS PERM = 2000000; %s", escapeIdentifier(database), createTableQuery);
        } else {
            return createTableQuery;
        }
    }

    /**
     * Generates the column definitions for a create table query.
     *
     * @param columns The list of columns.
     * @return The column definitions.
     */
    static String getColumnDefinitions(List<Column> columns) {
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
                return "FLOAT PRECISION";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case NAIVE_DATE:
                return "DATE";
            case NAIVE_DATETIME:
            case UTC_DATETIME:
                return "TIMESTAMP(6)";
            case BINARY:
                return "BLOB";
            case JSON:
                return "JSON";
            case UNSPECIFIED:
            case XML:
            case STRING:
            default:
                return "VARCHAR(64000)";
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
        // SingleStore doesn't support more than 6 digits after a period
        int dotPos = dateTime.indexOf(".", 0);
        if (dotPos != -1 && dotPos + 6 < dateTime.length()) {
            return dateTime.substring(0, dotPos + 6 + 1);
        }
        return dateTime;
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
                        stmt.setBoolean(id, true);
                    } else if (value.equalsIgnoreCase("false")) {
                        stmt.setBoolean(id, false);
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
                case FLOAT:
                    stmt.setFloat(id, Float.parseFloat(value));
                    break;
                case DOUBLE:
                    stmt.setDouble(id, Double.parseDouble(value));
                    break;
                case BINARY:
                    stmt.setBytes(id, Base64.getDecoder().decode(value));
                    break;

                case NAIVE_DATETIME:
                case UTC_DATETIME:
                    stmt.setString(id, formatISODateTime(value));
                    break;

                case DECIMAL:
                case NAIVE_DATE:
                case XML:
                case STRING:
                case JSON:
                case UNSPECIFIED:
                default:
                    stmt.setString(id, value);
                    break;
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

    /**
     * Exception thrown when a table does not exist.
     */
    static class TableNotExistException extends Exception {
        TableNotExistException() {
            super("Table doesn't exist");
        }
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
     * @param conf The Teradata configuration.
     * @param schema The schema name.
     * @param table The table name.
     * @return The table name.
     */
    public static String getTableName(TeradataConfiguration conf, String schema, String table) {
        return conf.database() != null ? table : table;
    }
}