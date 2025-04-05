package com.teradata.fivetran.destination;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import com.teradata.fivetran.destination.warning_util.WarningHandler;
import fivetran_sdk.v2.AlterTableResponse;
import org.junit.jupiter.api.BeforeAll;

import com.google.common.collect.ImmutableMap;

import static org.junit.jupiter.api.Assertions.*;

public class IntegrationTestBase {
    // Database connection configuration
    static String host = System.getenv("TERADATA_HOST");
    static String user = System.getenv("TERADATA_USER");
    static String password = System.getenv("TERADATA_PASSWORD");
    static String database = System.getenv("TERADATA_DATABASE");
    static String schema = System.getenv("TERADATA_SCHEMA");
    static String logmech = System.getenv("TERADATA_LOGMECH");
    static String tmode = System.getenv("TERADATA_TMODE");

    // Immutable map to hold the configuration
    static ImmutableMap<String, String> confMap =
            ImmutableMap.of("host", host, "user", user, "td2password", password, "database", database, "logmech", logmech, "tmode", tmode);
    static TeradataConfiguration conf = new TeradataConfiguration(confMap);

    // List of all column names for the allTypesTable
    static List<String> allTypesColumns = Arrays.asList(
            "id", "byteintColumn", "smallintColumn", "bigintColumn", "decimalColumn",
            "floatColumn", "doubleColumn", "dateColumn", "timestampColumn", "blobColumn",
            "jsonColumn", "xmlColumn", "varcharColumn"
    );

    // Method to create a table with various data types
    void createAllTypesTable() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + conf.database() + ".allTypesTable (\n" +
                    "  id INTEGER PRIMARY KEY NOT NULL,\n" +
                    "  byteintColumn BYTEINT,\n" +
                    "  smallintColumn SMALLINT,\n" +
                    "  bigintColumn BIGINT,\n" +
                    "  decimalColumn DECIMAL(18,4),\n" +
                    "  floatColumn FLOAT,\n" +
                    "  doubleColumn DOUBLE PRECISION,\n" +
                    "  dateColumn DATE,\n" +
                    "  timestampColumn TIMESTAMP,\n" +
                    "  blobColumn BLOB,\n" +
                    "  jsonColumn JSON,\n" +
                    "  xmlColumn XML,\n" +
                    "  varcharColumn VARCHAR(100)\n" +
                    ")");
        }
    }

    // Method to initialize the database before all tests
    @BeforeAll
    static void init() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            try {
                // Delete and drop the database if it exists
                stmt.execute(String.format("DELETE DATABASE %s", database));
                stmt.execute(String.format("DROP DATABASE %s", database));
            } catch (SQLException e) {
                if (e.getErrorCode() != 3802) { // 3802: Database does not exist
                    throw e;
                }
            }
            // Create a new database
            stmt.execute(String.format("CREATE DATABASE %s AS PERM = 10000000", database));
        }
    }

    // Method to check the result of a query against expected values
    void checkResult(String query, List<List<String>> expected) throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                for (List<String> row : expected) {
                    assertTrue(rs.next());
                    for (int i = 0; i < row.size(); i++) {
                        if (rs.getMetaData().getColumnType(i + 1) == java.sql.Types.BLOB) {
                            assertArrayEquals(Base64.getDecoder().decode(row.get(i)), rs.getBytes(i + 1));
                        } else {
                            assertEquals(row.get(i), rs.getString(i + 1));
                        }
                    }
                }
                assertFalse(rs.next());
            }
        }
    }

    // Warning handler for alter table responses
    WarningHandler<AlterTableResponse> testWarningHandle = new WarningHandler<AlterTableResponse>(null) {
        @Override
        public void handle(String message) {
        }

        @Override
        public void handle(String message, Throwable t) {

        }

        @Override
        public AlterTableResponse createWarning(String message) {
            return null;
        }
    };
}