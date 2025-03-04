package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import com.teradata.fivetran.destination.warning_util.WarningHandler;
import fivetran_sdk.v2.AlterTableResponse;
import org.junit.jupiter.api.BeforeAll;

import com.google.common.collect.ImmutableMap;

public class IntegrationTestBase {
    static String host = "127.0.0.1";
    static String port = "3306";
    static String user = "root";
    static String password = System.getenv("ROOT_PASSWORD");
    static String database = "db";

    static ImmutableMap<String, String> confMap =
            ImmutableMap.of("host", host, "port", port, "user", user, "password", password);
    static TeradataConfiguration conf = new TeradataConfiguration(confMap);

    static List<String> allTypesColumns = Arrays.asList("id", "boolColumn", "booleanColumn", "bitColumn",
            "tinyintColumn", "smallintColumn", "mediumintColumn", "intColumn", "integerColumn",
            "bigintColumn", "floatColumn", "doubleColumn", "realColumn", "dateColumn", "timeColumn",
            "time6Column", "datetimeColumn", "datetime6Column", "timestampColumn",
            "timestamp6Column", "yearColumn", "decimalColumn", "decColumn", "fixedColumn",
            "numericColumn", "charColumn", "mediumtextColumn", "binaryColumn", "varcharColumn",
            "varbinaryColumn", "longtextColumn", "textColumn", "tinytextColumn", "longblobColumn",
            "mediumblobColumn", "blobColumn", "tinyblobColumn", "jsonColumn", "geographyColumn",
            "geographypointColumn");

    void createAllTypesTable() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE `%s`", database));
            stmt.execute("CREATE ROWSTORE TABLE `allTypesTable` (\n" + "  `id` INTEGER,\n"
                    + "  `boolColumn` BOOL,\n" + "  `booleanColumn` BOOLEAN,\n"
                    + "  `bitColumn` BIT(64),\n" + "  `tinyintColumn` TINYINT,\n"
                    + "  `smallintColumn` SMALLINT,\n" + "  `mediumintColumn` MEDIUMINT,\n"
                    + "  `intColumn` INT,\n" + "  `integerColumn` INTEGER,\n"
                    + "  `bigintColumn` BIGINT,\n" + "  `floatColumn` FLOAT,\n"
                    + "  `doubleColumn` DOUBLE,\n" + "  `realColumn` REAL,\n"
                    + "  `dateColumn` DATE,\n" + "  `timeColumn` TIME,\n"
                    + "  `time6Column` TIME(6),\n" + "  `datetimeColumn` DATETIME,\n"
                    + "  `datetime6Column` DATETIME(6),\n" + "  `timestampColumn` TIMESTAMP,\n"
                    + "  `timestamp6Column` TIMESTAMP(6),\n" + "  `yearColumn` YEAR,\n"
                    + "  `decimalColumn` DECIMAL(65, 30),\n" + "  `decColumn` DEC,\n"
                    + "  `fixedColumn` FIXED,\n" + "  `numericColumn` NUMERIC,\n"
                    + "  `charColumn` CHAR,\n" + "  `mediumtextColumn` MEDIUMTEXT,\n"
                    + "  `binaryColumn` BINARY,\n" + "  `varcharColumn` VARCHAR(100),\n"
                    + "  `varbinaryColumn` VARBINARY(100),\n" + "  `longtextColumn` LONGTEXT,\n"
                    + "  `textColumn` TEXT,\n" + "  `tinytextColumn` TINYTEXT,\n"
                    + "  `longblobColumn` LONGBLOB,\n" + "  `mediumblobColumn` MEDIUMBLOB,\n"
                    + "  `blobColumn` BLOB,\n" + "  `tinyblobColumn` TINYBLOB,\n"
                    + "  `jsonColumn` JSON,\n" + "  `geographyColumn` GEOGRAPHY,\n"
                    + "  `geographypointColumn` GEOGRAPHYPOINT,\n" + "  PRIMARY KEY (`id`)\n"
                    + ") ");
        }
    }

    @BeforeAll
    static void init() throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.executeQuery(String.format("DROP DATABASE IF EXISTS `%s`", database));
            stmt.executeQuery(String.format("CREATE DATABASE `%s`", database));
        }
    }

    void checkResult(String query, List<List<String>> expected) throws Exception {
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("USE `%s`", database));
            try (ResultSet rs = stmt.executeQuery(query)) {
                for (List<String> row : expected) {
                    assertTrue(rs.next());
                    for (int i = 0; i < row.size(); i++) {
                        assertEquals(row.get(i), rs.getString(i + 1));
                    }
                }
                assertFalse(rs.next());
            }
        }
    }


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