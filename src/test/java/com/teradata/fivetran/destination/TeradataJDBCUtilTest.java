package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import java.sql.Connection;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

public class TeradataJDBCUtilTest extends IntegrationTestBase {

    // Test to verify the driver parameters and connection
    @Test
    public void driverParameters() throws Exception {
        // Create a TeradataConfiguration object with specified parameters
        TeradataConfiguration conf = new TeradataConfiguration(ImmutableMap.of("host", host, "user", user, "td2password", password, "database", database, "logmech", logmech, "tmode", tmode));

        // Establish a connection and execute a query
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.executeQuery("SELECT 1; SELECT 2");
        }
    }
}