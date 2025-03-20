package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import java.sql.Connection;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

public class TeradataJDBCUtilTest extends IntegrationTestBase {
    @Test
    public void driverParameters() throws Exception {
        TeradataConfiguration conf = new TeradataConfiguration(ImmutableMap.of("host", host, "user", user, "password", password, "database", database));
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement();) {
            stmt.executeQuery("SELECT 1; SELECT 2");
        }
    }
}