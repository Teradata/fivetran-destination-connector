package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.teradata.fivetran.destination.TeradataConfiguration;
import org.junit.jupiter.api.Test;
import com.google.common.collect.ImmutableMap;

public class TeradataConfigurationTest extends IntegrationTestBase {

    // Test to verify the default values of the TeradataConfiguration
    @Test
    public void defaultValues() throws Exception {
        // Create a TeradataConfiguration object with specified parameters
        TeradataConfiguration conf = new TeradataConfiguration(ImmutableMap.of("host", host, "user", user, "database", database, "password", password, "batch.size", "10000"));

        // Assert that the configuration values match the expected values
        assertEquals(host, conf.host());
        assertEquals(user, conf.user());
        assertEquals(database, conf.database());
        assertEquals(password, conf.password());
        assertEquals(10000, conf.batchSize());
    }
}