package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TeradataJDBCUtil.formatISODateTime() to verify proper fractional second padding.
 */
public class FormatISODateTimeTest {

    @Test
    public void formatISODateTimeWithSixDecimalPlaces() {
        String result = TeradataJDBCUtil.formatISODateTime("2026-06-15T09:36:10.527000Z");
        assertEquals("2026-06-15 09:36:10.527000", result);
    }

    @Test
    public void formatISODateTimeWithThreeDecimalPlaces() {
        String result = TeradataJDBCUtil.formatISODateTime("2026-06-15T09:36:10.527Z");
        assertEquals("2026-06-15 09:36:10.527000", result);
    }

    @Test
    public void formatISODateTimeWithOneDecimalPlace() {
        String result = TeradataJDBCUtil.formatISODateTime("2026-06-15T09:36:10.5Z");
        assertEquals("2026-06-15 09:36:10.500000", result);
    }

    @Test
    public void formatISODateTimeWithNoDecimalPlaces() {
        String result = TeradataJDBCUtil.formatISODateTime("2026-06-15T09:36:10Z");
        assertEquals("2026-06-15 09:36:10.000000", result);
    }

    @Test
    public void formatISODateTimeWithMoreThanSixDecimalPlaces() {
        String result = TeradataJDBCUtil.formatISODateTime("2026-06-15T09:36:10.123456789Z");
        assertEquals("2026-06-15 09:36:10.123456", result);
    }

    @Test
    public void formatISODateTimeWithTwoDecimalPlaces() {
        String result = TeradataJDBCUtil.formatISODateTime("2026-06-15T09:36:10.52Z");
        assertEquals("2026-06-15 09:36:10.520000", result);
    }
}
