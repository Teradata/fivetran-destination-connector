package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.writers.Writer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServiceUtilTest {

    @Test
    public void testGetStackTraceOneLine() {
        Exception ex = new RuntimeException("Test Exception");
        String stackTrace = ServiceUtil.getStackTraceOneLine(ex);
        assertTrue(stackTrace.contains("java.lang.RuntimeException: Test Exception"));
        assertTrue(
                stackTrace.contains("at com.teradata.fivetran.destination.ServiceUtilTest.testGetStackTraceOneLine"));
    }

    @Test
    public void testProcessFiles() throws Exception {
        MockWriter writer = new MockWriter();
        List<String> files = Arrays.asList("file1", "file2");

        ServiceUtil.processFiles(writer, files, "Test Message");

        assertEquals(2, writer.writtenFiles.size());
        assertEquals("file1", writer.writtenFiles.get(0));
        assertEquals("file2", writer.writtenFiles.get(1));
    }

    private static class MockWriter extends Writer {
        public List<String> writtenFiles = new ArrayList<>();

        public MockWriter() {
            super(null, null, null, null, null, null, null);
        }

        @Override
        public void write(String file) throws Exception {
            writtenFiles.add(file);
        }

        @Override
        public void setHeader(List<String> header) {
        }

        @Override
        public void writeRow(List<String> row) throws Exception {
        }

        @Override
        public void commit() throws java.io.IOException, java.sql.SQLException, InterruptedException {
        }
    }
}
