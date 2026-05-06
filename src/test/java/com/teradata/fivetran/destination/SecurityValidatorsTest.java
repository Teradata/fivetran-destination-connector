package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.writers.Writer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Unit tests for the security validators added in ticket IDE-26066 —
// no Teradata instance required.
public class SecurityValidatorsTest {

    // -- Host validator --------------------------------------------------

    @Test
    void hostValidator_acceptsValidHosts() {
        assertDoesNotThrow(() -> TeradataJDBCUtil.validateHost("10.25.56.44"));
        assertDoesNotThrow(() -> TeradataJDBCUtil.validateHost("10.25.56.44:1025"));
        assertDoesNotThrow(() -> TeradataJDBCUtil.validateHost("teradata.example.com"));
        assertDoesNotThrow(() -> TeradataJDBCUtil.validateHost("td-prod.example.com:1025"));
        assertDoesNotThrow(() -> TeradataJDBCUtil.validateHost("localhost"));
        assertDoesNotThrow(() -> TeradataJDBCUtil.validateHost("[::1]:1025"));
    }

    @Test
    void hostValidator_rejectsNullOrEmpty() {
        assertThrows(IllegalArgumentException.class, () -> TeradataJDBCUtil.validateHost(null));
        assertThrows(IllegalArgumentException.class, () -> TeradataJDBCUtil.validateHost(""));
    }

    @Test
    void hostValidator_rejectsControlAndSeparatorChars() {
        assertThrows(IllegalArgumentException.class, () -> TeradataJDBCUtil.validateHost("host\nevil"));
        assertThrows(IllegalArgumentException.class, () -> TeradataJDBCUtil.validateHost("host\revil"));
        assertThrows(IllegalArgumentException.class, () -> TeradataJDBCUtil.validateHost("host;DROP TABLE"));
        assertThrows(IllegalArgumentException.class, () -> TeradataJDBCUtil.validateHost("host with spaces"));
        assertThrows(IllegalArgumentException.class, () -> TeradataJDBCUtil.validateHost("file:///etc/passwd"));
        assertThrows(IllegalArgumentException.class, () -> TeradataJDBCUtil.validateHost("host/../../other"));
    }

    // -- driver.parameters token validator -------------------------------

    @Test
    void driverParameterToken_acceptsPlainValues() {
        assertDoesNotThrow(() -> TeradataJDBCUtil.validateDriverParameterToken("CHARSET"));
        assertDoesNotThrow(() -> TeradataJDBCUtil.validateDriverParameterToken("UTF8"));
    }

    @Test
    void driverParameterToken_rejectsNewlineCarriageReturnSemicolon() {
        assertThrows(IllegalArgumentException.class,
                () -> TeradataJDBCUtil.validateDriverParameterToken("key\nextra"));
        assertThrows(IllegalArgumentException.class,
                () -> TeradataJDBCUtil.validateDriverParameterToken("key\rextra"));
        assertThrows(IllegalArgumentException.class,
                () -> TeradataJDBCUtil.validateDriverParameterToken("key;other"));
    }

    // -- Batch file path validator ---------------------------------------

    @Test
    void batchFilePath_acceptsReadableFile(@TempDir Path tmp) throws IOException {
        Path f = Files.createFile(tmp.resolve("batch.csv"));
        Files.writeString(f, "header\nrow1\n");
        assertDoesNotThrow(() -> Writer.validateBatchFilePath(f.toString()));
    }

    @Test
    void batchFilePath_rejectsNullOrNullByte() {
        assertThrows(IllegalArgumentException.class, () -> Writer.validateBatchFilePath(null));
        assertThrows(IllegalArgumentException.class,
                () -> Writer.validateBatchFilePath("/tmp/batch\0/etc/passwd"));
    }

    @Test
    void batchFilePath_rejectsNonExistentOrDirectory(@TempDir Path tmp) {
        Path missing = tmp.resolve("does-not-exist.csv");
        assertThrows(IllegalArgumentException.class,
                () -> Writer.validateBatchFilePath(missing.toString()));
        assertThrows(IllegalArgumentException.class,
                () -> Writer.validateBatchFilePath(tmp.toString()));
    }

    // -- escapeIdentifier ------------------------------------------------

    @Test
    void escapeIdentifier_wrapsInDoubleQuotes() {
        assertEquals("\"col\"", TeradataJDBCUtil.escapeIdentifier("col"));
    }

    @Test
    void escapeIdentifier_doublesEmbeddedDoubleQuote() {
        // Identifier a"b must not be able to break out of the wrapping quotes.
        assertEquals("\"a\"\"b\"", TeradataJDBCUtil.escapeIdentifier("a\"b"));
        // Full injection attempt: a"; DROP TABLE x; --
        assertEquals("\"a\"\"; DROP TABLE x; --\"",
                TeradataJDBCUtil.escapeIdentifier("a\"; DROP TABLE x; --"));
    }

    @Test
    void escapeIdentifier_doublesEmbeddedBacktick() {
        // Legacy backtick escaping kept for backward compatibility.
        assertEquals("\"a``b\"", TeradataJDBCUtil.escapeIdentifier("a`b"));
    }
}
