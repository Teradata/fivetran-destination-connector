package com.teradata.fivetran.destination.writers;

import com.github.luben.zstd.ZstdInputStream;
import com.google.protobuf.ByteString;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.Compression;
import fivetran_sdk.v2.FileParams;
import fivetran_sdk.v2.Encryption;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public abstract class Writer {

    protected Connection conn;
    protected String database;
    protected String table;
    protected List<Column> columns;
    protected FileParams params;
    protected Map<String, ByteString> secretKeys;
    protected Integer batchSize;

    /**
     * Constructor for Writer.
     *
     * @param conn       The database connection.
     * @param database   The database name.
     * @param table      The table name.
     * @param columns    The list of columns.
     * @param params     The file parameters.
     * @param secretKeys The map of secret keys.
     * @param batchSize  The batch size for writing rows.
     */
    public Writer(Connection conn, String database, String table, List<Column> columns,
                  FileParams params, Map<String, ByteString> secretKeys, Integer batchSize) {
        this.conn = conn;
        this.database = database;
        this.table = table;
        this.columns = columns;
        this.params = params;
        this.secretKeys = secretKeys;
        this.batchSize = batchSize;
    }

    /**
     * Sets the header for the CSV file.
     *
     * @param header The list of header columns.
     * @throws SQLException, IOException If an error occurs while setting the header.
     */
    public abstract void setHeader(List<String> header) throws SQLException, IOException;

    /**
     * Writes a row to the database.
     *
     * @param row The list of row values.
     * @throws Exception If an error occurs while writing the row.
     */
    public abstract void writeRow(List<String> row) throws Exception;

    /**
     * Reads the initialization vector (IV) from the input stream.
     *
     * @param is   The input stream.
     * @param file The file name.
     * @return The IV parameter specification.
     * @throws Exception If an error occurs while reading the IV.
     */
    private IvParameterSpec readIV(InputStream is, String file) throws Exception {
        byte[] ivBytes = new byte[16];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(ivBytes); // Generate a random IV
        int bytesRead = 0;
        while (bytesRead < ivBytes.length) {
            int read = is.read(ivBytes, bytesRead, ivBytes.length - bytesRead);
            if (read == -1) {
                throw new Exception(String.format("Failed to read initialization vector. File '%s' has only %d bytes", file, bytesRead));
            }
            bytesRead += read;
        }
        return new IvParameterSpec(ivBytes);
    }

    /**
     * Decodes the AES-encrypted input stream.
     *
     * @param is            The input stream.
     * @param secretKeyBytes The secret key bytes.
     * @param file          The file name.
     * @return The decoded input stream.
     * @throws Exception If an error occurs while decoding.
     */
    private InputStream decodeAES(InputStream is, byte[] secretKeyBytes, String file) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec iv = readIV(is, file);
        SecretKey secretKey = new SecretKeySpec(secretKeyBytes, "AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, iv);
        return new CipherInputStream(is, cipher);
    }

    /**
     * Writes the content of the file to the database.
     *
     * @param file The file name.
     * @throws Exception If an error occurs while writing.
     */
    public void write(String file) throws Exception {
        try (FileInputStream is = new FileInputStream(file)) {
            write(file, is);
        }
    }

    /**
     * Writes the content of the input stream to the database.
     *
     * @param file The file name.
     * @param is   The input stream.
     * @throws Exception If an error occurs while writing.
     */
    public void write(String file, InputStream is) throws Exception {
        InputStream decoded = is;
        if (params.getEncryption() == Encryption.AES) {
            decoded = decodeAES(is, secretKeys.get(file).toByteArray(), file);
        }

        InputStream uncompressed = decoded;
        if (params.getCompression() == Compression.ZSTD) {
            uncompressed = new ZstdInputStream(decoded);
        } else if (params.getCompression() == Compression.GZIP) {
            uncompressed = new GZIPInputStream(decoded);
        }

        try (CSVReader csvReader = new CSVReaderBuilder(new BufferedReader(new InputStreamReader(uncompressed)))
                .withCSVParser(new CSVParserBuilder().withEscapeChar('\0').build())
                .build()) {
            String[] headerString = csvReader.readNext();
            if (headerString == null) {
                // Finish if file is empty
                return;
            }

            List<String> header = new ArrayList<>(Arrays.asList(headerString));
            setHeader(header);

            String[] tokens;
            while ((tokens = csvReader.readNext()) != null) {
                List<String> row = new ArrayList<>(Arrays.asList(tokens));
                writeRow(row);
            }
        }

        commit();
    }

    /**
     * Commits the current batch of rows to the database.
     *
     * @throws InterruptedException, IOException, SQLException If an error occurs while committing.
     */
    public abstract void commit() throws InterruptedException, IOException, SQLException;
}