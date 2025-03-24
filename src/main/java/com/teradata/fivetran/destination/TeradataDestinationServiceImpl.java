package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.warning_util.AlterTableWarningHandler;
import com.teradata.fivetran.destination.warning_util.DescribeTableWarningHandler;
import com.teradata.fivetran.destination.warning_util.WriteBatchWarningHandler;
import com.teradata.fivetran.destination.writers.*;
import fivetran_sdk.v2.*;
import io.grpc.stub.StreamObserver;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the gRPC service for Teradata destination connector.
 */
public class TeradataDestinationServiceImpl extends DestinationConnectorGrpc.DestinationConnectorImplBase {
    private static final Logger logger = LoggerFactory.getLogger(TeradataDestinationServiceImpl.class);
    private static final String INFO = "INFO";
    private static final String WARNING = "WARNING";
    private static final String SEVERE = "SEVERE";

    /**
     * Handles the configuration form request.
     *
     * @param request The configuration form request.
     * @param responseObserver The response observer to send the response.
     */
    @Override
    public void configurationForm(ConfigurationFormRequest request, StreamObserver<ConfigurationFormResponse> responseObserver) {
        logMessage(INFO, "Fetching configuration form");
        responseObserver.onNext(getConfigurationForm());
        responseObserver.onCompleted();
    }

    /**
     * Generates the configuration form response.
     *
     * @return The configuration form response.
     */
    private ConfigurationFormResponse getConfigurationForm() {
        FormField writerType = FormField.newBuilder()
                .setName("writerType")
                .setLabel("Writer Type")
                .setDescription("Choose the destination type")
                .setDropdownField(
                        DropdownField.newBuilder()
                                .addAllDropdownField(Arrays.asList("Database", "File", "Cloud"))
                                .build())
                .setDefaultValue("Database")
                .build();

        FormField host = FormField.newBuilder()
                .setName("host")
                .setLabel("Host")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_host_details")
                .build();

        FormField user = FormField.newBuilder()
                .setName("user")
                .setLabel("User")
                .setTextField(TextField.PlainText)
                .setPlaceholder("user_name")
                .build();

        FormField password = FormField.newBuilder()
                .setName("password")
                .setLabel("Password")
                .setTextField(TextField.Password)
                .setPlaceholder("your_password")
                .build();

        FormField database = FormField.newBuilder()
                .setName("database")
                .setLabel("Database")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_database_name")
                .build();

        FormField table = FormField.newBuilder()
                .setName("table")
                .setLabel("Table")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_table_name")
                .build();

        return ConfigurationFormResponse.newBuilder()
                .setSchemaSelectionSupported(true)
                .setTableSelectionSupported(true)
                .addAllFields(Arrays.asList(writerType, host, user, password, database, table))
                .addAllTests(Arrays.asList(
                        ConfigurationTest.newBuilder().setName("connect").setLabel("Tests connection").build()))
                .build();
    }

    /**
     * Handles the test request.
     *
     * @param request The test request.
     * @param responseObserver The response observer to send the response.
     */
    @Override
    public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {
        String testName = request.getName();

        if ("connect".equals(testName)) {
            TeradataConfiguration configuration = new TeradataConfiguration(request.getConfigurationMap());
            try (Connection conn = TeradataJDBCUtil.createConnection(configuration);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");
                responseObserver.onNext(TestResponse.newBuilder().setSuccess(true).build());
            } catch (Exception e) {
                responseObserver.onNext(TestResponse.newBuilder().setSuccess(false).setFailure(e.getMessage()).build());
            }
            responseObserver.onCompleted();
        }
    }

    /**
     * Handles the describe table request.
     *
     * @param request The describe table request.
     * @param responseObserver The response observer to send the response.
     */
    @Override
    public void describeTable(DescribeTableRequest request, StreamObserver<DescribeTableResponse> responseObserver) {
        logger.info("######################################################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = TeradataJDBCUtil.getTableName(conf, request.getSchemaName(), request.getTableName());

        logMessage(INFO, String.format("Database: %s, Table: %s", database, table));

        try {
            Table t = TeradataJDBCUtil.getTable(conf, database, table, table, new DescribeTableWarningHandler(responseObserver));
            logMessage(INFO, String.format("Table metadata: %s", t));
            responseObserver.onNext(DescribeTableResponse.newBuilder().setTable(t).build());
        } catch (TeradataJDBCUtil.TableNotExistException e) {
            logger.warn(String.format("Table %s doesn't exist", TeradataJDBCUtil.escapeTable(database, table)));
            responseObserver.onNext(DescribeTableResponse.newBuilder().setNotFound(true).build());
        } catch (Exception e) {
            logger.warn(String.format("DescribeTable failed for %s", TeradataJDBCUtil.escapeTable(database, table)), e);
            responseObserver.onNext(DescribeTableResponse.newBuilder()
                    .setWarning(Warning.newBuilder().setMessage(e.getMessage()).build())
                    .build());
        }
        responseObserver.onCompleted();
    }

    /**
     * Handles the create table request.
     *
     * @param request The create table request.
     * @param responseObserver The response observer to send the response.
     */
    @Override
    public void createTable(CreateTableRequest request, StreamObserver<CreateTableResponse> responseObserver) {
        logger.info("######################################################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            String query = TeradataJDBCUtil.generateCreateTableQuery(conf, stmt, request);
            logger.info(String.format("Executing SQL:\n %s", query));

            logMessage(INFO, String.format("[CreateTable]: %s | %s | %s",
                    request.getSchemaName(), request.getTable().getName(), request.getTable().getColumnsList()));
            stmt.execute(query);

            responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(true).build());
        } catch (Exception e) {
            String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
            String table = TeradataJDBCUtil.getTableName(conf, request.getSchemaName(), request.getTable().getName());

            logger.warn(String.format("CreateTable failed for %s", TeradataJDBCUtil.escapeTable(database, table)), e);
            responseObserver.onNext(CreateTableResponse.newBuilder()
                    .setTask(Task.newBuilder().setMessage(e.getMessage()).build())
                    .setSuccess(false)
                    .build());
        }
        responseObserver.onCompleted();
    }

    /**
     * Handles the alter table request.
     *
     * @param request The alter table request.
     * @param responseObserver The response observer to send the response.
     */
    @Override
    public void alterTable(AlterTableRequest request,
                           StreamObserver<AlterTableResponse> responseObserver) {
        logger.info("######################################################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            String query = TeradataJDBCUtil.generateAlterTableQuery(request, new AlterTableWarningHandler(responseObserver));
            // query is null when table is not changed
            if (query != null) {
                String[] queries = query.split(";");
                for (String q : queries) {
                    logger.info(String.format("Executing SQL:\n %s", q));
                    if (!q.trim().isEmpty()) {
                        stmt.execute(q.trim());
                    }
                }
            }

            responseObserver.onNext(AlterTableResponse.newBuilder().setSuccess(true).build());
        } catch (Exception e) {
            String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
            String table = TeradataJDBCUtil.getTableName(conf, request.getSchemaName(),
                    request.getTable().getName());
            logger.warn(String.format("AlterTable failed for %s",
                    TeradataJDBCUtil.escapeTable(database, table)), e);

            responseObserver.onNext(AlterTableResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage(e.getMessage()).build())
                    .build());
            responseObserver.onNext(AlterTableResponse.newBuilder()
                    .setSuccess(false)
                    .build());
        }
        finally {
            responseObserver.onCompleted();
        }
    }

    /**
     * Handles the truncate table request.
     *
     * @param request The truncate table request.
     * @param responseObserver The response observer to send the response.
     */
    @Override
    public void truncate(TruncateRequest request,
                         StreamObserver<TruncateResponse> responseObserver) {
        logger.info("######################################################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = TeradataJDBCUtil.getTableName(conf, request.getSchemaName(), request.getTableName());

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            if (!TeradataJDBCUtil.checkTableExists(stmt, database, table)) {
                logger.warn(String.format("Table %s doesn't exist",
                        TeradataJDBCUtil.escapeTable(database, table)));
                responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
                return;
            }

            String queryNanosToTimestamp = String.format("SELECT TO_TIMESTAMP(CAST('%d' AS BIGINT)) + INTERVAL '0.%06d' SECOND",
                    request.getUtcDeleteBefore().getSeconds(), request.getUtcDeleteBefore().getNanos());
            logger.info(String.format("Executing SQL:\n %s", queryNanosToTimestamp));
            stmt.execute(queryNanosToTimestamp);
            String utcDeleteBefore = TeradataJDBCUtil.getSingleValue(stmt.getResultSet());


            String query = TeradataJDBCUtil.generateTruncateTableQuery(conf, request, utcDeleteBefore);
            logger.info(String.format("Executing SQL:\n %s", query));
            stmt.execute(query);

            responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.warn(String.format("TruncateTable failed for %s",
                    TeradataJDBCUtil.escapeTable(database, table)), e);

            responseObserver.onNext(TruncateResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage(e.getMessage()).build())
                    .build());
            responseObserver.onNext(TruncateResponse.newBuilder()
                    .setSuccess(false)
                    .build());
            responseObserver.onCompleted();
        }
    }

    /**
     * Handles the write batch request.
     *
     * @param request The write batch request.
     * @param responseObserver The response observer to send the response.
     */
    @Override
    public void writeBatch(WriteBatchRequest request,
                           StreamObserver<WriteBatchResponse> responseObserver) {
        logger.info("######################################################################################");
        logger.info("**************** In writeBatch");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table =
                TeradataJDBCUtil.getTableName(conf, request.getSchemaName(), request.getTable().getName());
        logger.info(String.format("Database: %s, Table: %s", database, table));
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);) {
            if (request.getTable().getColumnsList().stream()
                    .noneMatch(Column::getPrimaryKey)) {
                throw new Exception("No primary key found");
            }
            logger.info("****************************************************************** In LoadDataWriter");
            LoadDataWriter<WriteBatchResponse> w =
                    new LoadDataWriter<>(conn, database, table, request.getTable().getColumnsList(),
                            request.getFileParams(), request.getKeysMap(), conf.batchSize(),
                            new WriteBatchWarningHandler(responseObserver));
            for (String file : request.getReplaceFilesList()) {
                w.write(file);
            }
            logger.info("****************************************************************** In UpdateWriter");
            UpdateWriter u =
                    new UpdateWriter(conn, database, table, request.getTable().getColumnsList(),
                            request.getFileParams(), request.getKeysMap(), conf.batchSize());
            for (String file : request.getUpdateFilesList()) {
                u.write(file);
            }
            logger.info("****************************************************************** In DeleteWriter");
            DeleteWriter d =
                    new DeleteWriter(conn, database, table, request.getTable().getColumnsList(),
                            request.getFileParams(), request.getKeysMap(), conf.batchSize());
            for (String file : request.getDeleteFilesList()) {
                d.write(file);
            }

            responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.warn(String.format("WriteBatch failed for %s",
                    TeradataJDBCUtil.escapeTable(database, table)), e);

            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage(e.getMessage()).build())
                    .build());
            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setSuccess(false)
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void writeHistoryBatch(WriteHistoryBatchRequest request,
                                  StreamObserver<WriteBatchResponse> responseObserver) {
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table =
                TeradataJDBCUtil.getTableName(conf, request.getSchemaName(), request.getTable().getName());

        try (Connection conn = TeradataJDBCUtil.createConnection(conf);) {
            if (request.getTable().getColumnsList().stream()
                    .noneMatch(Column::getPrimaryKey)) {
                throw new Exception("No primary key found");
            }

            EarliestStartHistoryWriter e = new EarliestStartHistoryWriter(conn, database, table, request.getTable().getColumnsList(),
                    request.getFileParams(), request.getKeysMap(), conf.batchSize());
            for (String file : request.getEarliestStartFilesList()) {
                e.write(file);
            }

            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, table, request.getTable().getColumnsList(),
                    request.getFileParams(), request.getKeysMap(), conf.batchSize());
            for (String file : request.getUpdateFilesList()) {
                u.write(file);
            }

            LoadDataWriter<WriteBatchResponse> w = new LoadDataWriter<>(conn, database, table, request.getTable().getColumnsList(),
                    request.getFileParams(), request.getKeysMap(), conf.batchSize(),
                    new WriteBatchWarningHandler(responseObserver));
            for (String file : request.getReplaceFilesList()) {
                w.write(file);
            }

            DeleteHistoryWriter d = new DeleteHistoryWriter(conn, database, table, request.getTable().getColumnsList(),
                    request.getFileParams(), request.getKeysMap(), conf.batchSize());
            for (String file : request.getDeleteFilesList()) {
                d.write(file);
            }
        } catch (Exception e) {
            logger.warn(String.format("WriteHistoryBatch failed for %s",
                    TeradataJDBCUtil.escapeTable(database, table)), e);

            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage(e.getMessage()).build())
                    .build());
            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setSuccess(false)
                    .build());
            responseObserver.onCompleted();
        }
    }

    /**
     * Logs a message with the specified level.
     *
     * @param level The log level.
     * @param message The log message.
     */
    private void logMessage(String level, String message) {
        System.out.println(String.format("{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}", level, message));
    }
}