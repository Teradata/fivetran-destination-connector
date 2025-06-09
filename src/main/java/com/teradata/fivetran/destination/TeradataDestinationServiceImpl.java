package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.warning_util.AlterTableWarningHandler;
import com.teradata.fivetran.destination.warning_util.DescribeTableWarningHandler;
import com.teradata.fivetran.destination.warning_util.WriteBatchWarningHandler;
import com.teradata.fivetran.destination.writers.*;
import fivetran_sdk.v2.*;
import io.grpc.stub.StreamObserver;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;

/**
 * Implementation of the gRPC service for Teradata destination connector.
 */
public class TeradataDestinationServiceImpl extends DestinationConnectorGrpc.DestinationConnectorImplBase {

    /**
     * Handles the configuration form request.
     *
     * @param request The configuration form request.
     * @param responseObserver The response observer to send the response.
     */
    @Override
    public void configurationForm(ConfigurationFormRequest request, StreamObserver<ConfigurationFormResponse> responseObserver) {
        Logger.logMessage(Logger.LogLevel.INFO, "Fetching configuration form");
        responseObserver.onNext(getConfigurationForm());
        responseObserver.onCompleted();
    }

    /**
     * Generates the configuration form response.
     *
     * @return The configuration form response.
     */
    private ConfigurationFormResponse getConfigurationForm() {

        FormField host = FormField.newBuilder()
                .setName("host")
                .setLabel("Host")
                .setRequired(true)
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_host_details")
                .build();

        FormField user = FormField.newBuilder()
                .setRequired(true)
                .setName("user")
                .setLabel("User")
                .setTextField(TextField.PlainText)
                .setPlaceholder("user_name")
                .build();

        FormField td2Password = FormField.newBuilder()
                .setRequired(true)
                .setName("td2password")
                .setLabel("Password")
                .setTextField(TextField.Password)
                .setPlaceholder("your_password")
                .build();

        FormField ldapPassword = FormField.newBuilder()
                .setRequired(true)
                .setName("ldappassword")
                .setLabel("Password")
                .setTextField(TextField.Password)
                .setPlaceholder("your_password")
                .build();

        FormField logmech =  FormField.newBuilder().setName("logmech").setLabel("Logon Mechanism")
                .setRequired(true)
                .setDescription(
                        "Logon Mechanism.\n"
                                + "Options include:\n"
                                + " * 'TD2' uses Teradata Method 2;\n"
                                + " * 'LDAP' uses Lightweight Directory Access Protocol;\n"
                                + " * 'BROWSER' uses Browser Authentication")
                .setDropdownField(DropdownField.newBuilder()
                        .addDropdownField("TD2")
                        .addDropdownField("LDAP")
                        .addDropdownField("BROWSER"))
                .build();

        FormField TD2Logmech = FormField.newBuilder()
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(VisibilityCondition.newBuilder()
                                        .setConditionField("logmech")
                                        .setStringValue("TD2")
                                        .build()
                                )
                                .addAllFields(Arrays.asList(user, td2Password))
                                .build()).build();

        FormField LDAPLogmech = FormField.newBuilder()
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(VisibilityCondition.newBuilder()
                                        .setConditionField("logmech")
                                        .setStringValue("LDAP")
                                        .build()
                                )
                                .addAllFields(Arrays.asList(user, ldapPassword))
                                .build()).build();

        FormField database = FormField.newBuilder()
                .setRequired(true)
                .setDescription("NOTE: Please make sure Database already exists")
                .setName("database")
                .setLabel("Database")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_database_name")
                .build();

        FormField tmode =  FormField.newBuilder().setName("tmode").setLabel("Transaction Mode")
                .setRequired(true)
                .setDescription(
                        "Transaction Mode.\n"
                                + "Options include:\n"
                                + " * 'ANSI' uses American National Standards Institute (ANSI) transaction semantics. This mode is recommended.;\n"
                                + " * 'TERA' uses legacy Teradata transaction semantics. This mode is only recommended for legacy applications that require Teradata transaction semantics;\n"
                                + " * 'DEFAULT' (the default) uses the default transaction mode configured for the database, which may be either ANSI or TERA mode.")
                .setDropdownField(DropdownField.newBuilder()
                        .addDropdownField("ANSI")
                        .addDropdownField("TERA")
                        .addDropdownField("DEFAULT"))
                .build();

        FormField varcharCharacterSet =  FormField.newBuilder().setName("varchar.character.set").setLabel("Varchar column CHARACTER SET")
                .setRequired(true)
                .setDescription(
                        "Specifies the character set for VARCHAR columns.\n"
                                + "Options include:\n"
                                + " * 'LATIN' uses the LATIN character set;\n"
                                + " * 'UNICODE' uses the UNICODE character set.\n"
                                + "The default is 'LATIN'.")
                .setDropdownField(DropdownField.newBuilder()
                        .addDropdownField("LATIN")
                        .addDropdownField("UNICODE"))
                .build();


        FormField serverCert = FormField.newBuilder().setName("ssl.server.cert")
                .setLabel("SSL Server's Certificate").setRequired(true)
                .setDescription(
                        "Server's certificate.\n"
                        + "Please specify the base64url encoded contents of a PEM file that contains Certificate Authority (CA) certificates.\n"
                        + "The base64url encoded value must conform to IETF RFC 4648 Section 5 - Base 64 Encoding with URL and Filename Safe Alphabet.\n"
                        + "Example Linux command to print the base64url encoded contents of a PEM file: base64 -w0 < cert.pem | tr +/ -_ | tr -d = \n"
                        + "For more information, please refer: https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#URL_SSLBASE64\n")
                .setTextField(TextField.PlainText)
                .build();


        FormField sslMode = FormField.newBuilder().setName("ssl.mode").setLabel("SSL mode")
                .setRequired(false)
                .setDescription(
                        "Whether to use an encrypted connection to Teradata.\n"
                                + "Options include:\n"
                                + " * 'DISABLE' disables HTTPS/TLS connections and uses only non-TLS connections;\n"
                                + " * 'ALLOW' uses non-TLS connections unless the database requires HTTPS/TLS connections;\n"
                                + " * 'PREFER' uses HTTPS/TLS connections unless the database does not offer HTTPS/TLS connections\n"
                                + " * 'REQUIRE' uses HTTPS/TLS connections\n"
                                + " * 'VERIFY-CA' uses HTTPS/TLS connections and verifies that the server certificate is valid and trusted\n"
                                + " * 'VERIFY-FULL' uses HTTPS/TLS connections, verifies that the server certificate is valid and trusted, and verifies that the server certificate matches the database hostname.")
                .setDropdownField(DropdownField.newBuilder()
                        .addDropdownField("DISABLE")
                        .addDropdownField("ALLOW")
                        .addDropdownField("PREFER")
                        .addDropdownField("REQUIRE")
                        .addDropdownField("VERIFY-CA")
                        .addDropdownField("VERIFY-FULL"))
                .build();

        FormField sslVerifyCa = FormField.newBuilder()
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(VisibilityCondition.newBuilder()
                                        .setConditionField("ssl.mode")
                                        .setStringValue("VERIFY-CA")
                                        .build()
                                )
                                .addAllFields(
                                        Collections.singletonList(serverCert))
                                .build())
                .build();

        FormField sslVerifyFull = FormField.newBuilder()
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(VisibilityCondition.newBuilder()
                                        .setConditionField("ssl.mode")
                                        .setStringValue("VERIFY-FULL")
                                        .build()
                                )
                                .addAllFields(
                                        Collections.singletonList(serverCert))
                                .build())
                .build();

        FormField driverParameters = FormField.newBuilder()
                .setName("driver.parameters")
                .setLabel("Driver Parameters")
                .setRequired(false)
                .setDescription(
                        "Additional JDBC parameters to use with connection string to Teradata Vantage.\n"
                                + "Format: 'param1=value1,param2=value2, ...'.\n"
                                + "The supported parameters are available in the https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/frameset.html")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_driver_parameters")
                .build();

        FormField BatchSize = FormField.newBuilder()
                .setName("batch.size")
                .setLabel("Batch Size")
                .setRequired(false)
                .setDescription("Maximum number of rows that will be changed by a query. Default is 10000")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_batch_size")
                .build();

        FormField queryBand = FormField.newBuilder()
                .setName("query.band")
                .setLabel("Query Band")
                .setRequired(false)
                .setDescription("Specify the Query Band string to be set in key-value pairs. Query Band is used for telemetry purposes"+
                        "format for query band is \"key=value;key2=value2;\""
                        +" Default would be \"org=teradata-internal-telem;appname=fivetran;\"")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_query_band")
                .build();

        return ConfigurationFormResponse.newBuilder()
                .setSchemaSelectionSupported(true)
                .setTableSelectionSupported(true)
                .addAllFields(Arrays.asList(host, logmech, TD2Logmech, LDAPLogmech, database, tmode, varcharCharacterSet, sslMode, sslVerifyCa, sslVerifyFull, driverParameters, BatchSize, queryBand))
                .addAllTests(Arrays.asList(
                        ConfigurationTest.newBuilder().setName("connect").setLabel("Tests connection").build()))
                .build();
    }

    @Override
    public void capabilities(CapabilitiesRequest request,
                             StreamObserver<CapabilitiesResponse> responseObserver) {
        responseObserver.onNext(CapabilitiesResponse
                .newBuilder()
                .setBatchFileFormat(BatchFileFormat.CSV)
                .build());
        responseObserver.onCompleted();
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
                Logger.logMessage(Logger.LogLevel.SEVERE, String.format("Test failed with exception %s", e.getMessage()));
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
        Logger.logMessage(Logger.LogLevel.INFO,"########################describeTable##############################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = TeradataJDBCUtil.getTableName(request.getSchemaName(), request.getTableName());

        Logger.logMessage(Logger.LogLevel.INFO, String.format("Database: %s, Table: %s", database, table));

        try {
            Table t = TeradataJDBCUtil.getTable(conf, database, table, request.getTableName(), new DescribeTableWarningHandler(responseObserver));
            Logger.logMessage(Logger.LogLevel.INFO, String.format("Table %s exists", TeradataJDBCUtil.escapeTable(database, table)));
            Logger.logMessage(Logger.LogLevel.INFO, String.format("Table %s description: %s", TeradataJDBCUtil.escapeTable(database, table), t));
            DescribeTableResponse response = DescribeTableResponse.newBuilder().setTable(t).build();
            responseObserver.onNext(response);
        } catch (Exception e) {
            if (e.getCause() instanceof TableNotExistException) {
                Logger.logMessage(Logger.LogLevel.WARNING, String.format("Table %s doesn't exist", TeradataJDBCUtil.escapeTable(database, table)));
                DescribeTableResponse response =
                        DescribeTableResponse.newBuilder().setNotFound(true).build();
                responseObserver.onNext(response);
            } else {
                Logger.logMessage(Logger.LogLevel.SEVERE, String.format("DescribeTable failed for %s with exception %s",
                        TeradataJDBCUtil.escapeTable(database, table), e.getMessage()));
                responseObserver.onNext(DescribeTableResponse.newBuilder()
                        .setWarning(Warning.newBuilder().setMessage("describeTable :: Table: " + TeradataJDBCUtil.escapeTable(database, table) + ", Error: " + e.getMessage()).build())
                        .build());
            }
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
        Logger.logMessage(Logger.LogLevel.INFO,"#########################createTable#############################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String query = "";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            query = TeradataJDBCUtil.generateCreateTableQuery(conf, stmt, request);
            if(query == null) {
                throw new Exception("Table or Database is empty");
            }
            Logger.logMessage(Logger.LogLevel.INFO, String.format("Executing SQL:\n %s", query));
            stmt.execute(query);

            responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(true).build());
        } catch (Exception e) {
            Logger.logMessage(Logger.LogLevel.SEVERE, String.format("CreateTable failed with exception %s", e.getMessage()));
            responseObserver.onNext(CreateTableResponse.newBuilder()
                    .setTask(Task.newBuilder().setMessage("createTable :: Table: "
                            + TeradataJDBCUtil.escapeTable(conf.database(), request.getTable().getName())
                            + ", SQL: " + query.replace("\n", " ")
                            + ", Error: " + getStackTraceOneLine(e)).build())
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
        Logger.logMessage(Logger.LogLevel.INFO,"#########################alterTable#############################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String query = "";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {

            query = TeradataJDBCUtil.generateAlterTableQuery(request, new AlterTableWarningHandler(responseObserver));
            // query is null when table is not changed
            if (query != null) {
                String[] queries = query.split(";");
                for (String q : queries) {
                    Logger.logMessage(Logger.LogLevel.INFO, String.format("Executing SQL:\n %s", q));
                    if (!q.trim().isEmpty()) {
                        stmt.execute(q.trim() + ";");
                    }
                }
            }

            responseObserver.onNext(AlterTableResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            Logger.logMessage(Logger.LogLevel.SEVERE, String.format("AlterTable failed with exception %s", e.getMessage()));
            responseObserver.onNext(AlterTableResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage("alterTable :: Table: " 
                                    + TeradataJDBCUtil.escapeTable(conf.database(), request.getTable().getName()) 
                                    + ", SQL: " + query.replace("\n", " ")
                                    + ", Error: " + getStackTraceOneLine(e)).build())
                    .build());
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
        Logger.logMessage(Logger.LogLevel.INFO,"#########################truncate#############################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = TeradataJDBCUtil.getTableName(request.getSchemaName(), request.getTableName());
        String query = "";
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);
             Statement stmt = conn.createStatement()) {
            if (!TeradataJDBCUtil.checkTableExists(stmt, database, table)) {
                Logger.logMessage(Logger.LogLevel.WARNING, String.format("Table %s doesn't exist", TeradataJDBCUtil.escapeTable(database, table)));
                responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
                return;
            }
            long nanoseconds = request.getUtcDeleteBefore().getSeconds();
            String nanos = String.valueOf( nanoseconds * 1000000L);
            nanos = nanos.substring(0, 6);
            String queryNanosToTimestamp = String.format("SELECT TO_TIMESTAMP(CAST('%d' AS BIGINT)) + INTERVAL '0.%06d' SECOND",
                    request.getUtcDeleteBefore().getSeconds(), Long.parseLong(nanos));
            stmt.execute(queryNanosToTimestamp);
            String utcDeleteBefore = TeradataJDBCUtil.getSingleValue(stmt.getResultSet());


            query = TeradataJDBCUtil.generateTruncateTableQuery(database, table, request, utcDeleteBefore);
            Logger.logMessage(Logger.LogLevel.INFO,String.format("Executing SQL:\n %s", query));
            stmt.execute(query);

            responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            Logger.logMessage(Logger.LogLevel.SEVERE, String.format("TruncateTable failed for %s with exception %s",
                    TeradataJDBCUtil.escapeTable(database, table), e.getMessage()));
            responseObserver.onNext(TruncateResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage("truncate :: Table: " +
                                    TeradataJDBCUtil.escapeTable(conf.database(), request.getTableName())
                                    + ", SQL: " + query.replace("\n", " ")
                                    + ", Error: " + getStackTraceOneLine(e)).build())
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
        Logger.logMessage(Logger.LogLevel.INFO,"#########################writeBatch#############################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = TeradataJDBCUtil.getTableName(request.getSchemaName(), request.getTable().getName());
        LoadDataWriter w = null;
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);) {
            if (request.getTable().getColumnsList().stream()
                    .noneMatch(column -> column.getPrimaryKey())) {
                throw new Exception("No primary key found");
            }
            Logger.logMessage(Logger.LogLevel.INFO, "********************************In LoadDataWriter**********************************");
            w = new LoadDataWriter(conn, database, table, request.getTable().getColumnsList(),
                            request.getFileParams(), request.getKeysMap(), conf.batchSize(),
                            new WriteBatchWarningHandler(responseObserver));
            Logger.logMessage(Logger.LogLevel.INFO, "No. of files to be written: " + request.getReplaceFilesList().size());
            for (String file : request.getReplaceFilesList()) {
                w.write(file);
            }
            if(!request.getReplaceFilesList().isEmpty()) {
                w.deleteInsert();
                w.dropTempTable();
            }
            Logger.logMessage(Logger.LogLevel.INFO, "********************************In UpdateWriter**********************************");
            UpdateWriter u =
                    new UpdateWriter(conn, database, table, request.getTable().getColumnsList(),
                            request.getFileParams(), request.getKeysMap(), conf.batchSize());
            Logger.logMessage(Logger.LogLevel.INFO, "No. of files to be updated: " + request.getUpdateFilesList().size());
            for (String file : request.getUpdateFilesList()) {
                u.write(file);
            }
            Logger.logMessage(Logger.LogLevel.INFO, "********************************In DeleteWriter**********************************");
            DeleteWriter d =
                    new DeleteWriter(conn, database, table, request.getTable().getColumnsList(),
                            request.getFileParams(), request.getKeysMap(), conf.batchSize());
            Logger.logMessage(Logger.LogLevel.INFO, "No. of files to be deleted: " + request.getDeleteFilesList().size());
            for (String file : request.getDeleteFilesList()) {
                d.write(file);
            }

            responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }
        catch (BatchUpdateException bue) {
            String actualError = "";
            SQLException next = bue.getNextException();
            while (next != null) {
                Logger.logMessage(Logger.LogLevel.INFO, "Caused by: " + next.getMessage());
                next = next.getNextException();
            }
//            if (bue.getNextException() != null) {
//                Exception nextException = bue.getNextException();
//                actualError = nextException.getMessage();
//            } else {
//                actualError = bue.getMessage();
//            }
            Logger.logMessage(Logger.LogLevel.SEVERE, String.format("WriteBatch failed with exception %s", actualError));
            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage("writeBatch :: Table: " + TeradataJDBCUtil.escapeTable(database, table) + ", Error: " + actualError).build())
                    .build());
            responseObserver.onCompleted();
        }
        catch (Exception e) {
            Logger.logMessage(Logger.LogLevel.SEVERE, String.format("WriteBatch failed for %s with exception %s",
                    TeradataJDBCUtil.escapeTable(database, table), e.getMessage()));
            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage("writeBatch :: Table: " + TeradataJDBCUtil.escapeTable(database, table) + ", Error: " + getStackTraceOneLine(e)).build())
                    .build());
            responseObserver.onCompleted();
        }
        finally {
            if (w != null && !request.getReplaceFilesList().isEmpty()) {
                w.dropTempTable();
            }
        }
    }

    @Override
    public void writeHistoryBatch(WriteHistoryBatchRequest request,
                                  StreamObserver<WriteBatchResponse> responseObserver) {
        Logger.logMessage(Logger.LogLevel.INFO,"#########################writeHistoryBatch#############################################################");
        TeradataConfiguration conf = new TeradataConfiguration(request.getConfigurationMap());
        String database = TeradataJDBCUtil.getDatabaseName(conf, request.getSchemaName());
        String table = TeradataJDBCUtil.getTableName(request.getSchemaName(), request.getTable().getName());
        LoadDataWriter<WriteBatchResponse> w = null;
        try (Connection conn = TeradataJDBCUtil.createConnection(conf);) {
            if (request.getTable().getColumnsList().stream()
                    .noneMatch(Column::getPrimaryKey)) {
                throw new Exception("No primary key found");
            }
            Logger.logMessage(Logger.LogLevel.INFO,"********************************In EarliestStartHistoryWriter**********************************");
            EarliestStartHistoryWriter e = new EarliestStartHistoryWriter(conn, database, table, request.getTable().getColumnsList(),
                    request.getFileParams(), request.getKeysMap(), conf.batchSize());
            Logger.logMessage(Logger.LogLevel.INFO, "No. of files to be written with earliest start: " + request.getEarliestStartFilesList().size());
            for (String file : request.getEarliestStartFilesList()) {
                e.write(file);
            }
            Logger.logMessage(Logger.LogLevel.INFO, "********************************In UpdateHistoryWriter**********************************");
            UpdateHistoryWriter u = new UpdateHistoryWriter(conn, database, table, request.getTable().getColumnsList(),
                    request.getFileParams(), request.getKeysMap(), conf.batchSize());
            Logger.logMessage(Logger.LogLevel.INFO, "No. of files to be updated with history: " + request.getUpdateFilesList().size());
            for (String file : request.getUpdateFilesList()) {
                u.write(file);
            }
            Logger.logMessage(Logger.LogLevel.INFO, "********************************In LoadDataWriter**********************************");
            w = new LoadDataWriter<>(conn, database, table, request.getTable().getColumnsList(),
                    request.getFileParams(), request.getKeysMap(), conf.batchSize(),
                    new WriteBatchWarningHandler(responseObserver));
            Logger.logMessage(Logger.LogLevel.INFO, "No. of files to be written with history: " + request.getReplaceFilesList().size());
            for (String file : request.getReplaceFilesList()) {
                w.write(file);
            }
            if(!request.getReplaceFilesList().isEmpty()) {
                w.deleteInsert();
                w.dropTempTable();
            }
            Logger.logMessage(Logger.LogLevel.INFO, "********************************In DeleteHistoryWriter**********************************");
            DeleteHistoryWriter d = new DeleteHistoryWriter(conn, database, table, request.getTable().getColumnsList(),
                    request.getFileParams(), request.getKeysMap(), conf.batchSize());
            Logger.logMessage(Logger.LogLevel.INFO, "No. of files to be deleted with history: " + request.getDeleteFilesList().size());
            for (String file : request.getDeleteFilesList()) {
                d.write(file);
            }

            responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }
        catch (BatchUpdateException bue) {
            String actualMessage = "";
            if (bue.getNextException() != null) {
                Exception nextException = bue.getNextException();
                actualMessage = nextException.getMessage();
            } else {
                actualMessage = bue.getMessage();
            }
            Logger.logMessage(Logger.LogLevel.SEVERE, String.format("writeHistoryBatch failed with exception %s", actualMessage));
            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage("writeHistoryBatch :: Table: " + TeradataJDBCUtil.escapeTable(database, table) + ", Error: " + actualMessage).build())
                    .build());
            responseObserver.onCompleted();
        }
        catch (Exception e) {
            Logger.logMessage(Logger.LogLevel.SEVERE, String.format("WriteHistoryBatch failed for %s with exception %s",
                    TeradataJDBCUtil.escapeTable(database, table), e.getMessage()));

            responseObserver.onNext(WriteBatchResponse.newBuilder()
                    .setTask(Task.newBuilder()
                            .setMessage("writeHistoryBatch :: Table: " + TeradataJDBCUtil.escapeTable(database, table) + ", Error: " + getStackTraceOneLine(e)).build())
                    .build());
            responseObserver.onCompleted();
        }
        finally {
            if (w != null && !request.getReplaceFilesList().isEmpty()) {
                w.dropTempTable();
            }
        }
    }

    private static String getStackTraceOneLine(Exception ex) {
        StringBuilder sb = new StringBuilder();
        sb.append(ex.getClass().getName()).append(": ").append(ex.getMessage()).append(" | ");
        for (StackTraceElement element : ex.getStackTrace()) {
            sb.append("at ").append(element.toString()).append(" | ");
        }
        return sb.toString();
    }
}
