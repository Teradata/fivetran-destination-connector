package com.teradata.fivetran.destination.writers;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.google.protobuf.ByteString;
import com.teradata.fivetran.destination.TeradataConfiguration;
import com.teradata.fivetran.destination.warning_util.WarningHandler;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.FileParams;
import com.teradata.fivetran.destination.Logger;

import com.teradata.connector.sample.plugin.utils.CommonDBSchemaUtils;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;

public class FastLoadDataWriter {
    protected static String SQL_GET_LSN = "{fn teradata_logon_sequence_number()}";
    protected static String jdbcDriver = "com.teradata.jdbc.TeraDriver";


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

    public FastLoadDataWriter(TeradataConfiguration conf, Connection conn, String database, String table, List<Column> columns,
                              FileParams params, Map<String, ByteString> secretKeys, Integer batchSize, List<String> sourceFilesList) {

        String dbsHost = conf.host();
        String username = conf.user();
        String password = conf.password();
        String targetTable = table;
        String outputTableName = String.format("%s_%s", "td_tmp", UUID.randomUUID().toString().replace("-", "_"));
        String errorTable1 = outputTableName + "_ERR1";
        String errorTable2 = outputTableName + "_ERR2";

        int instances = sourceFilesList.size();

        String endLoading = "END LOADING";

        Connection lsnConnection = null;
        Statement stmt = null;

        String lsnUrl = "jdbc:teradata://" + dbsHost
                + "/CHARSET=UTF8,LSS_TYPE=L,TMODE=TERA,CONNECT_FUNCTION=1,TSNANO=3"; // Control Session
        try {
            Class.forName(jdbcDriver);
            lsnConnection = DriverManager.getConnection(lsnUrl, username, password);
            String lsnNumber = lsnConnection.nativeSQL(SQL_GET_LSN);
            Logger.logMessage(Logger.LogLevel.INFO,"FastLoad LSN: " + lsnNumber);
            String fastLoadURL = "jdbc:teradata://" + dbsHost
                    + "/CHARSET=UTF8,LSS_TYPE=L,PARTITION=FASTLOAD,CONNECT_FUNCTION=2,TSNANO=3,LOGON_SEQUENCE_NUMBER="
                    + lsnNumber; // FastLoad Session
            // Session
            String beginLoading = "BEGIN LOADING " + outputTableName + " ERRORFILES " + errorTable1 + ", " + errorTable2
                    + " WITH INTERVAL";

            // Creating FastLoad Connections
            FastLoad fastLoad[] = new FastLoad[instances];
            for (int i = 0; i < instances; i++) {
                fastLoad[i] = new FastLoad();
            }
            Logger.logMessage(Logger.LogLevel.INFO,"fastLoadURL: " + fastLoadURL);
            Logger.logMessage(Logger.LogLevel.INFO,"username: " + username);
            Logger.logMessage(Logger.LogLevel.INFO,"password: " + password);

            for (int i = 0; i < instances; i++) {
                Logger.logMessage(Logger.LogLevel.INFO,"calling createFastLoadConnection() for instance : " + i);
                fastLoad[i].createFastLoadConnection(i + 1, fastLoadURL, username, password);
            }

            // Submitting beginLoading
            stmt = lsnConnection.createStatement();
            lsnConnection.setAutoCommit(true);
            stmt.execute(beginLoading);

            String usingInsertSQL = getusingInsertSQL(lsnConnection, outputTableName);
            //String usingInsertSQL = "USING \"Associate_Id\" (INTEGER), \"Associate_Name\" (VARCHAR(100)) INSERT INTO \"large_td_table_export\" ( \"Associate_Id\", \"Associate_Name\") VALUES ( :\"Associate_Id\", :\"Associate_Name\")";
            Logger.logMessage(Logger.LogLevel.INFO,"usingInsertSQL: " + usingInsertSQL);

            // submitting usingInsertSQL
            lsnConnection.setAutoCommit(false);
            stmt.executeUpdate(usingInsertSQL);

            FastLoadThread fastLoadThread[] = new FastLoadThread[instances];
            for (int i = 0; i < instances; i++) {
                fastLoadThread[i] = new FastLoadThread(fastLoad[i], sourceFilesList.get(i), columns, params, secretKeys);
                Logger.logMessage(Logger.LogLevel.INFO,"Starting Thread: " + i);
                fastLoadThread[i].start();
            }
            System.out.println("**********************: Threads done");
            int count = 0;
            while (true) {
                System.out.println("**********************: in while");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                }
                for (int i = 0; i < instances; i++) {
                    System.out.println("**********************: in for");
                    if (fastLoad[i].getLoadCompleted()) {
                        count++;
                    }
                }
                if (count == instances) {
                    break;
                }
                count = 0;
            }

            /*
             * // Passing data to each session for (int i = 0; i < instances; i++) {
             * fastLoad[i].loadData(datasetList.get(i), structFields); }
             */

            // Load left over rows
            for (int i = 0; i < instances; i++) {
                fastLoad[i].loadLeftOverRows();
            }

            stmt.executeUpdate("CHECKPOINT LOADING END");
            lsnConnection.commit();

            // submitting endLoading
            stmt.executeUpdate(endLoading);
            lsnConnection.commit();

            lsnConnection.setAutoCommit(true);
            stmt.close();

            for (int i = 0; i < instances; i++) {
                fastLoad[i].closeFastLoadConnection();
            }

            lsnConnection.close();
        } catch (SQLException ex) {
            try {
                stmt.executeUpdate(endLoading);
                lsnConnection.commit();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static String getusingInsertSQL(Connection con, String outputTableName) {
        try {
            Statement stmt = con.createStatement();
            ResultSet res = stmt
                    .executeQuery("select count(*) from dbc.columns where tablename='" + outputTableName + "';");
            res.next();
            String[] colNames = new String[res.getInt(1)];
            res = null;
            res = stmt.executeQuery("select columnName from dbc.columns where tablename='" + outputTableName + "';");
            int c = 0;

            while (res.next()) {
                colNames[c] = res.getString("columnName");
                c++;
            }
            Logger.logMessage(Logger.LogLevel.INFO, Arrays.toString(colNames));
            TeradataColumnDesc[] fieldDescs = CommonDBSchemaUtils.getColumnDesc(outputTableName, colNames, con);

            String[] fieldTypes4Using = new String[fieldDescs.length];
            String[] fieldNames = new String[fieldDescs.length];

            int index;

            /*
             * Determine the lowest timestamp precision (scale) to set for all
             * time/timestamp columns in USING statement
             */
            int lowestScaleForTime = TeradataColumnDesc.TIME_SCALE_DEFAULT;
            int lowestScaleForTimeStamp = TeradataColumnDesc.TIME_SCALE_DEFAULT;
            for (index = 0; index < fieldDescs.length; index++) {
                TeradataColumnDesc fieldDesc = fieldDescs[index];
                if (fieldDesc.getType() == java.sql.Types.TIME) {
                    if (fieldDesc.getScale() < lowestScaleForTime) {
                        lowestScaleForTime = fieldDesc.getScale();
                    }
                } else if (fieldDesc.getType() == java.sql.Types.TIMESTAMP) {
                    if (fieldDesc.getScale() < lowestScaleForTimeStamp) {
                        lowestScaleForTimeStamp = fieldDesc.getScale();
                    }
                }
            }

            /*
             * Loop through all fields and get type string; if this is a TIME or TIMESTAMP
             * field, use the lowest scale calculated above
             */
            for (index = 0; index < fieldDescs.length; index++) {
                TeradataColumnDesc fieldDesc = fieldDescs[index];
                fieldNames[index] = fieldDesc.getName();
                fieldTypes4Using[index] = fieldDesc.getTypeString4Using("UTF-8", lowestScaleForTime,
                        lowestScaleForTimeStamp);
            }

            Logger.logMessage(Logger.LogLevel.INFO,"Field Names: " + Arrays.toString(fieldNames));
            Logger.logMessage(Logger.LogLevel.INFO,"Field Types: " + Arrays.toString(fieldTypes4Using));
            return TeradataConnection.getUsingSQL(outputTableName, fieldNames, fieldTypes4Using, "UTF-8");
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        return null;
    }
}