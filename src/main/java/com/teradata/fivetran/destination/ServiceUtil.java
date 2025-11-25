package com.teradata.fivetran.destination;

import com.teradata.fivetran.destination.writers.Writer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class ServiceUtil {

    public static void setTimeZoneToUTCIfNeeded(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("HELP SESSION;")) {

            if (rs.next()) {
                String tz = rs.getString("Session Time Zone");
                Logger.logMessage(Logger.LogLevel.INFO, "Current TIME ZONE: " + tz);

                if (tz != null && !"00:00".equals(tz.trim())) {
                    Logger.logMessage(Logger.LogLevel.INFO, "Setting TIME ZONE INTERVAL '0:00' HOUR TO MINUTE");
                    stmt.execute("SET TIME ZONE INTERVAL '0:00' HOUR TO MINUTE");
                }
            }
        }
    }

    public static String getStackTraceOneLine(Exception ex) {
        StringBuilder sb = new StringBuilder();
        sb.append(ex.getClass().getName()).append(": ").append(ex.getMessage()).append(" | ");
        for (StackTraceElement element : ex.getStackTrace()) {
            sb.append("at ").append(element.toString()).append(" | ");
        }
        return sb.toString();
    }

    public static void processFiles(Writer writer, List<String> files, String logMessage) throws Exception {
        Logger.logMessage(Logger.LogLevel.INFO, logMessage + ": " + files.size());
        for (String file : files) {
            writer.write(file);
        }
    }
}
