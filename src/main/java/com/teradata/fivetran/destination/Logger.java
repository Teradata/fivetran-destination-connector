package com.teradata.fivetran.destination;

import org.apache.commons.lang3.StringEscapeUtils;

public class Logger {
    // Enum to define different log levels
    public enum LogLevel {
        INFO,       // Informational messages
        WARNING,    // Warning messages
        SEVERE,     // Severe error messages
        DISABLED    // Special value to disable logging
    }

    // Default log level for debugging
    public static LogLevel debugLogLevel = LogLevel.DISABLED;

    /**
     * Logs a message at the specified log level.
     *
     * @param level   The log level (INFO, WARNING, SEVERE, or DISABLED).
     * @param message The message to log.
     */
    public static void logMessage(LogLevel level, String message) {
        // Uncomment the line below to disable all logs
        // level = LogLevel.DISABLED;

        // If logging is disabled, return immediately
        if (level == LogLevel.DISABLED) {
            return;
        }

        // Escape special characters in the message for safe logging
        message = StringEscapeUtils.escapeJava(message);

        // Print the log message in JSON format
        System.out.println(String.format(
                "{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}",
                level.name(),
                message
        ));
    }
}