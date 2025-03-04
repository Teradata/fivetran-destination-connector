package com.teradata.fivetran.destination;

import java.util.Map;

public class TeradataConfiguration {
    private final String host;
    private final String database;
    private final String user;
    private final String password;

    /**
     * Constructs a TeradataConfiguration object using the provided configuration map.
     *
     * @param conf The configuration map containing the connection details.
     */
    public TeradataConfiguration(Map<String, String> conf) {
        this.host = conf.get("host");
        this.database = getOrDefault(conf.get("database"), null);
        this.user = conf.get("user");
        this.password = conf.get("password");
    }

    /**
     * Returns the value if it is not null or empty, otherwise returns the default value.
     *
     * @param value The value to check.
     * @param defaultValue The default value to return if the value is null or empty.
     * @return The value or the default value.
     */
    private String getOrDefault(String value, String defaultValue) {
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }

    /**
     * Returns the host.
     *
     * @return The host.
     */
    public String host() {
        return host;
    }

    /**
     * Returns the database.
     *
     * @return The database.
     */
    public String database() {
        return database;
    }

    /**
     * Returns the user.
     *
     * @return The user.
     */
    public String user() {
        return user;
    }

    /**
     * Returns the password.
     *
     * @return The password.
     */
    public String password() {
        return password;
    }
}