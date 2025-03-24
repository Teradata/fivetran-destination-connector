package com.teradata.fivetran.destination;

import java.util.Map;

public class TeradataConfiguration {
    private final String host;
    private final String logmech;
    private final String database;
    private final String user;
    private final String password;
    private final String TMODE;
    private final String sslMode;
    private final String sslServerCert;
    private final String driverParameters;
    private final Integer batchSize;
    private final String queryBand;

    /**
     * Constructs a TeradataConfiguration object using the provided configuration map.
     *
     * @param conf The configuration map containing the connection details.
     */
    public TeradataConfiguration(Map<String, String> conf) {
        this.host = conf.get("host");
        this.database = getOrDefault(conf.get("database"), null);
        this.logmech = conf.get("logmech");
        this.user = conf.get("user");
        this.password = conf.get("password");
        this.TMODE = conf.get("TMODE");
        this.sslMode = getOrDefault(conf.get("ssl.mode"), "DISABLE");
        this.sslServerCert = getOrDefault(conf.get("ssl.server.cert"), null);
        this.driverParameters = getOrDefault(conf.get("driver.parameters"), null);
        this.batchSize = Integer.valueOf(getOrDefault(conf.get("batch.size"), "10000"));
        this.queryBand = getOrDefault(conf.get("query.band"), "org=teradata-internal-telem;appname=fivetran;");
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

    public String logmech() {
        return logmech;
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

    public String TMODE() { return TMODE; }

    public String sslMode() {
        return sslMode;
    }

    public String sslServerCert() {
        return sslServerCert;
    }

    public String driverParameters() {
        return driverParameters;
    }

    /**
     * Returns the batch size.
     *
     * @return The batch size.
    */
    public Integer batchSize() {
        return batchSize;
    }

    public String queryBand(){ return queryBand; }
}