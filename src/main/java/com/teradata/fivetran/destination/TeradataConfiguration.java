package com.teradata.fivetran.destination;

import java.util.Map;

public class TeradataConfiguration {
    private final String host;
    private final String logmech;
    private final String database;
    private final String user;
    private final String td2password;
    private final String ldappassword;
    private final String tmode;
    private final boolean useFastLoad;
    private static int defaultVarcharSize = 256; // Default value for TMODE
    private static String varcharCharacterSet = "LATIN"; // Default value for VARCHAR character set
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
        this.user = conf.get("user");
        this.logmech = conf.get("logmech");
        this.td2password = conf.get("td2password");
        this.ldappassword = conf.get("ldappassword");
        this.database = getOrDefault(conf.get("database"), conf.get("user"));
        this.tmode = getOrDefault(conf.get("tmode"), "DEFAULT");
        this.useFastLoad = Boolean.parseBoolean(getOrDefault(conf.get("use.fastload"), "false"));
        defaultVarcharSize = Integer.parseInt(getOrDefault(conf.get("default.varchar.size"), "256"));
        varcharCharacterSet = getOrDefault(conf.get("varchar.character.set"), "LATIN");
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
        if (logmech !=null && logmech.equals("LDAP")) {
            return ldappassword;
        }
        return td2password;
    }

    public String tmode() { return tmode; }

    public static int defaultVarcharSize() {
        return defaultVarcharSize;
    }

    public static String varcharCharacterSet() {
        return varcharCharacterSet;
    }

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

    public boolean useFastLoad() {
        return useFastLoad;
    }

    public String queryBand(){ return queryBand; }
}