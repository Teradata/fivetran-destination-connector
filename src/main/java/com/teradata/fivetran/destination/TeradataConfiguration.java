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
    private final String sslCrc;
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
        this.logmech = conf.get("logmech");
        // The configuration form must use distinct field names per logon-mechanism branch
        // (the Fivetran SDK rejects duplicate names across the flattened form), so the
        // branch-specific keys are coalesced here based on the selected logmech, mirroring
        // password() below. The legacy "user" key is still honored so configurations saved
        // before the split keep working.
        this.user = firstNonEmpty(
                "LDAP".equals(this.logmech) ? conf.get("ldapuser") : conf.get("td2user"),
                conf.get("user"));
        this.td2password = conf.get("td2password");
        this.ldappassword = conf.get("ldappassword");
        this.database = getOrDefault(conf.get("database"), this.user);
        this.tmode = getOrDefault(conf.get("tmode"), "DEFAULT");
        this.useFastLoad = Boolean.parseBoolean(getOrDefault(conf.get("use.fastload"), "false"));
        defaultVarcharSize = Integer.parseInt(getOrDefault(conf.get("default.varchar.size"), "256"));
        varcharCharacterSet = getOrDefault(conf.get("varchar.character.set"), "LATIN");
        this.sslMode = getOrDefault(conf.get("ssl.mode"), "DISABLE");
        // Same per-branch split as user/password above: resolve by the selected sslMode so a
        // configuration with both VERIFY-CA and VERIFY-FULL keys present doesn't always pick
        // one value regardless of which branch is actually active. bySslMode() only
        // distinguishes VERIFY-CA vs VERIFY-FULL; for any other sslMode these fields aren't
        // shown in the form, so the branch chosen is irrelevant (both keys will be absent).
        this.sslCrc = getOrDefault(
                firstNonEmpty(bySslMode(conf, "ssl.crc"), conf.get("ssl.crc")),
                "ALLOW");
        this.sslServerCert = firstNonEmpty(bySslMode(conf, "ssl.server.cert"), conf.get("ssl.server.cert"));
        this.driverParameters = getOrDefault(conf.get("driver.parameters"), null);
        this.batchSize = Integer.valueOf(getOrDefault(conf.get("batch.size"), "10000"));
        this.queryBand = getOrDefault(conf.get("query.band"), "org=teradata-internal-telem;appname=fivetran;");
    }

    /**
     * Resolves a key that is split per SSL-verification branch (".verify.ca" /
     * ".verify.full"), picking the suffix that matches the currently selected sslMode.
     * Only VERIFY-CA and VERIFY-FULL expose these fields in the form; any other sslMode
     * (DISABLE, ALLOW, PREFER, REQUIRE, or unset) falls back to the VERIFY-CA suffix, but
     * that branch is never populated for those modes, so the choice has no effect.
     *
     * @param conf The configuration map containing the connection details.
     * @param baseKey The shared key prefix, e.g. "ssl.crc" or "ssl.server.cert".
     * @return The value for the branch matching sslMode, or null if absent.
     */
    private String bySslMode(Map<String, String> conf, String baseKey) {
        String suffix = "VERIFY-FULL".equals(this.sslMode) ? ".verify.full" : ".verify.ca";
        return conf.get(baseKey + suffix);
    }

    /**
     * Returns the first value that is neither null nor empty, or null if there is none.
     *
     * @param values The values to check, in priority order.
     * @return The first non-empty value, or null.
     */
    private static String firstNonEmpty(String... values) {
        for (String value : values) {
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return null;
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

    public Object sslCrc() {
        return sslCrc;
    }
}