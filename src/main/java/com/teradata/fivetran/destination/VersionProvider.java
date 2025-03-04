package com.teradata.fivetran.destination;

import java.io.IOException;
import java.util.Properties;

public class VersionProvider {

    private static final String VERSION;

    static {
        VERSION = loadVersion();
    }

    /**
     * Loads the version from the properties file.
     *
     * @return The version string.
     */
    private static String loadVersion() {
        String versionProperty = "unknown";
        try {
            Properties props = new Properties();
            props.load(VersionProvider.class.getResourceAsStream("/version.properties"));
            versionProperty = props.getProperty("version", versionProperty).trim();
        } catch (IOException ex) {
            // Log the exception if needed
        }
        return versionProperty;
    }

    /**
     * Returns the version.
     *
     * @return The version string.
     */
    public static String getVersion() {
        return VERSION;
    }
}