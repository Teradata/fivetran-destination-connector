package com.teradata.fivetran.destination;

import java.io.IOException;
import java.util.Properties;

public class VersionProvider {

    // Static variable to hold the version string
    private static final String VERSION;

    // Static block to initialize the VERSION variable
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
            // Load the properties file from the classpath
            props.load(VersionProvider.class.getResourceAsStream("/version.properties"));
            // Retrieve the version property, defaulting to "unknown" if not found
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