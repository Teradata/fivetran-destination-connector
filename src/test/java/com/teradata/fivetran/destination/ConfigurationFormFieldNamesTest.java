package com.teradata.fivetran.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import fivetran_sdk.v2.ConfigurationFormRequest;
import fivetran_sdk.v2.ConfigurationFormResponse;
import fivetran_sdk.v2.FormField;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for IDE-26752.
 *
 * <p>The Fivetran SDK (2.26.0908.001+) flattens the configuration form recursively and
 * rejects the connector with {@code SdkException: Duplicate configuration field name ...}
 * if any name occurs more than once. Previously {@code user}, {@code ssl.server.cert} and
 * {@code ssl.crc} were each emitted into two mutually exclusive conditional groups, which
 * aborted the tester before any destination RPC ran.
 */
public class ConfigurationFormFieldNamesTest {

    private ConfigurationFormResponse form() {
        AtomicReference<ConfigurationFormResponse> captured = new AtomicReference<>();
        new TeradataDestinationServiceImpl().configurationForm(
                ConfigurationFormRequest.newBuilder().build(),
                new StreamObserver<ConfigurationFormResponse>() {
                    @Override
                    public void onNext(ConfigurationFormResponse value) {
                        captured.set(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new IllegalStateException(t);
                    }

                    @Override
                    public void onCompleted() {
                    }
                });

        ConfigurationFormResponse response = captured.get();
        assertNotNull(response, "configurationForm did not emit a response");
        return response;
    }

    /**
     * Flattens the form the same way the SDK validator does: every top-level field plus,
     * recursively, every field nested inside a ConditionalFields group.
     */
    private List<FormField> flatten(List<FormField> fields) {
        List<FormField> flattened = new ArrayList<>();
        for (FormField field : fields) {
            flattened.add(field);
            if (field.hasConditionalFields()) {
                flattened.addAll(flatten(field.getConditionalFields().getFieldsList()));
            }
        }
        return flattened;
    }

    @Test
    public void fieldNamesAreUniqueAcrossFlattenedForm() {
        Set<String> seen = new HashSet<>();
        List<String> duplicates = new ArrayList<>();

        for (FormField field : flatten(form().getFieldsList())) {
            if (!seen.add(field.getName())) {
                duplicates.add(field.getName());
            }
        }

        assertTrue(duplicates.isEmpty(),
                "Configuration form contains duplicate field names: " + duplicates);
    }

    @Test
    public void everyFieldHasANonBlankName() {
        for (FormField field : flatten(form().getFieldsList())) {
            assertFalse(field.getName().trim().isEmpty(),
                    "Found a form field with a blank name (label: " + field.getLabel() + ")");
        }
    }

    @Test
    public void credentialsAndSslFieldsAreSplitPerBranch() {
        Set<String> names = new HashSet<>();
        for (FormField field : flatten(form().getFieldsList())) {
            names.add(field.getName());
        }

        assertTrue(names.contains("td2user"), "Expected TD2 branch to expose td2user");
        assertTrue(names.contains("ldapuser"), "Expected LDAP branch to expose ldapuser");
        assertFalse(names.contains("user"), "Shared 'user' field should no longer be emitted");

        assertTrue(names.contains("ssl.server.cert.verify.ca"));
        assertTrue(names.contains("ssl.server.cert.verify.full"));
        assertFalse(names.contains("ssl.server.cert"),
                "Shared 'ssl.server.cert' field should no longer be emitted");

        assertTrue(names.contains("ssl.crc.verify.ca"));
        assertTrue(names.contains("ssl.crc.verify.full"));
        assertFalse(names.contains("ssl.crc"), "Shared 'ssl.crc' field should no longer be emitted");
    }

    @Test
    public void branchSpecificKeysResolveToASingleConfiguration() {
        TeradataConfiguration td2 = new TeradataConfiguration(ImmutableMap.of(
                "host", "h", "logmech", "TD2", "td2user", "td2_user", "td2password", "p"));
        assertEquals("td2_user", td2.user());
        assertEquals("td2_user", td2.database(), "database should fall back to the resolved user");

        TeradataConfiguration ldap = new TeradataConfiguration(ImmutableMap.of(
                "host", "h", "logmech", "LDAP", "ldapuser", "ldap_user", "ldappassword", "p"));
        assertEquals("ldap_user", ldap.user());
    }

    @Test
    public void legacyKeysRemainSupported() {
        TeradataConfiguration conf = new TeradataConfiguration(ImmutableMap.<String, String>builder()
                .put("host", "h")
                .put("logmech", "TD2")
                .put("user", "legacy_user")
                .put("td2password", "p")
                .put("ssl.mode", "VERIFY-CA")
                .put("ssl.server.cert", "legacy_cert")
                .put("ssl.crc", "REQUIRE")
                .build());

        assertEquals("legacy_user", conf.user());
        assertEquals("legacy_cert", conf.sslServerCert());
        assertEquals("REQUIRE", conf.sslCrc());
    }

    @Test
    public void branchSpecificSslKeysTakePrecedenceOverLegacyKeys() {
        TeradataConfiguration verifyFull = new TeradataConfiguration(ImmutableMap.<String, String>builder()
                .put("host", "h")
                .put("logmech", "TD2")
                .put("td2user", "u")
                .put("td2password", "p")
                .put("ssl.mode", "VERIFY-FULL")
                .put("ssl.server.cert.verify.full", "full_cert")
                .put("ssl.crc.verify.full", "PREFER")
                .put("ssl.server.cert", "legacy")
                .put("ssl.crc", "REQUIRE")
                .build());

        assertEquals("full_cert", verifyFull.sslServerCert());
        assertEquals("PREFER", verifyFull.sslCrc());

        TeradataConfiguration verifyCa = new TeradataConfiguration(ImmutableMap.<String, String>builder()
                .put("host", "h")
                .put("logmech", "TD2")
                .put("td2user", "u")
                .put("td2password", "p")
                .put("ssl.mode", "VERIFY-CA")
                .put("ssl.server.cert.verify.ca", "ca_cert")
                .put("ssl.crc.verify.ca", "PREFER")
                .put("ssl.server.cert", "legacy")
                .put("ssl.crc", "REQUIRE")
                .build());

        assertEquals("ca_cert", verifyCa.sslServerCert());
        assertEquals("PREFER", verifyCa.sslCrc());
    }

    @Test
    public void inactiveBranchSslKeysAreIgnored() {
        // mode=VERIFY-FULL but only .verify.ca keys are populated (stale from a prior
        // ssl.mode selection): the inactive branch's keys must not leak through, and
        // resolution should fall back to the legacy keys instead of the wrong branch.
        TeradataConfiguration conf = new TeradataConfiguration(ImmutableMap.<String, String>builder()
                .put("host", "h")
                .put("logmech", "TD2")
                .put("td2user", "u")
                .put("td2password", "p")
                .put("ssl.mode", "VERIFY-FULL")
                .put("ssl.server.cert.verify.ca", "stale_ca_cert")
                .put("ssl.crc.verify.ca", "stale_ca_crc")
                .put("ssl.server.cert", "legacy_cert")
                .put("ssl.crc", "legacy_crc")
                .build());

        assertEquals("legacy_cert", conf.sslServerCert());
        assertEquals("legacy_crc", conf.sslCrc());
    }

    @Test
    public void ldapUserIgnoresStaleTd2User() {
        // A config map can retain a stale td2user from a previous TD2 selection even
        // after switching logmech to LDAP; the active branch (LDAP) must win.
        TeradataConfiguration conf = new TeradataConfiguration(ImmutableMap.<String, String>builder()
                .put("host", "h")
                .put("logmech", "LDAP")
                .put("td2user", "stale_td2_user")
                .put("ldapuser", "ldap_user")
                .put("ldappassword", "p")
                .build());

        assertEquals("ldap_user", conf.user());
    }

    @Test
    public void sslCrcDefaultsToAllowWhenAbsent() {
        TeradataConfiguration conf = new TeradataConfiguration(ImmutableMap.of(
                "host", "h", "logmech", "TD2", "td2user", "u", "td2password", "p"));

        assertEquals("ALLOW", conf.sslCrc());
    }
}
