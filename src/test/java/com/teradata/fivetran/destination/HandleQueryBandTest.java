package com.teradata.fivetran.destination;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HandleQueryBandTest {

    @Test
    void emptyInput_shouldAddOrgAndAppname() {
        String result = TeradataJDBCUtil.handleQueryBand("");
        assertEquals(
                "org=teradata-internal-telem;appname=fivetran;",
                result
        );
    }

    @Test
    void onlyAppname_shouldAddOrgFirst_andAppendFivetran() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=myapp;");
        assertEquals(
                "org=teradata-internal-telem;appname=myapp_fivetran;",
                result
        );
    }

    @Test
    void onlyOrg_shouldAddAppname() {
        String result = TeradataJDBCUtil.handleQueryBand("org=myorg;");
        assertEquals(
                "org=myorg;appname=fivetran;",
                result
        );
    }

    @Test
    void appnameAlreadyContainsFivetran_shouldNotDuplicate() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=my_fivetran_app;");
        assertEquals(
                "org=teradata-internal-telem;appname=my_fivetran_app;",
                result
        );
    }

    @Test
    void bothPresent_inCorrectOrder_shouldPreserveAndNormalize() {
        String result = TeradataJDBCUtil.handleQueryBand("org=abc;appname=test;");
        assertEquals(
                "org=abc;appname=test_fivetran;",
                result
        );
    }

    @Test
    void bothPresent_inReverseOrder_shouldReorder() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=test;org=abc;");
        assertEquals(
                "org=abc;appname=test_fivetran;",
                result
        );
    }

    @Test
    void customKeys_shouldBePreserved_afterOrgAndAppname() {
        String result = TeradataJDBCUtil.handleQueryBand("foo=1;bar=2;");
        assertEquals(
                "org=teradata-internal-telem;appname=fivetran;foo=1;bar=2;",
                result
        );
    }

    @Test
    void orgAppnameAndCustomKeys_shouldKeepOrder() {
        String result = TeradataJDBCUtil.handleQueryBand("bar=2;appname=test;foo=1;");
        assertEquals(
                "org=teradata-internal-telem;appname=test_fivetran;bar=2;foo=1;",
                result
        );
    }

    @Test
    void trailingSemicolons_shouldNotBreakParsing() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=test;;;");
        assertEquals(
                "org=teradata-internal-telem;appname=test_fivetran;",
                result
        );
    }

    @Test
    void multipleCustomKeys_shouldPreserveRelativeOrder() {
        String result = TeradataJDBCUtil.handleQueryBand("z=9;y=8;x=7;");
        assertEquals(
                "org=teradata-internal-telem;appname=fivetran;z=9;y=8;x=7;",
                result
        );
    }

    @Test
    void appnameWithoutSemicolon_shouldStillWork() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=test");
        assertEquals(
                "org=teradata-internal-telem;appname=test_fivetran;",
                result
        );
    }

    @Test
    void orgWithoutSemicolon_shouldStillWork() {
        String result = TeradataJDBCUtil.handleQueryBand("org=myorg");
        assertEquals(
                "org=myorg;appname=fivetran;",
                result
        );
    }

    @Test
    void appnameWithUpperCaseFivetran_shouldNotAppend() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=FIVETRAN_APP;");
        assertEquals(
                "org=teradata-internal-telem;appname=FIVETRAN_APP;",
                result
        );
    }

    @Test
    void orgAtEnd_shouldBeMovedToFirst() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=test;foo=1;bar=2;org=myorg;");
        assertEquals(
                "org=myorg;appname=test_fivetran;foo=1;bar=2;",
                result
        );
    }

    // Test for null input
    @Test
    void nullInput_shouldAddOrgAndAppname() {
        String result = TeradataJDBCUtil.handleQueryBand(null);
        assertEquals(
                "org=teradata-internal-telem;appname=fivetran;",
                result
        );
    }

    // Test for case insensitive "fivetran" check in appname value
    @Test
    void appnameWithUpperCaseFIVETRAN_shouldNotAppend() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=FIVETRAN_APP;");
        assertEquals(
                "org=teradata-internal-telem;appname=FIVETRAN_APP;",
                result
        );
    }

    @Test
    void appnameWithMixedCaseFivetran_shouldNotAppend() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=Fivetran_APP;");
        assertEquals(
                "org=teradata-internal-telem;appname=Fivetran_APP;",
                result
        );
    }

    @Test
    void appnameWithFIVETRANInMiddle_shouldNotAppend() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=myFIVETRANapp;");
        assertEquals(
                "org=teradata-internal-telem;appname=myFIVETRANapp;",
                result
        );
    }

    // Test for case insensitive key handling - APPNAME
    @Test
    void uppercaseAPPNAME_shouldBeRecognized() {
        String result = TeradataJDBCUtil.handleQueryBand("APPNAME=test;");
        assertEquals(
                "org=teradata-internal-telem;appname=test_fivetran;",
                result
        );
    }

    @Test
    void mixedCaseAppName_shouldBeRecognized() {
        String result = TeradataJDBCUtil.handleQueryBand("AppName=test;");
        assertEquals(
                "org=teradata-internal-telem;appname=test_fivetran;",
                result
        );
    }

    // Test for case insensitive key handling - ORG
    @Test
    void uppercaseORG_shouldBeRecognized() {
        String result = TeradataJDBCUtil.handleQueryBand("ORG=myorg;");
        assertEquals(
                "org=myorg;appname=fivetran;",
                result
        );
    }

    @Test
    void mixedCaseOrg_shouldBeRecognized() {
        String result = TeradataJDBCUtil.handleQueryBand("Org=myorg;");
        assertEquals(
                "org=myorg;appname=fivetran;",
                result
        );
    }

    @Test
    void uppercaseORGAndAPPNAME_shouldBeRecognized() {
        String result = TeradataJDBCUtil.handleQueryBand("ORG=myorg;APPNAME=test;");
        assertEquals(
                "org=myorg;appname=test_fivetran;",
                result
        );
    }

    // Test for duplicate keys (case insensitive) - last one wins
    @Test
    void duplicateAppname_lastOneWins() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=first;appname=second;");
        assertEquals(
                "org=teradata-internal-telem;appname=second_fivetran;",
                result
        );
    }

    @Test
    void duplicateOrg_lastOneWins() {
        String result = TeradataJDBCUtil.handleQueryBand("org=first;org=second;");
        assertEquals(
                "org=second;appname=fivetran;",
                result
        );
    }

    @Test
    void duplicateCaseInsensitive_lastOneWins() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=first;APPNAME=second;AppName=third;");
        assertEquals(
                "org=teradata-internal-telem;appname=third_fivetran;",
                result
        );
    }

    @Test
    void duplicateOrgCaseInsensitive_lastOneWins() {
        String result = TeradataJDBCUtil.handleQueryBand("org=first;ORG=second;Org=third;");
        assertEquals(
                "org=third;appname=fivetran;",
                result
        );
    }

    @Test
    void duplicateCustomKey_lastOneWins() {
        String result = TeradataJDBCUtil.handleQueryBand("foo=1;foo=2;foo=3;");
        assertEquals(
                "org=teradata-internal-telem;appname=fivetran;foo=3;",
                result
        );
    }

    // Test for special characters and edge cases
    @Test
    void valueContainsEquals_shouldHandleCorrectly() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=test;foo=bar=baz;");
        assertEquals(
                "org=teradata-internal-telem;appname=test_fivetran;foo=bar=baz;",
                result
        );
    }

    @Test
    void valueContainsMultipleEquals_shouldHandleCorrectly() {
        String result = TeradataJDBCUtil.handleQueryBand("param=a=b=c=d;");
        assertEquals(
                "org=teradata-internal-telem;appname=fivetran;param=a=b=c=d;",
                result
        );
    }

    @Test
    void valueWithSpecialCharacters_shouldPreserve() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=test-app_123;foo=bar@baz.com;");
        assertEquals(
                "org=teradata-internal-telem;appname=test-app_123_fivetran;foo=bar@baz.com;",
                result
        );
    }

    @Test
    void emptyValue_shouldBePreserved() {
        String result = TeradataJDBCUtil.handleQueryBand("appname=;foo=bar;");
        assertEquals(
                "org=teradata-internal-telem;appname=_fivetran;foo=bar;",
                result
        );
    }

    @Test
    void whitespaceInValues_shouldBeTrimmed() {
        String result = TeradataJDBCUtil.handleQueryBand("appname= test ;org= myorg ;");
        assertEquals(
                "org=myorg;appname=test_fivetran;",
                result
        );
    }

    @Test
    void whitespaceInKeys_shouldBeTrimmed() {
        String result = TeradataJDBCUtil.handleQueryBand(" appname =test; org =myorg;");
        assertEquals(
                "org=myorg;appname=test_fivetran;",
                result
        );
    }
}
