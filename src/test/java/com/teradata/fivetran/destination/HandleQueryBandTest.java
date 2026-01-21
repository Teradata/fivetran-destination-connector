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
    void mixedCaseKeys_shouldBeHandledAsDistinct() {
        String result = TeradataJDBCUtil.handleQueryBand("Org=abc;AppName=test;");
        assertEquals(
                "org=teradata-internal-telem;appname=fivetran;Org=abc;AppName=test;",
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
}
