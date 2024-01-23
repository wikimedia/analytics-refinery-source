// Copyright 2014 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.core;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import java.util.HashMap;

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class TestPageview {

    private PageviewDefinition pageviewDefinition = PageviewDefinition.getInstance();

    @Test
    @FileParameters(
        value = "src/test/resources/pageview_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    @Deprecated
    public void testIsPageviewNoHeader(
        String test_description,
        String project,
        String dialect,
        String page_title,
        boolean is_pageview,
        boolean is_legacy_pageview,
        String ip_address,
        String x_forwarded_for,
        String uri_host,
        String uri_path,
        String uri_query,
        String http_status,
        String content_type,
        String user_agent,
        String x_analytics_header
    ) {

        WebrequestData webrequest = new WebrequestData(uri_host,
            uri_path,
            uri_query,
            http_status,
            content_type,
            user_agent,
            new HashMap<>()) ;

        if (x_analytics_header.isEmpty())
            assertEquals(
                test_description,
                is_pageview,
                pageviewDefinition.isPageview(webrequest)
            );
    }

    @Test
    @FileParameters(
            value = "src/test/resources/pageview_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    public void testIsPageviewHeader(
            String test_description,
            String project,
            String dialect,
            String page_title,
            boolean is_pageview,
            boolean is_legacy_pageview,
            String ip_address,
            String x_forwarded_for,
            String uri_host,
            String uri_path,
            String uri_query,
            String http_status,
            String content_type,
            String user_agent,
            String x_analytics_header_str
    ) throws HiveException {

        WebrequestData webrequest = new WebrequestData(uri_host,
            uri_path,
            uri_query,
            http_status,
            content_type,
            user_agent,
            Utils.parseXAnalyticsHeader(x_analytics_header_str)) ;

        assertEquals(
                test_description,
                is_pageview,
                pageviewDefinition.isPageview(webrequest)
        );
    }

    @Test
    @FileParameters(
            value = "src/test/resources/pageview_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    public void testGetProjectFromHost(
            String test_description,
            String project,
            String dialect,
            String page_title,
            boolean is_pageview,
            boolean is_legacy_pageview,
            String ip_address,
            String x_forwarded_for,
            String uri_host,
            String uri_path,
            String uri_query,
            String http_status,
            String content_type,
            String user_agent,
            String x_analytics_header
    ) {
        assertEquals(
                test_description,
                project,
                Webrequest.getProjectFromHost(uri_host)
        );
    }

    @Test
    @FileParameters(
            value = "src/test/resources/pageview_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    public void testGetDialectFromPath(
            String test_description,
            String project,
            String dialect,
            String page_title,
            boolean is_pageview,
            boolean is_legacy_pageview,
            String ip_address,
            String x_forwarded_for,
            String uri_host,
            String uri_path,
            String uri_query,
            String http_status,
            String content_type,
            String user_agent,
            String x_analytics_header
    ) {
        if (is_pageview) {
            assertEquals(
                    test_description,
                    dialect,
                    pageviewDefinition.getLanguageVariantFromPath(uri_path)
            );
        }
    }

    @Test
    @FileParameters(
            value = "src/test/resources/pageview_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    public void testGetPageTitleFromUri(
            String test_description,
            String project,
            String dialect,
            String page_title,
            boolean is_pageview,
            boolean is_legacy_pageview,
            String ip_address,
            String x_forwarded_for,
            String uri_host,
            String uri_path,
            String uri_query,
            String http_status,
            String content_type,
            String user_agent,
            String x_analytics_header
    ) {
        if (is_pageview) {
            assertEquals(
                    test_description,
                    page_title,
                    pageviewDefinition.getPageTitleFromUri(uri_path, uri_query)
            );
        }
    }

    @Test
    public void testIsRedirectToPageview() {

        WebrequestData webrequest = new WebrequestData("en.wikipedia.org",
            "/wiki/SomePage",
            "",
            "302",
            "text/html",
            "some",
            new HashMap<>()) ;

        assertEquals(false, pageviewDefinition.isPageview(webrequest));
        assertEquals(true, pageviewDefinition.isRedirectToPageview(webrequest));


    }

    @Test
    public void testIsRedirectToPageviewNoContentType() {

        WebrequestData webrequest = new WebrequestData("en.wikipedia.org",
            "/wiki/SomePage",
            "",
            "302",
            "-",
            "some",
            new HashMap<>()) ;

        assertEquals(false, pageviewDefinition.isPageview(webrequest));
        assertEquals(true, pageviewDefinition.isRedirectToPageview(webrequest));


    }

    @Test
    public void testWikipedia15IsNotPageview() throws HiveException {

        WebrequestData webrequest = new WebrequestData(
            "15.wikipedia.org",
            "/",
            "?search=something",
            "200",
            "text/html",
            "Mozilla/5.0...",
            Utils.parseXAnalyticsHeader("https=1"));

        assertEquals(false, pageviewDefinition.isPageview(webrequest));
    }

    @Test
    public void testWikidataQueryIsNotPageview() throws HiveException {

        WebrequestData webrequest = new WebrequestData(
            "query.wikidata.org",
            "/wiki/SomePage",
            "?search=something",
            "200",
            "text/html",
            "Mozilla/5.0...",
            Utils.parseXAnalyticsHeader("https=1"));

        assertEquals(false, pageviewDefinition.isPageview(webrequest));
    }

    @Test
    public void testPageTitlesWithSpecialCharacters() {

        assertEquals(
                "Page title with \\n end-of-line in uri_path",
                PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE,
                pageviewDefinition.getPageTitleFromUri("/wiki/wrong\ntitle", "")
        );

        assertEquals(
                "Page title with trailing \\n end-of-line in uri_path",
                PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE,
                pageviewDefinition.getPageTitleFromUri("/wiki/wrong-title\n", "")
        );

        assertEquals(
                "Page title with \\r end-of-line in uri_path",
                PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE,
                pageviewDefinition.getPageTitleFromUri("/wiki/wrong\rtitle", "")
        );

        assertEquals(
                "Page title with tabulation in uri_path",
                PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE,
                pageviewDefinition.getPageTitleFromUri("/wiki/wrong\ttitle", "")
        );

        assertEquals(
                "Page title with \\n end-of-line in uri_query",
                PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE,
                pageviewDefinition.getPageTitleFromUri("/w/index.php", "?title=wrong\ntitle")
        );

        assertEquals(
                "Page title with trailing \\n end-of-line in uri_query",
                PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE,
                pageviewDefinition.getPageTitleFromUri("/w/index.php", "?title=wrong-title\n")
        );

        assertEquals(
                "Page title with \\r end-of-line in uri_query",
                PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE,
                pageviewDefinition.getPageTitleFromUri("/w/index.php", "?title=wrong\rtitle")
        );

        assertEquals(
                "Page title with tabulation in uri_query",
                PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE,
                pageviewDefinition.getPageTitleFromUri("/w/index.php", "?title=wrong\ttitle")
        );

    }

    @Test
    public void testPageTitleValidation() {

        assertTrue(
                "Regular Page title",
                pageviewDefinition.isValidPageTitle("A_Valid_page_title")
        );

        assertFalse(
                "Page title with \\n end-of-line in the middle",
                pageviewDefinition.isValidPageTitle("wrong\ntitle")
        );

        assertFalse(
                "Page title with \\n end-of-line at the end",
                pageviewDefinition.isValidPageTitle("wrong-title\n")
        );

        assertFalse(
                "Page title with \\n end-of-line in the front",
                pageviewDefinition.isValidPageTitle("\nwrong-title")
        );

    }

}
