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
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class TestPageview {

    @Test
    @FileParameters(
        value = "src/test/resources/pageview_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    public void testIsPageview(
        String test_description,
        String project,
        String dialect,
        String page_title,
        boolean is_pageview,
        boolean is_legacy_pageview,
        boolean is_app_pageview,
        String ip_address,
        String x_forwarded_for,
        String uri_host,
        String uri_path,
        String uri_query,
        String http_status,
        String content_type,
        String user_agent
    ) {
        PageviewDefinition PageviewDefinitionInstance = PageviewDefinition.getInstance();

        assertEquals(
            test_description,
            is_pageview,
            PageviewDefinitionInstance.isPageview(
                uri_host,
                uri_path,
                uri_query,
                http_status,
                content_type,
                user_agent
            )
        );
    }

    @Test
    @FileParameters(
        value = "src/test/resources/pageview_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    public void testIsAppPageview(
        String test_description,
        String project,
        String dialect,
        String page_title,
        boolean is_pageview,
        boolean is_legacy_pageview,
        boolean is_app_pageview,
        String ip_address,
        String x_forwarded_for,
        String uri_host,
        String uri_path,
        String uri_query,
        String http_status,
        String content_type,
        String user_agent
    ) {
        PageviewDefinition PageviewDefinitionInstance = PageviewDefinition.getInstance();

        assertEquals(
            test_description,
            is_app_pageview,
            PageviewDefinitionInstance.isAppPageview(
                uri_path,
                uri_query,
                content_type,
                user_agent
            )
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
            boolean is_app_pageview,
            String ip_address,
            String x_forwarded_for,
            String uri_host,
            String uri_path,
            String uri_query,
            String http_status,
            String content_type,
            String user_agent
    ) {
        PageviewDefinition PageviewDefinitionInstance = PageviewDefinition.getInstance();
        assertEquals(
                test_description,
                project,
                PageviewDefinitionInstance.getProjectFromHost(uri_host)
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
            boolean is_app_pageview,
            String ip_address,
            String x_forwarded_for,
            String uri_host,
            String uri_path,
            String uri_query,
            String http_status,
            String content_type,
            String user_agent
    ) {
        PageviewDefinition PageviewDefinitionInstance = PageviewDefinition.getInstance();
        if (is_pageview) {
            assertEquals(
                    test_description,
                    dialect,
                    PageviewDefinitionInstance.getLanguageVariantFromPath(uri_path)
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
            boolean is_app_pageview,
            String ip_address,
            String x_forwarded_for,
            String uri_host,
            String uri_path,
            String uri_query,
            String http_status,
            String content_type,
            String user_agent
    ) {
        PageviewDefinition PageviewDefinitionInstance = PageviewDefinition.getInstance();
        if (is_pageview) {
            assertEquals(
                    test_description,
                    page_title,
                    PageviewDefinitionInstance.getPageTitleFromUri(uri_path, uri_query)
            );
        }
    }
    @Test
    public void testIsPageviewXAnalyticsPreview(

    ){
        String uri_host = "en.wikipedia";
        String uri_path = "/wiki/Horseshoe%20crab#anchor"; ;
        String uri_query = "-";
        String http_status = "200";
        String content_type = "text/html";
        String user_agent = "turnip/";

        assertTrue("Preview requests are not pageviews", PageviewDefinition.getInstance().isPageview(
            uri_host,
            uri_path,
            uri_query,
            http_status,
            content_type,
            user_agent,
            "WMF-Last-Access=10-Jan-2016;preview=1"

        ) == false);

    }

    /**
     * We only accept value "1" for preview header
     */
    @Test
    public void testIsPageviewXAnalyticsPreviewBadHeaderValue(

    ){
        String uri_host = "en.wikipedia";
        String uri_path = "/wiki/Horseshoe%20crab#anchor"; ;
        String uri_query = "-";
        String http_status = "200";
        String content_type = "text/html";
        String user_agent = "turnip/";

        assertTrue("A bad value for preview request header should not be consider a preview", PageviewDefinition.getInstance().isPageview(
            uri_host,
            uri_path,
            uri_query,
            http_status,
            content_type,
            user_agent,
            "WMF-Last-Access=10-Jan-2016;preview=BAD;pageview=1"

        ) == true);

    }

    /**
     * If a request comes tagged as pageview is counted as such
     * regardless of uri_host, uri_path...
     */
    @Test
    public void testIsPageviewXAnalyticsPageviewTagged(

    ){
        String uri_host = "en.wikipedia";
        String uri_path = "blah"; ;
        String uri_query = "-";
        String http_status = "200";
        String content_type = "blah";
        String user_agent = "blah/";

        assertTrue("Request tagged as pageview in x-analytics should be consider pageviews", PageviewDefinition.getInstance().isPageview(
            uri_host,
            uri_path,
            uri_query,
            http_status,
            content_type,
            user_agent,
            "WMF-Last-Access=10-Jan-2016;pageview=1"

        ) == true);

    }
}