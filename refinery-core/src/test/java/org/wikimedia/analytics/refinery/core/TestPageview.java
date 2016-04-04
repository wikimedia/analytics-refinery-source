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
        String uri_host = "en.wikipedia.org";
        String uri_path = "/wiki/Horseshoe%20crab#anchor"; ;
        String uri_query = "-";
        String http_status = "200";
        String content_type = "text/html";
        String user_agent = "Mozilla/4.0";

        // first make sure this request is a pageview
        assertTrue("Should be a pageview", PageviewDefinition.getInstance().isPageview(
            uri_host,
            uri_path,
            uri_query,
            http_status,
            content_type,
            user_agent,
            "WMF-Last-Access=10-Jan-2016;"

        ) == true);

        //bad preview header value, still pageview
        assertTrue("Bad preview header value, still pageview", PageviewDefinition.getInstance().isPageview(
                uri_host,
                uri_path,
                uri_query,
                http_status,
                content_type,
                user_agent,
                "WMF-Last-Access=10-Jan-2016;preview=jajaj"

        ) == true);

        // add preview header, we only accept "1"
        assertTrue("Add preview header, still no pageview", PageviewDefinition.getInstance().isPageview(
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
     * If a request comes tagged as pageview is not counted as such
     * unless is a request for an app (pageview tagging is restricted to apps
     * for now)
     **/
    @Test
    public void testIsPageviewXAnalyticsPageviewTagged(

    ){
        String uri_host = "en.wikipedia.org";
        String uri_path = "/wiki/Horseshoe%20crab#anchor"; ;
        String uri_query = "-";
        String http_status = "200";
        String content_type = "text/html";
        String user_agent = "Mozilla/4.0";

       //first make sure request is a pageview
        assertTrue("Should be a pageview", PageviewDefinition.getInstance().isPageview(
                uri_host,
                uri_path,
                uri_query,
                http_status,
                content_type,
                user_agent,
                "WMF-Last-Access=10-Jan-2016;"

        ) == true);

        //change something so it is not a pageview request
        assertTrue("Should not be a pageview", PageviewDefinition.getInstance().isPageview(
                uri_host,
                uri_path,
                uri_query,
                "500",
                content_type,
                user_agent,
                "WMF-Last-Access=10-Jan-2016;"

        ) == false);

        //tagging it as pageview should not make this request a pageview, it is not
        //an app request
        assertTrue("Not a pageview,tagging it as pageview should not change anything", PageviewDefinition.getInstance().isPageview(
            uri_host,
            uri_path,
            uri_query,
                "500",
            content_type,
            user_agent,
            "WMF-Last-Access=10-Jan-2016;pageview=1"

        ) == false);

    }

    /**
     * Apps pageviews that come tagged as 'pageview' are counted as such
     * regardless of url
     */
    @Test
    public void testIsPageviewXAnalyticsPageviewTaggedAppsPageview(

    ){
        String uri_path = "api.php"; ;
        String uri_query = "sections=0";
        String content_type = "application/json";
        String user_agent = "WikipediaApp";



        //first make sure this is a pageview
        assertTrue("Should be an app pageview", PageviewDefinition.getInstance().isAppPageview(

                uri_path,
                uri_query,
                content_type,
                user_agent,
                "WMF-Last-Access=10-Jan-2016;"

        ) == true);

        //change something so it is not an app pageview
        assertTrue("Not an app pageview", PageviewDefinition.getInstance().isAppPageview(

                uri_path,
                "garbage",
                content_type,
                user_agent,
                "WMF-Last-Access=10-Jan-2016;"

        ) == false);

        //tagging it as pageview should make it so
        assertTrue("App request tagged as pageview in x-analytics should be a pageview regardless of url",
                PageviewDefinition.getInstance().isAppPageview(
                uri_path,
                "garbage",
                content_type,
                user_agent,
                "WMF-Last-Access=10-Jan-2016;pageview=1"

        ) == true);

    }


}