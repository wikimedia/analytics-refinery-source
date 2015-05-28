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

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;

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
                    PageviewDefinitionInstance.getDialectFromPath(uri_path)
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

}