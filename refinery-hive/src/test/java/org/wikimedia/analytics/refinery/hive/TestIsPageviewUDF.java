/**
 * Copyright (C) 2014  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.analytics.refinery.hive;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.json.simple.parser.JSONParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class TestIsPageviewUDF {
    IsPageviewUDF udf = null;
    JSONParser parser = null;

    @Before
    public void setUp() throws HiveException{
        udf = new IsPageviewUDF();
        parser = new JSONParser();

    }

    @Test
    @FileParameters(
        value = "../refinery-core/src/test/resources/pageview_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    // this mapper cannot deal with reading strings formed like
    // the x analytics map: "{"WMF-Last-Access":"12-Aug-2015","https":"1"}
    public void testIsPageview(
        String test_description,
        String project,
        String dialect,
        String pageTitle,
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
    ){

        Map<String, String> xAnalyticsMap = new HashMap<String, String>();

        assertEquals(
            test_description,
            is_pageview,
            udf.isPageview(
                uri_host,
                uri_path,
                uri_query,
                http_status,
                content_type,
                user_agent,
                xAnalyticsMap

            )
        );
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

        Map<String, String> xAnalyticsMap = new HashMap<String, String>();

        xAnalyticsMap.put("preview", "1");

        assertTrue("Preview requests are not pageviews", udf.isPageview(
            uri_host,
            uri_path,
            uri_query,
            http_status,
            content_type,
            user_agent,
            xAnalyticsMap

        ) == false);
    }


}