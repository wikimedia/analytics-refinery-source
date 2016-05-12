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

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;

@RunWith(JUnitParamsRunner.class)
public class TestIsLegacyPageviewUDF {


    @Test
    @FileParameters(
        value = "../refinery-core/src/test/resources/pageview_test_data.csv",
        mapper = CsvWithHeaderMapper.class
    )
    public void testIsPageview(
        String test_description,
        String project,
        String dialect,
        String pageTitle,
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
        IsLegacyPageviewUDF udf = new IsLegacyPageviewUDF();

        assertEquals(
            test_description,
            is_legacy_pageview,
            udf.evaluate(
                ip_address,
                x_forwarded_for,
                uri_host,
                uri_path,
                http_status
            )
        );
    }
}