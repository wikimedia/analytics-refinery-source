/**
 * Copyright (C) 2015 Wikimedia Foundation
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

package org.wikimedia.analytics.refinery.core;

import junit.framework.TestCase;
import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

/**
 * We test the most popular browser + device combos (from sampled logs)
 * and the ua parser reporting on on those.
 * <p/>
 * Test failing will indicate than the newer version of ua parser
 * is significantly different from the prior one.
 */
@RunWith(JUnitParamsRunner.class)
public class TestUAParserUserAgentMostPopular extends TestCase {

    UAParser uaParser = null;
    JSONParser jsonParser = null;

    @Before
    public void setUp() {
        uaParser = new UAParser();
        jsonParser = new JSONParser();
    }

    @Test
    @FileParameters(
                value = "src/test/resources/ua_most_popular_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    public void testMatchingOfMostPopularUA(String uaString, String jsonMapResult) throws ParseException {


        // decode expected output and turn it into an object
        Object obj = jsonParser.parse(jsonMapResult);
        JSONObject expected_ua = (JSONObject) obj;

        // Get computed output
        Map<String, String> computed_ua = uaParser.getUAMap(uaString);

        assertEquals("OS name check", expected_ua.get("os_family"),
                computed_ua.get("os_family"));

        assertEquals("OS major version check", expected_ua.get("os_major"),
                computed_ua.get("os_major"));

        assertEquals("OS minor version check", expected_ua.get("os_minor"),
                computed_ua.get("os_minor"));

        assertEquals("browser check", expected_ua.get("browser_family"),
                computed_ua.get("browser_family"));

        assertEquals("browser major version check", expected_ua.get("browser_major"),
                computed_ua.get("browser_major"));

        assertEquals("device check", expected_ua.get("device_family"),
                computed_ua.get("device_family"));

    }

}