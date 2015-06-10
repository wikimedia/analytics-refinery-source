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
import org.junit.Before;
import org.junit.Test;
import java.util.Map;


public class TestUAParserAppVersionRecognition extends TestCase {

    UAParser uaParser = null;

    @Before
    public void setUp() {
        uaParser = new UAParser();
    }

    @Test
    public void testAppRequest() {

        String ua1 = "WikipediaApp/2.0-r-2015-04-23 (Android 5.0.1; Phone) Google Play";
        Map<String, String> uaMap1 = uaParser.getUAMap(ua1);
        assertEquals("App version check", "2.0-r-2015-04-23", uaMap1.get("wmf_app_version"));

        String ua2 = "WikipediaApp/4.1.2 (iPhone OS 8.3; Tablet)";
        Map<String, String> uaMap2 = uaParser.getUAMap(ua2);
        assertEquals("App version check", "4.1.2", uaMap2.get("wmf_app_version"));

        String ua = "WikipediaApp/2.0-r-2015-04-23";
        Map<String, String> uaMap = uaParser.getUAMap(ua);
        assertEquals("App version check", "2.0-r-2015-04-23", uaMap.get("wmf_app_version"));
    }

    @Test
    public void testNonAppRequest() {

        String ua = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:19.0) Gecko/20100101 Firefox/19.0";
        Map<String, String> uaMap = uaParser.getUAMap(ua);
        assertEquals("App version check", uaParser.NA, uaMap.get("wmf_app_version"));
    }

}
