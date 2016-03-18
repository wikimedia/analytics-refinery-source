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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestUAParserUserAgentRecognition extends TestCase {

    UAParser uaParser = null;

    @Before
    public void setUp() {
        uaParser = new UAParser();
    }

    @Test
    public void testHappyCase() {

        String ua1 = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:19.0) Gecko/20100101 Firefox/19.0";
        String ua2 = "Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405";
        String ua3 = "Mozilla/5.0 (iPad; CPU OS 7_0_3 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B511 Safari/9537.53";

        Map<String, String> evaled = uaParser.getUAMap(ua1);
        assertEquals("OS name check", (new String("Ubuntu")),
                evaled.get("os_family").toString());
        assertEquals("Browser name check", (new String("Firefox")),
                evaled.get("browser_family").toString());


        evaled = uaParser.getUAMap(ua2);
        assertEquals("OS name check", (new String("iOS")),
                evaled.get("os_family").toString());

        assertEquals("Browser name check", (new String("Mobile Safari UIWebView")),
                evaled.get("browser_family").toString());


        evaled = uaParser.getUAMap(ua3);
        assertEquals("OS name check", (new String("iOS")),
                evaled.get("os_family").toString());
        assertEquals("Browser name check", (new String("Mobile Safari")),
                evaled.get("browser_family").toString());

    }

    /**
     * Tests what we return when browser is empty.
     *
     * UA parser will return this as "browser obj" for an empty user agent string:
     * {
     * user_agent: {family: "Other", major: null, minor: null, patch: null},
     * os: {family: "Other", major: null, minor: null, patch: null, patch_minor: null},
     * device: {family: "Other"}
     * }
     * function returns something like the following:
     * {
     * "device_family":"Other",
     * "browser_major":"-",
     * "os_family":"Other",
     * "os_major":"-",
     * "browser_family":"Other",
     * "os_minor":"-"
     * }
     **/
    @Test
    public void testEmptyUA() {

        Map<String, String> evaled = uaParser.getUAMap("");

        String resultOSName = evaled.get("os_family");
        String resultBrowserName = evaled.get("browser_family");
        String resultOsMinor = evaled.get("os_minor");
        assertEquals("OS name check", (new String("Other")),
                resultOSName.toString());
        assertEquals("Browser name check", (new String("Other")),
                resultBrowserName.toString());

        assertEquals("OS minor", (new String("-")),
                resultOsMinor.toString());
    }

    /**
     * Tests what we return when browser is null
     * Is this right? Maybe we should return just null
     *
     * UA parser will return this as "browser obj" for an empty user agent string:
     * {
     * user_agent: {family: "Other", major: null, minor: null, patch: null},
     * os: {family: "Other", major: null, minor: null, patch: null, patch_minor: null},
     * device: {family: "Other"}
     * }
     * function returns something like the following:
     * {
     * "device_family":"Other",
     * "browser_major":"-",
     * "os_family":"Other",
     * "os_major":"-",
     * "browser_family":"Other",
     * "os_minor":"-"
     * }
     **/
    @Test
    public void testHandlingOfNulls() {

        Map<String, String> evaled = uaParser.getUAMap(null);

        String resultOSName = evaled.get("os_family");
        String resultBrowserName = evaled.get("browser_family");
        String resultOsMinor = evaled.get("os_minor");
        assertEquals("OS name check", (new String("Other")),
                resultOSName.toString());
        assertEquals("Browser name check", (new String("Other")),
                resultBrowserName.toString());

        assertEquals("OS minor", (new String("-")),
                resultOsMinor.toString());

    }

}