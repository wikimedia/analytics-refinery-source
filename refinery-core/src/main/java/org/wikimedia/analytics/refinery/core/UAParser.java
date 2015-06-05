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


import org.apache.log4j.Logger;
import ua_parser.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains functions to parse user agent string using ua-parser library
 */
public class UAParser {

    public static final String NA = "-";

    static final Logger LOG = Logger.getLogger(UAParser.class.getName());

    private CachingParser cachingParser;
    private Map<String, String> result = new HashMap<String, String>();

    /**
     * Function replacing null/empty string with the NA one.
     * @param str the string to check
     * @return the original string if not null/empty, NA otherwise
     */
    private String replaceNA(String str) {
        final String ret;
        if (str == null || str.isEmpty() || str.equals("-")) {
            ret = NA;
        } else {
            ret = str;
        }
        return ret;
    }

    public UAParser() {
        try {
            cachingParser = new CachingParser();
        } catch (IOException e) {
            // no recovery should be possible, log and rethrow
            // runtime exception will be logged to stdout by default
            LOG.error(e.getMessage(), e);
            throw new RuntimeException("Failed to instantiate CachingParser");
        }
    }

    /**
     * Function extracting browser, device and os information from the UA string.
     * In case the uaString is null, make it an empty String.
     * @param uaString the ua string to parse
     * @return the ua map with browser_family, browser_major, device_family,
     * os_family, os_major, os_minor, wmf_app_version keys and associated values.
     */
    public Map<String, String> getUAMap(String uaString) {
        result.clear();

        UserAgent browser = null;
        Device device = null;
        OS os = null;

        if (uaString == null)
            uaString = "";

        try {
            Client c = cachingParser.parse(uaString);
            if (c != null) {
                browser = c.userAgent;
                device = c.device;
                os = c.os;
            }
        } catch (Exception e) {
            // catch it all to make sure job does not halt if one record is faulty
            LOG.error(e.getMessage(), e);
        }

        if (browser != null) {
            result.put("browser_family", replaceNA(browser.family));
            result.put("browser_major", replaceNA(browser.major));
        } else {
            result.put("browser_family", NA);
            result.put("browser_major", NA);
        }

        if (device != null) {
            result.put("device_family", replaceNA(device.family));
        } else {
            result.put("device_family", NA);
        }

        if (os != null) {
            result.put("os_family", replaceNA(os.family));
            result.put("os_major", replaceNA(os.major));
            result.put("os_minor", replaceNA(os.minor));
        } else {
            result.put("os_family", NA);
            result.put("os_major", NA);
            result.put("os_minor", NA);
        }

        String wmfAppStart = "WikipediaApp/";
        if (uaString.startsWith(wmfAppStart)) {
            int from = wmfAppStart.length();
            int to = uaString.indexOf(' ', from);
            result.put("wmf_app_version", uaString.substring(from, to));
        } else {
            result.put("wmf_app_version", NA);
        }

        return result;
    }

}
