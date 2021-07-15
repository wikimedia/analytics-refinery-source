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

    // On 2019-06-05 we reduced this limit from 1024 to 400
    // to prevent this code to parse malicious user agent strings
    // that clog the regular expressions in the ua parser.
    public static final int MAX_UA_LENGTH = 400;

    public static final String NA = "-";

    private static final Logger LOG = Logger.getLogger(UAParser.class.getName());

    private static CachingParser cachingParser;

    /*
     * Meta-methods to enable eager instantiation in a singleton-based way.
     * in non-Java terms: you get to only create one class instance, and only
     * when you need it, instead of always having everything (static/eager instantiation)
     * or always generating everything anew (!singletons). So we have:
     * (1) an instance;
     * (2) an empty constructor (to avoid people just calling the constructor);
     * (3) an actual getInstance method to allow for instantiation.
     */
    private static final UAParser instance = new UAParser();

    public static UAParser getInstance(){
        return instance;
    }

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
        // CachingParser default cache size is 1000.
        // Expanding it to 10000 divides computation
        // by ~2.5, and expanding it more has less impact
        if (cachingParser == null) {
            cachingParser = new CachingParser(10000);
        }
    }

    /**
     * Function extracting browser, device and os information from the UA string.
     * In case the uaString is null, make it an empty String.
     * In case the uaString is longer than 512 characters, don't even try to parse,
     * return empty map.
     * @param uaString the ua string to parse
     * @return the ua map with browser_family, browser_major, device_family,
     * os_family, os_major, os_minor, wmf_app_version keys and associated values.
     */
    public Map<String, String> getUAMap(String uaString) {
        // Presetting map size to correct number of slots
        Map<String, String> result = new HashMap<>(8);

        UserAgent browser = null;
        Device device = null;
        OS os = null;

        if (uaString == null)
            uaString = "";


        if (uaString.length() <= MAX_UA_LENGTH) {
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

        // Default wmf_app_version is NA
        String wmfAppVersion = NA;

        String wmfAppStart = "WikipediaApp/";
        if (uaString.startsWith(wmfAppStart)) {
            int from = wmfAppStart.length();
            // Take the substring until either space or end of string.
            int to = uaString.indexOf(' ', from);
            to = (to == -1) ? uaString.length() : to;
            wmfAppVersion = uaString.substring(from, to);
        }
        result.put("wmf_app_version", wmfAppVersion);

        return result;
    }

}
