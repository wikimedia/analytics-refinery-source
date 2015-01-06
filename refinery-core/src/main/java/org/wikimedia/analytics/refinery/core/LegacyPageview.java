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

import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Arrays;

/**
 * Static functions to identify what requests constitute "pageviews",
 * according to the definition at
 * https://github.com/wikimedia/analytics-refinery/blob/master/oozie/pagecounts-all-sites/load/insert_hourly_pagecounts.hql
 * This is the "legacy" definition, in use by WebStatsCollector and the
 * pageviews dumps at http://dumps.wikimedia.org/other/pagecounts-ez/
 * from 2007 to early 2015, and is to be superseded by the "Pageview" class
 * and isPageview method.
 */
public class LegacyPageview {

    private static final Pattern acceptedUriHostsPattern = Pattern.compile(
        "\\.(mediawiki|wik(ibooks|idata|imediafoundation|inews|ipedia|iquote|isource|tionary|iversity|ivoyage))\\.org$"
    );

    private static final Pattern acceptedMetaUriHostsPattern = Pattern.compile(
		    "(commons|incubator|meta|outreach|quality|species|strategy|usability)(\\.m)?\\.wikimedia\\.org$"
    );

    private static final Pattern acceptedUriPattern = Pattern.compile(
        "^/wiki/"
    );

    private static final Pattern rejectedUriPattern = Pattern.compile(
        "^/wiki/Special\\:CentralAutoLogin/"
    );
    private static final HashSet<String> rejectedUriPathPages = new HashSet<String>(Arrays.asList(
        "/wiki/undefined",
        "/wiki/Undefined"
    ));

    private static final HashSet<String> rejectedStatusCodes = new HashSet<String>(Arrays.asList(
        "301",
        "302",
        "303"
    ));

    private static final Pattern rejectedIPPattern = Pattern.compile(
        "^(10\\.20\\.0|10\\.64\\.0|10\\.128\\.0|10\\.64\\.32|208\\.80\\.15[2-5]|91\\.198\\.174)\\..+"
    );

    /**
     * Given a webrequest ip, x_forwarded_for, uri_host, uri_path, and http_status, returns
     * True if we consider this a 'legacy pageview', False otherwise.
     *
     * @param   ip             Requesting IP address
     * @param   xForwardedFor  the x_forwarded_for field
     * @param   uriHost        Hostname portion of the URI
     * @param   uriPath        Path portion of the URI
     * @param   uriQuery       Query portion of the URI
     * @param   httpStatus     HTTP request status code
     */
    public static boolean isLegacyPageview(
        String ip,
        String xForwardedFor,
        String uriHost,
        String uriPath,
        String httpStatus
    ) {

        return (
            //The status code is not 301, 302 or 303
            !rejectedStatusCodes.contains(httpStatus)

            //The host is a "recognised" project
            &&  (
                    Pageview.patternIsFound(acceptedUriHostsPattern, uriHost)
                    || Pageview.patternIsFound(acceptedMetaUriHostsPattern, uriHost)
                )
            //The URI path starts with /wiki/, and
            //isn't to undefined, Undefined or Special:CentralAutoLogin
            && Pageview.patternIsFound(acceptedUriPattern, uriPath)
            && !Pageview.patternIsFound(rejectedUriPattern, uriPath)
            && !rejectedUriPathPages.contains(uriPath)

            //The source IP isn't in a specified range (or,
            //is, but the XFF field is not empty)
            &&  (
                    !Pageview.patternIsFound(rejectedIPPattern, ip)
                    || !xForwardedFor.equals("-")
                )
        );
    }
}