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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.wikimedia.analytics.refinery.core.LegacyPageviewDefinition;


/**
 * A Hive UDF to identify what requests constitute "pageviews",
 * according to the definition at
 * https://github.com/wikimedia/analytics-refinery/blob/master/oozie/pagecounts-all-sites/load/insert_hourly_pagecounts.hql
 * This is the "legacy" definition, in use by WebStatsCollector and the
 * pageviews dumps at http://dumps.wikimedia.org/other/pagecounts-ez/
 * from 2007 to early 2015, and is to be superseded by the "Pageview" class
 * and isPageview method.
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION is_legacy_pageview AS
 *     'org.wikimedia.analytics.refinery.hive.IsLegacyPageviewUDF';
 *   SELECT
 *     LOWER(uri_host) as uri_host,
 *     count(*) as cnt
 *   FROM
 *     wmf_raw.webrequest
 *   WHERE
 *    webrequest_source = 'text'
 *     AND year=2014
 *     AND month=12
 *     AND day=7
 *     AND hour=12
 *     AND is_legacy_pageview(ip, x_forwarded_for, uri_host, uri_path, http_status)
 *   GROUP BY
 *     LOWER(uri_host)
 *   ORDER BY cnt desc
 *   LIMIT 10
 *   ;
 */
public class IsLegacyPageviewUDF extends UDF {
    public boolean evaluate(
        String ip,
        String xForwardedFor,
        String uriHost,
        String uriPath,
        String httpStatus
    ) {
        LegacyPageviewDefinition legacyPageviewDefinitionInstance = LegacyPageviewDefinition.getInstance();
        return legacyPageviewDefinitionInstance.isLegacyPageview(
            ip,
            xForwardedFor,
            uriHost,
            uriPath,
            httpStatus
        );
    }
}