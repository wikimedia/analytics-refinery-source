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
import org.wikimedia.analytics.refinery.core.PageviewDefinition;


/**
 * A Hive UDF to classify a Wikimedia webrequest as an app pageview.
 * See: https://meta.wikimedia.org/wiki/Research:Page_view/Generalised_filters
 *      and isPageviewUDF for information on how to classify a pageview generally.
 *
 * This is not a /complete/ definition - it was initially a private method. As a
 * result it does not do, for example, HTTP status filtering. See the example
 * query below for how to solve for that.
 *
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION is_app_pageview AS
 *     'org.wikimedia.analytics.refinery.hive.IsAppPageviewUDF';
 *   SELECT
 *     LOWER(uri_host) as uri_host,
 *     count(*) as cnt
 *   FROM
 *     wmf_raw.webrequest
 *   WHERE year=2014
 *     AND month=12
 *     AND day=7
 *     AND hour=12
 *     AND http_status IN ('200','304')
 *     AND is_app_pageview(uri_path, uri_query, http_status, content_type, user_agent)
 *   GROUP BY
 *     LOWER(uri_host)
 *   ORDER BY cnt desc
 *   LIMIT 10
 *   ;
 */
public class IsAppPageviewUDF extends UDF {
    public boolean evaluate(
        String uriPath,
        String uriQuery,
        String contentType,
        String userAgent
    ) {
        PageviewDefinition pageviewDefinitionInstance = PageviewDefinition.getInstance();
        return pageviewDefinitionInstance.isAppPageview(
            uriPath,
            uriQuery,
            contentType,
            userAgent
        );
    }
}
