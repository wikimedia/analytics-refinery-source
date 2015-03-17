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
 * A Hive UDF to classify a Wikimedia webrequest as a 'pageview'.
 * See: https://meta.wikimedia.org/wiki/Research:Page_view/Generalised_filters
 *      for information on how to classify a pageview.
 *
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION is_pageview AS
 *     'org.wikimedia.analytics.refinery.hive.IsPageviewUDF';
 *   SELECT
 *     LOWER(uri_host) as uri_host,
 *     count(*) as cnt
 *   FROM
 *     wmf_raw.webrequest
 *   WHERE
 *    webrequest_source = 'mobile'
 *     AND year=2014
 *     AND month=12
 *     AND day=7
 *     AND hour=12
 *     AND is_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent)
 *   GROUP BY
 *     LOWER(uri_host)
 *   ORDER BY cnt desc
 *   LIMIT 10
 *   ;
 */
public class IsPageviewUDF extends UDF {
    public boolean evaluate(
        String uriHost,
        String uriPath,
        String uriQuery,
        String httpStatus,
        String contentType,
        String userAgent
    ) {
        PageviewDefinition pageviewDefinitionInstance = PageviewDefinition.getInstance();
        return pageviewDefinitionInstance.isPageview(
            uriHost,
            uriPath,
            uriQuery,
            httpStatus,
            contentType,
            userAgent
        );
    }
}