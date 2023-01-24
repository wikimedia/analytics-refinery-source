/**
    * Copyright (C) 2014  Wikimedia Foundation
    * <p/>
    * Licensed under the Apache License, Version 2.0 (the "License");
    * you may not use this file except in compliance with the License.
    * You may obtain a copy of the License at
    * <p/>
    * http://www.apache.org/licenses/LICENSE-2.0
    * <p/>
    * Unless required by applicable law or agreed to in writing, software
    * distributed under the License is distributed on an "AS IS" BASIS,
    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    * See the License for the specific language governing permissions and
    * limitations under the License.
    */

package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;


/**
 * A Hive UDF to classify a Wikimedia webrequest as a 'redirect_to_pageview'.
 * this means that content_type might be "-" and redirect code is one of 301,302 ,307
 * the rest of pageview filters apply
 *
 * <p/>
 * <p/>
 * Hive Usage:
 * ADD JAR /path/to/refinery-hive.jar;
 * CREATE TEMPORARY FUNCTION is_pageview AS
 * 'org.wikimedia.analytics.refinery.hive.IsPageviewUDF';
 * SELECT
 * LOWER(uri_host) as uri_host,
 * count(*) as cnt
 * FROM
 * wmf_raw.webrequest
 * WHERE
 * webrequest_source = 'text'
 * AND year=2014
 * AND month=12
 * AND day=7
 * AND hour=12
 * AND is_redirect_to_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent, [x_analytics_header])
 * GROUP BY
 * LOWER(uri_host)
 * ORDER BY cnt desc
 * LIMIT 10
 * ;
 */
@Description(name = "is_redirect_to_pageview",
    value = "_FUNC_(uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics) " +
        "- Returns true if the request is a redirect to a pageview",
    extended = "")
@UDFType(deterministic = true)
public class IsRedirectToPageviewUDF extends IsPageviewUDF {

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException{
        return pageviewDefinition.isRedirectToPageview(buildWebrequestData(arguments));
    }


    @Override
    public String getDisplayString(String[] arguments) {
        return "is_redirect_to_pageview(" + arguments.toString() + ")";
    }

}
