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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.wikimedia.analytics.refinery.core.PageviewDefinition;


/**
 * A Hive UDF to identify a Wikimedia webrequest pageview project.
 * NOTE: this udf only works well if the uri_host comes from
 * a webrequest having is_pageview = true
 *
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_project AS
 *     'org.wikimedia.analytics.refinery.hive.IdentifyProjectUDF';
 *   SELECT
 *     get_project(uri_host) as project_qualifier,
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
 *     get_project(uri_host)
 *   ORDER BY cnt desc
 *   LIMIT 10
 *   ;
 */
@Description(name = "get_project",
        value = "_FUNC_(uri_host) - Returns the project identifier for the pageview request.",
        extended = "")
public class GetProjectUDF extends UDF {
    public String evaluate(String uriHost) {
        PageviewDefinition pageviewDefinitionInstance = PageviewDefinition.getInstance();
        return pageviewDefinitionInstance.getProjectFromHost(uriHost);
    }
}