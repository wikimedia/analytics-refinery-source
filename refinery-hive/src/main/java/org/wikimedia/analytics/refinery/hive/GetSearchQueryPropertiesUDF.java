/**
 * Copyright (C) 2015 Wikimedia Foundation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.wikimedia.analytics.refinery.core.SearchQuery;

/**
 * A Hive UDF to determine the features of a Cirrus search query.
 * <p>
 * For each query, the UDF produces an array of features such as
 * [has AND], [is insource, has even double quotes], [is simple],
 * and [has wildcard, ends with ?]. The UDF accepts a string as
 * the parameter and returns string "[feature1, ..., featureN]".
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_search_query_properties as 'org.wikimedia.analytics.refinery.hive.DeconstructQueryUDF';
 *   SELECT get_search_query_properties(query_string) AS features;
 * <p>
 * To make the above work with wmf_raw.CirrusSearchRequestSet's
 * data structure, operate on:
 *   requests.query[SIZE(requests.query)-1] AS query_string;
 */
@Description(
        name = "get_search_query_properties",
        value = "_FUNC_(query_string) - returns an array of features",
        extended = "")
public class GetSearchQueryPropertiesUDF extends UDF {
    public String evaluate(String query) {
        SearchQuery query_inst = SearchQuery.getInstance();
        return(query_inst.deconstructSearchQuery(query));
    }
}
