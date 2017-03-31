/**
 * Copyright (C) 2015  Wikimedia Foundation
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
import org.apache.hadoop.hive.ql.udf.UDFType;

import org.wikimedia.analytics.refinery.core.SearchRequest;

/**
 * A hive UDF to identify what type of search API the request uses,
 * e.g. cirrus (aka full-text), prefix, or geo/open/language search.
 */
@UDFType(deterministic = true)
@Description(name = "get_search_request_type",
        value = "_FUNC_(uriPath, uriQuery) - Returns a string with a classification of search API request (cirrus/prefix/geo/etc.)",
        extended = "arguments 0 and 1 are the path and query portions of the URI to analyze, respectively")
public class GetSearchRequestTypeUDF extends UDF {
    public String evaluate(String uriPath, String uriQuery) {
        return SearchRequest.getInstance().classifySearchRequest(uriPath, uriQuery);
    }
}
