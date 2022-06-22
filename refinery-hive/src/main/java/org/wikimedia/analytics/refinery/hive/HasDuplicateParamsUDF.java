/**
 * Copyright (C) 2022  Wikimedia Foundation
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

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * A hive UDF that checks whether a uri_query contains duplicate parameters.
 */
@UDFType(deterministic = true)
@Description(name = "has_duplicate_params",
        value = "_FUNC_(uri_query, only_conflicting) - Returns a boolean indicating whether the query contains duplicate parameters.",
        extended = "argument 0 is the uri_query to analyze; argument 1 controls whether duplicate parameters with identical values are ignored")
public class HasDuplicateParamsUDF extends UDF {
    public boolean evaluate(
        String uriQuery,
        boolean onlyConflicting
    ) {
        if (!uriQuery.startsWith("?")) {
            return false;
        }
        uriQuery = uriQuery.substring(1);
        HashMap<String, String> kvs = new HashMap<>();
        for (String kv : uriQuery.split("&")) {
            String[] pair = kv.split("=", 2);
            String key = pair[0];
            String val = pair.length > 1 ? pair[1] : "";
            String old = kvs.putIfAbsent(key, val);
            if (old != null && (!onlyConflicting || !val.equals(old))) {
                return true;
            }
        }
        return false;
    }
}
