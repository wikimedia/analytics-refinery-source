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

import org.wikimedia.analytics.refinery.core.referer.RefererClassifier;

/**
 * A Hive UDF to identify requests that come via external search engines.
 */
@UDFType(deterministic = true)
@Description(name = "get_referer_search_engine",
        value = "_FUNC_(referer) - Returns a string with a classification of a referer (none/unknown/google/etc.)",
        extended = "argument 0 is the referer url to analyze")
public class GetRefererSearchEngineUDF extends UDF {
    public String evaluate(String referer) {
        return RefererClassifier.getInstance().nameSearchEngine(referer);
    }
}
