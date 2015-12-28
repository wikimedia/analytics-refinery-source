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

import org.wikimedia.analytics.refinery.core.SearchEngineClassifier;

/**
 * A Hive UDF to classify whether the web request came internally,
 * externally, from a search engine, or without a referer.
 */
@UDFType(deterministic = true)
@Description(name = "referer_classify",
        value = "_FUNC_(referer) - Returns a string with a classification of a referer - e.g. none/unknown/internal/external/external (search engine)",
        extended = "argument 0 is the referer url to analyze")
public class SmartReferrerClassifierUDF extends UDF {
    public String evaluate(String referer) {
        return SearchEngineClassifier.getInstance().refererClassify(referer);
    }
}
