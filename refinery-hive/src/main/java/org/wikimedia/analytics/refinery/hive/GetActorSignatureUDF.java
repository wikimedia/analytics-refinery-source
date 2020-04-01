/*
 * Copyright (C) 2020  Wikimedia Foundation
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
import org.wikimedia.analytics.refinery.core.ActorSignatureGenerator;

import java.util.Map;

/**
 * UDF that encapsulates the generation of a hash serving as an 'actor' signature for
 * the purpose of labeling automated traffic.
 * it reproduces the following hive function:
 *   md5(
 *     concat(
 *       ip,
 *       substr(user_agent,0,200),
 *       accept_language,
 *       uri_host,
 *       COALESCE(x_analytics_map['wmfuuid'], parse_url(concat('http://bla.org/woo/', uri_query), 'QUERY', 'appInstallID'),'')
 *     )
 *   )
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_actor_signature as 'org.wikimedia.analytics.refinery.hive.GetActorSignatureUDF';
 *   SELECT get_actor_signature(ip, user_agent, accept_language, uri_host, uri_query, x_analytics_map) from webrequest where year = 2015 limit 10;
 */
@Description(
        name = "get_actor_signature",
        value = "_FUNC_(ip, user_agent, accept_language, uri_host, uri_query, x_analytics_map) - returns the actor signature for the given parameters",
        extended = "")
public class GetActorSignatureUDF extends UDF {

    private final ActorSignatureGenerator actorSignatureGenerator = ActorSignatureGenerator.getInstance();

    public String evaluate(String ip, String userAgent, String acceptLanguage, String uriHost,
                           String uriQuery, Map<String, String> xAnalyticsMap) {
        return actorSignatureGenerator.execute(ip, userAgent, acceptLanguage, uriHost, uriQuery, xAnalyticsMap);
    }
}
