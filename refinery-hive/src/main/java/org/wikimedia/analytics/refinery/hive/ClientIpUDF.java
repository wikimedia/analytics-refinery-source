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

import org.wikimedia.analytics.refinery.core.IpUtil;

/**
 * A Hive UDF to determine client IP given the source IP and X-Forwarded-For header.
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION client_ip as 'org.wikimedia.analytics.refinery.hive.ClientIpUDF';
 *   SELECT client_ip(ip, x_forwarded_for) from webrequest where year = 2015 limit 10;
 */
@Description(
        name = "client_ip",
        value = "_FUNC_(ip, xff) - returns the client IP given the source IP and X-Forwarded-For " +
                "header",
        extended = "")
public class ClientIpUDF extends UDF {

    private IpUtil ipUtil = new IpUtil();

    public String evaluate(String ip, String xff) {
        assert ipUtil != null : "Evaluate called without initializing 'ipUtil'";

        return ipUtil.getClientIp(ip, xff);
    }
}