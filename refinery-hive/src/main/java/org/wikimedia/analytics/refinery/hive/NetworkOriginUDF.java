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
 * A Hive UDF to determine network origin for an IP address.
 * <p>
 * This broadly partitions all of the Internet into "internal" (originating
 * from an IP owned/leased by the Wikimedia Foundation), "external"
 * (originating from an IP not owned/leased by the Wikimedia Foundation), and
 * "labs" (subset of "internal" that has been allocated to the Wikimedia Labs
 * project).
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION network_origin as 'org.wikimedia.analytics.refinery.hive.NetworkOriginUDF';
 *   SELECT network_origin(ip) from webrequest where year = 2015 limit 10;
 */
@Description(
        name = "network_origin",
        value = "_FUNC_(ip) - returns the network origin "
            + "(internal, external, labs) for the given IP address",
        extended = "")
public class NetworkOriginUDF extends UDF {

    private IpUtil ipUtil = new IpUtil();

    public String evaluate(String ip) {
        assert ipUtil != null : "Evaluate called without initializing 'ipUtil'";

        return ipUtil.getNeworkOrigin(ip).toString();
    }
}
