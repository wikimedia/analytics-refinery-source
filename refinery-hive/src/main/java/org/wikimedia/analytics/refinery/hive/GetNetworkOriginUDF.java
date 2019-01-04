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
 * This broadly partitions all of the IPv4+IPv6 address space into "wikimedia"
 * (originating from an IP owned/leased by the Wikimedia Foundation),
 * "wikimedia_labs" (subset of "wikimedia" that has been allocated to the
 * Wikimedia Labs project), and "internet" (originating from an IP not
 * owned/leased by the Wikimedia Foundation).
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_network_origin as 'org.wikimedia.analytics.refinery.hive.NetworkOriginUDF';
 *   SELECT get_network_origin(ip) from webrequest where year = 2015 limit 10;
 */
@Description(
        name = "get_network_origin",
        value = "_FUNC_(ip) - returns the network origin "
            + "(wikimedia, wikimedia_labs, internet) for the given IP address",
        extended = "")
public class GetNetworkOriginUDF extends UDF {

    private IpUtil ipUtil = new IpUtil();

    public String evaluate(String ip) {
        assert ipUtil != null : "Evaluate called without initializing 'ipUtil'";

        return ipUtil.getNetworkOrigin(ip).toString();
    }
}
