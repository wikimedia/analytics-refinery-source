package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.wikimedia.analytics.refinery.core.IpUtil;

/**
 * A Hive UDF to decode hex-encoded IP address.
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_hex_decoded_ip as 'org.wikimedia.analytics.refinery.hive.GetHexDecodedIpUDF';
 *   SELECT get_hex_decoded_ip(ip_hex) from cu_changes where year = 2015 limit 10;
 */
@Description(
        name = "get_hex_decoded_ip",
        value = "_FUNC_(ip_hex) - returns the decoded IP given the hex-encoded IP",
        extended = "")
public class GetHexDecodedIpUDF extends UDF {

    private final IpUtil ipUtil = new IpUtil();

    public String evaluate(String ipHex) {
        return ipUtil.formatHex(ipHex);
    }
}
