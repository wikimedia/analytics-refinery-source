package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * Deprecated - Use GetUAPropertiesUDF
 * UDF that parses user agent strings using UA parser and returns a hashmap in
 * this form:
 * {
 * "device_family":"Other",
 * "browser_major":"11",
 * "os_family":"Windows",
 * "os_major":"-",
 * "browser_family":"IE",
 * "os_minor":"-"
 * }
 * <p/>
 * Records are processed one by one.
 */

@Deprecated
@UDFType(deterministic = true)
@Description(name = "ua_parser", value = "_FUNC_(UA) - "
        + "Returns a map with browser_name, browser_major, device, os_name, os_minor, os_major keys and "
        + "the appropriate values for each of them")
public class UAParserUDF extends GetUAPropertiesUDF {}
