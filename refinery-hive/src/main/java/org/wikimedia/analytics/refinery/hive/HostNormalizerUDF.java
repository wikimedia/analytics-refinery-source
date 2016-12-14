package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * Deprecated - Use GetHostPropertiesUDF
 * UDF that normalizes host (lower case, split) and returns a struct.
 * Records are processed one by one.<p/>
 * Example:<br/>
 * SELECT normalize_host('en.m.zero.wikipedia.org') FROM test_table LIMIT 1;<br/>
 * Returns:<br/>
 * {
 * "project_class":"wikipedia",
 * "project":"en",
 * "qualifiers":["m", "zero"],
 * "tld":"org",
 * }
 * <p/>
 * Struct fields can be access using dotted notation:<br/>
 * SELECT normalize_host('en.m.zero.wikipedia.org').project FROM test_table LIMIT 1;<br/>
 * Returns: en<br/>
 */

@Deprecated
@UDFType(deterministic = true)
@Description(name = "normalize_host", value = "_FUNC_(uri_host) - "
        + "Returns a map with project_class, project, qualifiers, tld keys and "
        + "the appropriate values for each of them")
public class HostNormalizerUDF extends GetHostPropertiesUDF {}