/*
 * Copyright (C) 2026  Wikimedia Foundation
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
import org.wikimedia.analytics.refinery.core.Sdbm;

/**
 * A Hive UDF that computes the SDBM hash of a string, returned as a BIGINT.
 * <p>
 * Uses a reimplementation of
 * <a href="https://github.com/haproxy/haproxy/blob/c4deec58198b2327187f115deb0ea52543d602a4/src/hash.c#L78">
 *     haproxy's version
 * </a>.
 * A NULL input returns NULL.
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION sdbm_hash as 'org.wikimedia.analytics.refinery.hive.SdbmHashUDF';
 *   SELECT sdbm_hash(uri_host) from webrequest where year = 2015 limit 10;
 */
@Description(
        name = "sdbm_hash",
        value = "_FUNC_(string) - returns the unsigned 32-bit SDBM hash of the string as a BIGINT",
        extended = "Example:\n"
                + "  > SELECT _FUNC_('Wikipedia');\n"
                + "  2154307799")
@UDFType(deterministic = true)
public class SdbmHashUDF extends UDF {

    public Long evaluate(String value) {
        if (value == null) {
            return null;
        }
        return Sdbm.hash(value);
    }
}
