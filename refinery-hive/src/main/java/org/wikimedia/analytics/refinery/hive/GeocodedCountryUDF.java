/**
 * Copyright (C) 2014  Wikimedia Foundation
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
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * Deprecated - use GetCountryISOCodesUDF
 * A Hive UDF to lookup country codes from IP addresses.
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION geocode_country as 'org.wikimedia.analytics.refinery.hive.GeocodedCountryUDF';
 *   SELECT geocode_country(ip) from webrequest where year = 2014 limit 10;
 *
 * The above steps assume that the two required files - GeoIP2-Country.mmdb and GeoIP2-City.mmdb - are available
 * in their default path /usr/share/GeoIP. If not, then add the following steps:
 *
 *   SET maxmind.database.country=/path/to/GeoIP2-Country.mmdb;
 *   SET maxmind.database.city=/path/to/GeoIP2-City.mmdb;
 */
@Deprecated
@UDFType(deterministic = true)
@Description(
        name = "geocoded_country",
        value = "_FUNC_(ip) - returns the ISO country code that corresponds to the given IP",
        extended = "")
public class GeocodedCountryUDF extends GetCountryISOCodeUDF {}
