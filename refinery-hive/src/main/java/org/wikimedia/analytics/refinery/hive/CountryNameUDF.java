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
 * Deprecated - Use GetCountryNameUDF
 * A Hive UDF to lookup country name from country code.
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION country_name as 'org.wikimedia.analytics.refinery.hive.CountryNameUDF';
 *   SELECT country_name(country_code) from pageview_hourly where year = 2015 limit 10;
 *
 * NOTE: If this UDF receives a bad country code or null it returns "Unknown" as the country name
 * NOTE: This does not depend on MaxMind
 */
@Deprecated
@UDFType(deterministic = true)
@Description(
        name = "country_name",
        value = "_FUNC_(country_code) - returns the ISO country name that corresponds to the given country code",
        extended = "")
public class CountryNameUDF extends GetCountryNameUDF {}
