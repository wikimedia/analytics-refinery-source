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
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.Geocode;

/**
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
@UDFType(deterministic = true)
@Description(
        name = "country_name",
        value = "_FUNC_(country_code) - returns the ISO country name that corresponds to the given country code",
        extended = "")
public class CountryNameUDF extends GenericUDF {

    private final Text result = new Text();
    private StringObjectInspector argumentOI;

    static final Logger LOG = Logger.getLogger(CountryNameUDF.class.getName());

    /**
     * Checks arguments size is 1
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        GenericUDFHelper argsHelper = new GenericUDFHelper();
        argsHelper.checkArgsSize(arguments, 1, 1);
        argsHelper.checkArgPrimitive(arguments, 0);
        argsHelper.checkArgType(arguments, 0, PrimitiveCategory.STRING);

        //Cache the argument to be used in evaluate
        argumentOI = (StringObjectInspector) arguments[0];

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        result.clear();
        String countryCode = argumentOI.getPrimitiveJavaObject(arguments[0].get());
        result.set(Geocode.getCountryName(countryCode));
        return result;
    }

    @Override
    public String getDisplayString(String[] arguments) {
        assert (arguments.length == 1);
        return "country_name(" + arguments[0] + ")";
    }
}
