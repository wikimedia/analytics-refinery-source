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

import java.io.IOException;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.Geocode;

/**
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
@UDFType(deterministic = true)
@Description(
        name = "geocoded_country",
        value = "_FUNC_(ip) - returns the ISO country code that corresponds to the given IP",
        extended = "")
public class GeocodedCountryUDF extends GenericUDF {

    private final Text result = new Text();
    private ObjectInspector argumentOI;
    private Geocode geocode;

    static final Logger LOG = Logger.getLogger(GeocodedCountryUDF.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The GeocodedCountryUDF takes an array with only 1 element as argument");
        }

        ObjectInspector arg1 = arguments[0];

        if (arg1.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "A string argument was expected but an argument of type " + arg1.getTypeName()
                            + " was given.");
        }

        PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) arg1).getPrimitiveCategory();

        if (primitiveCategory != PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0,
                    "A string argument was expected but an argument of type " + arg1.getTypeName()
                            + " was given.");
        }

        //Cache the argument to be used in evaluate
        argumentOI = arg1;

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public void configure(MapredContext context) {
        if (geocode == null) {
            try {
                JobConf jobConf = context.getJobConf();
                geocode = new Geocode(
                    jobConf.getTrimmed("maxmind.database.country"),
                    jobConf.getTrimmed("maxmind.database.city")
                );
            } catch (IOException ex) {
                LOG.error(ex);
                throw new RuntimeException(ex);
            }
        }

        super.configure(context);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert geocode != null : "Evaluate called without initializing 'geocode'";

        result.clear();

        if (arguments.length == 1 && argumentOI != null && arguments[0] != null) {
            String ip = ((StringObjectInspector) argumentOI).getPrimitiveJavaObject(arguments[0].get());
            result.set(geocode.getCountryCode(ip));
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] arguments) {
        assert (arguments.length == 1);
        return "geocoded_country(" + arguments[0] + ")";
    }
}
