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
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.mapred.JobConf;

import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.Geocode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A Hive UDF to lookup location fields from IP addresses.
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_geo_data as 'org.wikimedia.analytics.refinery.hive.GetGeoDataUDF';
 *   SELECT get_geo_data(ip)['country'], get_geo_data(ip)['city'] from webrequest where year = 2014 limit 10;
 *
 * The above steps assume that the two required files - GeoIP2-Country.mmdb and GeoIP2-City.mmdb - are available
 * in their default path /usr/share/GeoIP. If not, then add the following steps:
 *
 *   SET maxmind.database.country=/path/to/GeoIP2-Country.mmdb;
 *   SET maxmind.database.city=/path/to/GeoIP2-City.mmdb;
 */
@UDFType(deterministic = true)
@Description(name = "get_geo_data", value = "_FUNC_(ip) - "
        + "Returns a map with continent, country_code, country, city, subdivision, postal_code, latitude, longitude, "
        + "timezone keys and the appropriate values for each of them")
public class GetGeoDataUDF extends GenericUDF {

    Map<String, String> result;
    private ObjectInspector argumentOI;
    private Geocode geocode;

    static final Logger LOG = Logger.getLogger(GetGeoDataUDF.class.getName());

    /**
     * The initialize method is called only once during the lifetime of the UDF.
     * <p/>
     * Method checks for the validity (number, type, etc)
     * of the arguments being passed to the UDF.
     * It also sets the return type of the result of the UDF,
     * in this case the ObjectInspector equivalent of
     * Map<String,String>
     *
     * @param arguments
     * @return ObjectInspector Map<String,String>
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The GetGeoDataUDF takes an array with only 1 element as argument");
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

        argumentOI = arg1;

        result = new HashMap<String, String>();

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
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

    /**
     * Takes the actual arguments and returns the result.
     * Gets passed the input, does whatever it wants to it,
     * and then returns the output.
     * <p/>
     * The input is accessed using the ObjectInspectors that
     * were saved into global variables in the call to initialize()
     * <p/>
     * This method is called once for every row of data being processed.
     * UDFs are called during the map phase of the MapReduce job.
     * This means that we have no control over the order in which the
     * records get sent to the UDF.
     *
     * @param arguments
     * @return Object Map<String, String>
     * @throws HiveException
     */
    @SuppressWarnings("unchecked")
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert geocode != null : "Evaluate called without initializing 'geocode'";

        result.clear();

        if (arguments.length == 1 && argumentOI != null && arguments[0] != null) {
            String ip = ((StringObjectInspector) argumentOI).getPrimitiveJavaObject(arguments[0].get());
            Map<String, Object> geoDataResult = geocode.getGeocodedData(ip);
            if (geoDataResult != null) {
                for (String field : geoDataResult.keySet()) {
                    Object value = geoDataResult.get(field);
                    if (value != null) {
                        result.put(field, value.toString());
                    }
                }
            }
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] arguments) {
        assert (arguments.length == 1);
        return "get_geo_data(" + arguments[0] + ")";
    }
}
