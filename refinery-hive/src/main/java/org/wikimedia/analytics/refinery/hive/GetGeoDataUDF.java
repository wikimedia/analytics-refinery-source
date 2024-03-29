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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.maxmind.GeocodeDatabaseReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A Hive UDF to lookup location fields from IP addresses.
 * <p>
 * Hive/Spark SQL Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_geo_data as 'org.wikimedia.analytics.refinery.hive.GetGeoDataUDF';
 *   SELECT get_geo_data(ip)['country'], get_geo_data(ip)['city'] from webrequest where year = 2014 limit 10;
 *
 * The above steps assume that the required file GeoIP2-City.mmdb is available
 * in its default path /usr/share/GeoIP. If not, then add the following step:
 *
 * Hive: SET maxmind.database.city=/path/to/GeoIP2-City.mmdb;
 * Spark: Launch with `--conf "spark.driver.extraJavaOptions=-Dmaxmind.database.city=/path/to/GeoIP2-City.mmdb" \
 *                     --conf "spark.executor.extraJavaOptions=-Dmaxmind.database.city=/path/to/GeoIP2-City.mmdb"
 *
 * Warning: The given file is to be available on all hadoop workers (except in case of local job only)!
 */
@UDFType(deterministic = true)
@Description(name = "get_geo_data", value = "_FUNC_(ip) - "
        + "Returns a map with continent, country_code, country, city, subdivision, postal_code, latitude, longitude, "
        + "timezone keys and the appropriate values for each of them")
public class GetGeoDataUDF extends GenericUDF {
    private Converter[] converters = new Converter[1];
    private PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];

    Map<String, String> result;
    protected GeocodeDatabaseReader maxMindGeocode;

    static final Logger LOG = Logger.getLogger(GetGeoDataUDF.class.getName());

    private void initializeReader(String configPath, String context) {
        try {
            maxMindGeocode = new GeocodeDatabaseReader(configPath);
        } catch (IOException ex) {
            LOG.error("Error initializing maxmind geocode database reader in " + context + " context", ex);
            throw new RuntimeException(ex);
        }
    }

    private void initializeReader(SessionState hiveSessionState) {
        String maxmindConfigPath = "";
        if (hiveSessionState != null) {
            maxmindConfigPath = hiveSessionState.getConf().get(GeocodeDatabaseReader.DEFAULT_DATABASE_CITY_PROP);
        }
        initializeReader(maxmindConfigPath, "global");
    }

    private void initializeReader(MapredContext context) {
        String maxmindConfigPath = "";
        if (context != null) {
            maxmindConfigPath = context.getJobConf().getTrimmed(GeocodeDatabaseReader.DEFAULT_DATABASE_CITY_PROP);
        }
        initializeReader(maxmindConfigPath, "mapreduce");
    }

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

        initializeReader(SessionState.get());

        checkArgsSize(arguments, 1, 1);
        checkArgPrimitive(arguments, 0);
        checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);
        obtainStringConverter(arguments, 0, inputTypes, converters);

        result = new HashMap<>();

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    /**
     * This function initializes the maxmind reader in a mapreduce context and is
     * necessary to correctly parameterize the maxmind database set in hive (if any).
     *
     * @param context the mapreduce context to extract the config property from
     */
    @Override
    public void configure(MapredContext context) {
        initializeReader(context);
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
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert maxMindGeocode != null : "Evaluate called without initializing 'geocodeCity'";

        result.clear();

        String ip = getStringValue(arguments, 0, converters);
        Map<String, String> geoDataResult = maxMindGeocode.getResponse(ip).getMap();

        if (geoDataResult != null) {
            for (String field : geoDataResult.keySet()) {
                Object value = geoDataResult.get(field);
                if (value != null) {
                    result.put(field, value.toString());
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
