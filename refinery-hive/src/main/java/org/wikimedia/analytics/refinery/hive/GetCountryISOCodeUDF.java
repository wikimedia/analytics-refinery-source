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

import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.maxmind.CountryDatabaseReader;

import java.io.IOException;

/**
 * A Hive UDF to lookup country codes from IP addresses.
 * <p>
 * Hive/Spark SQL Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_country_iso as 'org.wikimedia.analytics.refinery.hive.GetCountryISOCodeUDF';
 *   SELECT get_country_iso(ip) from webrequest where year = 2014 limit 10;
 *
 * The above steps assume that the required file GeoIP2-Country.mmdb is available
 * in its default path /usr/share/GeoIP. If not, then add the following step:
 *
 * Hive: SET maxmind.database.country=/path/to/GeoIP2-Country.mmdb;
 * Spark: Launch with `--conf "spark.driver.extraJavaOptions=-Dmaxmind.database.country=/path/to/GeoIP2-Country.mmdb" \
 *                     --conf "spark.executor.extraJavaOptions=-Dmaxmind.database.country=/path/to/GeoIP2-Country.mmdb"
 *
 * Warning: The given file is to be available on all hadoop workers (except in case of local job only)!
 */
@UDFType(deterministic = true)
@Description(
        name = "get_country_iso",
        value = "_FUNC_(ip) - returns the ISO country code that corresponds to the given IP",
        extended = "")
public class GetCountryISOCodeUDF extends GenericUDF {

    private final Text result = new Text();
    private ObjectInspector argumentOI;
    private CountryDatabaseReader maxMindCountryCode;

    static final Logger LOG = Logger.getLogger(GetCountryISOCodeUDF.class.getName());

    private void initializeReader(String configPath, String context) {
        if (maxMindCountryCode == null) {
            try {
                maxMindCountryCode = new CountryDatabaseReader(configPath);
            } catch (IOException ex) {
                LOG.error("Error initializing maxmind country database reader in " + context + " context", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    private void initializeReader(SessionState hiveSessionState) {
        String maxmindConfigPath = "";
        if (hiveSessionState != null) {
            maxmindConfigPath = hiveSessionState.getConf().get(CountryDatabaseReader.DEFAULT_DATABASE_COUNTRY_PROP);
        }
        initializeReader(maxmindConfigPath, "global");
    }

    private void initializeReader(MapredContext context) {
        String maxmindConfigPath = "";
        if (context != null) {
            maxmindConfigPath = context.getJobConf().getTrimmed(CountryDatabaseReader.DEFAULT_DATABASE_COUNTRY_PROP);
        }
        initializeReader(maxmindConfigPath, "mapreduce");
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        initializeReader(SessionState.get());

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The GetCountryISOCodeUDF takes an array with only 1 element as argument");
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

    @SuppressWarnings("unchecked")
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert maxMindCountryCode != null : "Evaluate called without initializing 'maxMindCountryCode'";

        result.clear();

        if (arguments.length == 1 && argumentOI != null && arguments[0] != null) {
            String ip = ((StringObjectInspector) argumentOI).getPrimitiveJavaObject(arguments[0].get());
            result.set(maxMindCountryCode.getResponse(ip).getCountryCode());
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] arguments) {
        assert (arguments.length == 1);
        return "get_country_iso(" + arguments[0] + ")";
    }
}
