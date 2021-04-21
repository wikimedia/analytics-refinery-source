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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.maxmind.ISPDatabaseReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A Hive UDF to lookup ISP fields from IP addresses.
 * <p>
 * Hive/Spark SQL Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION get_isp_data as 'org.wikimedia.analytics.refinery.hive.GetISPDataUDF';
 *   SELECT get_isp_data(ip)['isp'], get_isp_data(ip)['organization'] from webrequest where year = 2014 limit 10;
 *
 * The above steps assume that the required file GeoIP2-ISP.mmdb is available
 * in its default path /usr/share/GeoIP. If not, then add the following step:
 *
 * Hive: SET maxmind.database.isp=/path/to/GeoIP2-ISP.mmdb;
 * Spark: Launch with `--conf "spark.driver.extraJavaOptions=-Dmaxmind.database.isp=/path/to/GeoIP2-ISP.mmdb" \
 *                     --conf "spark.executor.extraJavaOptions=-Dmaxmind.database.isp=/path/to/GeoIP2-ISP.mmdb"
 *
 * Warning: The given file is to be available on all hadoop workers (except in case of local job only)!
 */
@UDFType(deterministic = true)
@Description(name = "get_isp_data", value = "_FUNC_(ip) - "
        + "Returns a map with isp, organization, autonomous_system_organization, autonomous_system_number "
        + "keys and the appropriate values for each of them")
public class GetISPDataUDF extends GenericUDF {

    Map<String, String> result;
    private ObjectInspector argumentOI;
    private ISPDatabaseReader maxMindISP;

    static final Logger LOG = Logger.getLogger(GetISPDataUDF.class.getName());

    private void initializeReader(String configPath, String context) {
        try {
            maxMindISP = new ISPDatabaseReader(configPath);
        } catch (IOException ex) {
            LOG.error("Error initializing maxmind ISP database reader in " + context + " context", ex);
            throw new RuntimeException(ex);
        }
    }

    private void initializeReader(SessionState hiveSessionState) {
        String maxmindConfigPath = "";
        if (hiveSessionState != null) {
            maxmindConfigPath = hiveSessionState.getConf().get(ISPDatabaseReader.DEFAULT_DATABASE_ISP_PROP);
        }
        initializeReader(maxmindConfigPath, "global");
    }

    private void initializeReader(MapredContext context) {
        String maxmindConfigPath = "";
        if (context != null) {
            maxmindConfigPath = context.getJobConf().getTrimmed(ISPDatabaseReader.DEFAULT_DATABASE_ISP_PROP);
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
        assert maxMindISP != null : "Evaluate called without initializing 'geocodeISP'";

        result.clear();

        if (arguments.length == 1 && argumentOI != null && arguments[0] != null) {
            String ip = ((StringObjectInspector) argumentOI).getPrimitiveJavaObject(arguments[0].get());
            Map<String, String> ispDataResult = maxMindISP.getResponse(ip).getMap();
            if (ispDataResult != null) {
                for (String field : ispDataResult.keySet()) {
                    Object value = ispDataResult.get(field);
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
        return "get_isp_data(" + arguments[0] + ")";
    }
}
