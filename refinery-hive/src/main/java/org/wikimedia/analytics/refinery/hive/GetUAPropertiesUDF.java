package org.wikimedia.analytics.refinery.hive;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.core.UAParser;

import java.util.HashMap;
import java.util.Map;

/**
 * UDF that parses user agent strings using UA parser and returns a hashmap in
 * this form:
 * {
 * "device_family":"Other",
 * "browser_major":"11",
 * "os_family":"Windows",
 * "os_major":"-",
 * "browser_family":"IE",
 * "os_minor":"-"
 * }
 * <p/>
 * Records are processed one by one.
 */

@UDFType(deterministic = true)
@Description(name = "get_ua_properties", value = "_FUNC_(UA) - "
        + "Returns a map with browser_name, browser_major, device, os_name, os_minor, os_major keys and "
        + "the appropriate values for each of them")
public class GetUAPropertiesUDF extends GenericUDF {
    private Converter[] converters = new Converter[1];
    private PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];

    private UAParser uaParser;

    // TODO figure out why not everything is logged to hive.log and some logging
    // TODO through log4j stays on the hadoop logs
    static final Logger Log = Logger.getLogger(GetUAPropertiesUDF.class.getName());

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

        checkArgsSize(arguments, 1, 1);
        checkArgPrimitive(arguments, 0);
        checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);
        obtainStringConverter(arguments, 0, inputTypes, converters);

        // Instantiate the UAParser
        uaParser = new UAParser();

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
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
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert uaParser != null: "Evaluate called without initializing 'uaParser'";

        String ua = getStringValue(arguments, 0, converters);
        return uaParser.getUAMap(ua);
    }

    /**
     * Returns string representation of the function.
     * Will be printed when an error happens.
     * *
     */
    @Override
    public String getDisplayString(String[] arguments) {
        assert (arguments.length == 1);
        return "get_ua_properties(" + arguments[0] + ")";

    }
}
