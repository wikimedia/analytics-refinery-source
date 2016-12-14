package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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
    private Map<String, String> emptyMap = new HashMap<String,String>();
    private UAParser uaParser;
    private ObjectInspector argumentOI;

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

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "The UAParserUDF takes an array with only 1 element as argument");
        }

        //we are expecting the parameter to be of String type.
        ObjectInspector arg = arguments[0];
        int argIndex = 0;

        if (arg.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(argIndex,
                    "A string argument was expected but an argument of type " + arg.getTypeName()
                            + " was given.");

        }

        // Now that we have made sure that the argument is of primitive type, we can get the primitive
        // category
        PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) arg)
                .getPrimitiveCategory();

        if (primitiveCategory != PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(argIndex,
                    "A string argument was expected but an argument of type " + arg.getTypeName()
                            + " was given.");

        }

        // Instantiate the UAParser
        uaParser = new UAParser();

        argumentOI = arg;
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
    @SuppressWarnings("unchecked")
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert uaParser != null: "Evaluate called without initializing 'uaParser'";

        if (arguments.length == 1 && argumentOI != null && arguments[0] != null) {
            String ua = ((StringObjectInspector) argumentOI).getPrimitiveJavaObject(arguments[0].get());
            return uaParser.getUAMap(ua);
        }

        // Return an empty map in case of arguments irregularity
        return emptyMap;
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
