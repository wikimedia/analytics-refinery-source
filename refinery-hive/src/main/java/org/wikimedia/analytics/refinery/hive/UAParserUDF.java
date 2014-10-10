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
import ua_parser.*;

import java.io.IOException;
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
 *
 * NOTE:  This UDF was original coded as part of the Kraken repository, and may have not
 * received as high of qualitiy code review as we would like.  However, it works as is,
 * and we want to make this functionality available for use.  Please also note that there
 * is currently not a process for ensuring that the dependent ua_parser package is up to date
 * with the latest user agent classification regexes.
 */

@UDFType(deterministic = true)
@Description(name = "ua_parser", value = "_FUNC_(UA) - "
        + "Returns a map with browser_name, browser_major, device, os_name, os_minor, os_major keys and "
        + "the appropriate values for each of them")
public class UAParserUDF extends GenericUDF {
    Map<String, String> result = new HashMap<String, String>();
    public CachingParser cachingParser;
    private ObjectInspector argumentOI;

    // TODO figure out why not everything is logged to hive.log and some logging
    // TODO through log4j stays on the hadoop logs
    static final Logger Log = Logger.getLogger(UAParserUDF.class.getName());

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


        try {
            cachingParser = new CachingParser();
        } catch (IOException e) {
            // no recovery should be possible, log and rethrow
            // runtime exception will be logged to stdout by default
            Log.error(e.getMessage(), e);
            throw new RuntimeException("Failed to instantiate CachingParser");
        }

        argumentOI = arg;
        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    private final String NA = "-";

    private String replaceNA(String str) {
        final String ret;
        if (str == null || str.isEmpty() || str.equals("-")) {
            ret = NA;
        } else {
            ret = str;
        }
        return ret;
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
        result.clear();

        UserAgent browser = null;
        Device device = null;
        OS os = null;

        try {
            if (arguments.length == 1
                    && argumentOI != null && arguments[0] != null) {
                Client c;
                String pattern = ((StringObjectInspector) argumentOI)
                        .getPrimitiveJavaObject(arguments[0].get());

                c = cachingParser.parse(pattern);
                if (c != null) {
                    browser = c.userAgent;
                    device = c.device;
                    os = c.os;
                }
            }
        } catch (Exception e) {
            // catch it all to make sure job does not halt if one record is faulty
            // TODO find out why this gets logged to hadoop but not to hive.log
            Log.error(e.getMessage(), e);
        }

        if (browser != null) {
            result.put("browser_family", replaceNA(browser.family));
            result.put("browser_major", replaceNA(browser.major));
        } else {
            result.put("browser_family", NA);
            result.put("browser_major", NA);
        }

        if (device != null) {
            result.put("device_family", replaceNA(device.family));
        } else {
            result.put("device_family", NA);
        }

        if (os != null) {
            result.put("os_family", replaceNA(os.family));
            result.put("os_major", replaceNA(os.major));
            result.put("os_minor", replaceNA(os.minor));
        } else {
            result.put("os_family", NA);
            result.put("os_major", NA);
            result.put("os_minor", NA);
        }

        return result;
    }

    /**
     * Returns string representation of the function.
     * Will be printed when an error happens.
     * *
     */
    @Override
    public String getDisplayString(String[] arguments) {
        assert (arguments.length == 1);
        return "user_agent_parser(" + arguments[0] + ")";

    }
}
