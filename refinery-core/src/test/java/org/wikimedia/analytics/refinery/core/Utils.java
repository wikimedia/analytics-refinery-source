package org.wikimedia.analytics.refinery.core;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStringToMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Map;

public class Utils {

    /**
     * Parse the x-analytics header into a map of key-value pairs
     *
     * @param xAnalyticsHeaderStr the x-analytics header string
     *
     * @return Map<String, String> the map of key-value pairs
     *
     * @throws HiveException
     */
    public static final Map<String, String> parseXAnalyticsHeader(String xAnalyticsHeaderStr) throws HiveException {
        // Create a GenericUDFStringToMap object to parse the x-analytics header
        GenericUDFStringToMap stringToMapUDF = new GenericUDFStringToMap();
        // Initialize the GenericUDFStringToMap object with its 3 arguments
        stringToMapUDF.initialize(new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        });
        // Create the 3 arguments for the GenericUDFStringToMap object
        GenericUDF.DeferredObject mainDelimiter = new GenericUDF.DeferredJavaObject(";");
        GenericUDF.DeferredObject secondaryDelimiter = new GenericUDF.DeferredJavaObject("=");
        GenericUDF.DeferredObject deferred_x_analytics_str = new GenericUDF.DeferredJavaObject(xAnalyticsHeaderStr);
        // Create the array of arguments for the GenericUDFStringToMap object
        GenericUDF.DeferredObject[] udfArgs = {deferred_x_analytics_str, mainDelimiter, secondaryDelimiter};
        // Parse the x-analytics header by actually calling the GenericUDFStringToMap object
        Map<String, String> xAnalyticsMap = (Map<String, String>) stringToMapUDF.evaluate(udfArgs);
        return xAnalyticsMap;
    }
}
