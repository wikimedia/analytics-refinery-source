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
import org.wikimedia.analytics.refinery.core.*;

import java.util.*;

/**
 * UDF that normalizes host (lower case, split) and returns a struct.
 * Records are processed one by one.<p/>
 * NOTE: project_class is renamed to project_family - we currently provide both.
 * Example:<br/>
 * SELECT get_host_properties('en.m.zero.wikipedia.org') FROM test_table LIMIT 1;<br/>
 * Returns:<br/>
 * {
 * "project_class":"wikipedia",
 * "project_family":"wikipedia",
 * "project":"en",
 * "qualifiers":["m", "zero"],
 * "tld":"org",
 * }
 * <p/>
 * Struct fields can be access using dotted notation:<br/>
 * SELECT get_host_properties('en.m.zero.wikipedia.org').project FROM test_table LIMIT 1;<br/>
 * Returns: en<br/>
 */

@UDFType(deterministic = true)
@Description(name = "get_host_properties", value = "_FUNC_(uri_host) - "
        + "Returns a map with project_family, project, qualifiers, tld keys and "
        + "the appropriate values for each of them")
public class GetHostPropertiesUDF extends GenericUDF {
    private Object[] result;

    private Webrequest webrequest;

    private StringObjectInspector argumentOI;

    private int IDX_PROJECT_CLASS;
    private int IDX_PROJECT_FAMILY;
    private int IDX_PROJECT;
    private int IDX_QUALIFIERS;
    private int IDX_TLD;


    /**
     * The initialize method is called only once during the lifetime of the UDF.
     * <p/>
     * Method checks for the validity (number, type, etc)
     * of the arguments being passed to the UDF.
     * It also sets the return type of the result of the UDF,
     * in this case the ObjectInspector equivalent of
     * Map<String,Object>
     *
     * @param arguments
     * @return ObjectInspector Map<String,Object>
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "The GetHostPropertiesUDF takes an array with only 1 element as argument");
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

        // Instantiate the Webrequest
        webrequest = Webrequest.getInstance();

        argumentOI = (StringObjectInspector) arg;
        List<String> fieldNames = new LinkedList<>();
        List<ObjectInspector> fieldOIs= new LinkedList<>();
        int idx = 0;

        fieldNames.add("project_class");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_PROJECT_CLASS=idx++;

        fieldNames.add("project_family");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_PROJECT_FAMILY=idx++;

        fieldNames.add("project");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_PROJECT=idx++;

        fieldNames.add("qualifiers");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector));
        IDX_QUALIFIERS=idx++;

        fieldNames.add("tld");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_TLD=idx++;

        result = new Object[idx];

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
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
        assert arguments != null : "Method 'evaluate' of GetHostPropertiesUDF "
                + "called with null arguments array";
        assert arguments.length == 1 : "Method 'evaluate' of "
                + "GetHostPropertiesUDF called arguments of length "
                + arguments.length + " (instead of 1)";
        // arguments is an array with exactly 1 entry.

        assert result != null : "Result object has not yet been initialized, "
                + "but evaluate called";
        // result object has been initialized. So it's an array of objects of
        // the right length.

        String uriHost = argumentOI.getPrimitiveJavaObject(arguments[0].get());

        NormalizedHostInfo normHost = webrequest.normalizeHost(uriHost);

        if (normHost == null) {
            result[IDX_PROJECT_CLASS] = NormalizedHostInfo.EMPTY_NORM_HOST_VALUE;
            result[IDX_PROJECT_FAMILY] = NormalizedHostInfo.EMPTY_NORM_HOST_VALUE;
            result[IDX_PROJECT] = NormalizedHostInfo.EMPTY_NORM_HOST_VALUE;
            result[IDX_QUALIFIERS] = new ArrayList<String>();
            result[IDX_TLD] = NormalizedHostInfo.EMPTY_NORM_HOST_VALUE;
        } else {
            result[IDX_PROJECT_CLASS] = normHost.getProjectFamily();
            result[IDX_PROJECT_FAMILY] = normHost.getProjectFamily();
            result[IDX_PROJECT] = normHost.getProject();
            result[IDX_QUALIFIERS] = normHost.getQualifiers();
            result[IDX_TLD] = normHost.getTld();
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
        String argument;
        if (arguments == null) {
            argument = "<arguments == null>";
        } else if (arguments.length == 1) {
            argument = arguments[0];
        } else {
            argument = "<arguments of length " + arguments.length + ">";
        }
        return "get_host_properties(" + argument +")";

    }
}
