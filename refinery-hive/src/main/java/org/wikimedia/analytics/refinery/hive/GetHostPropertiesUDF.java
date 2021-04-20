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
    private Converter[] converters = new Converter[1];
    private PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];

    private Object[] result;

    private Webrequest webrequest;

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

        checkArgsSize(arguments, 1, 1);
        checkArgPrimitive(arguments, 0);
        checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);
        obtainStringConverter(arguments, 0, inputTypes, converters);

        // Instantiate the Webrequest
        webrequest = Webrequest.getInstance();

        List<String> fieldNames = new LinkedList<>();
        List<ObjectInspector> fieldOIs= new LinkedList<>();
        int idx = 0;

        fieldNames.add("project_class");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_PROJECT_CLASS=idx++;

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

        fieldNames.add("project_family");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_PROJECT_FAMILY=idx++;

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
        assert result != null : "Result object has not yet been initialized, "
                + "but evaluate called";
        // result object has been initialized. So it's an array of objects of
        // the right length.

        String uriHost = getStringValue(arguments, 0, converters);

        NormalizedHostInfo normHost = webrequest.normalizeHost(uriHost);

        if (normHost == null) {
            result[IDX_PROJECT_CLASS] = NormalizedHostInfo.EMPTY_NORM_HOST_VALUE;
            result[IDX_PROJECT] = NormalizedHostInfo.EMPTY_NORM_HOST_VALUE;
            result[IDX_QUALIFIERS] = new ArrayList<String>();
            result[IDX_TLD] = NormalizedHostInfo.EMPTY_NORM_HOST_VALUE;
            result[IDX_PROJECT_FAMILY] = NormalizedHostInfo.EMPTY_NORM_HOST_VALUE;
        } else {
            result[IDX_PROJECT_CLASS] = normHost.getProjectFamily();
            result[IDX_PROJECT] = normHost.getProject();
            result[IDX_QUALIFIERS] = normHost.getQualifiers();
            result[IDX_TLD] = normHost.getTld();
            result[IDX_PROJECT_FAMILY] = normHost.getProjectFamily();
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
