package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.wikimedia.analytics.refinery.core.referer.RefererClassifier;

import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

/**
 * A UDF that analyses and classifies a referer url and returns a struct.
 * The struct returned contains information of the referer class and the
 * referer name where applicable.
 * eg. SELECT get_referer_data('http://www.bing.com/search?q=Svengali+movie+1931&filters=ufn')
 *     FROM test_table LIMIT 1;
 * Returns:
 * {
 *     "referer_class":"external (search_engine)",
 *     "referer_name":"Bing"
 * }
 * Struct fields can be accessed using dotted notation. For Example:
 * SELECT get_referer_data('http://www.bing.com/search?q=Svengali+movie+1931&filters=ufn').referer_name
 * FROM test_table LIMIT 1;
 * Will return: Bing
 */

@UDFType(deterministic = true)
@Description(name = "get_referer_data",
        value = "_FUNC_(referer) - Returns a struct with referer_class and referer_name",
        extended = "argument 0 is the referer url to analyze")
public class GetRefererDataUDF extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[1];
    private PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[1];

    private Object[] result;

    private RefererClassifier classifier;

    private int REFERER_CLASS_IDX;
    private int REFERER_NAME_IDX;

    /**
     * The initialize method is called only once during the lifetime of the UDF.
     * <p/>
     * Method checks for the validity (number, type, etc)
     * of the arguments being passed to the UDF.
     * It also sets the return type of the result of the UDF,
     * in this case the ObjectInspector equivalent of
     * Map<String,Object>
     *
     * @param arguments an ObjectInspector array
     * @return
     * @throws UDFArgumentException
     */
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        //perform checks on the input argument
        checkArgsSize(arguments, 1, 1);
        checkArgPrimitive(arguments, 0);
        checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);
        obtainStringConverter(arguments, 0, inputTypes, converters);

        // Instantiate the referer classifier
        classifier = RefererClassifier.getInstance();

        List<String> fieldNames = new LinkedList<>();
        List<ObjectInspector> fieldOIs= new LinkedList<>();
        int idx = 0;

        fieldNames.add("referer_class");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        REFERER_CLASS_IDX=idx++;

        fieldNames.add("referer_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        REFERER_NAME_IDX=idx++;

        result = new Object[idx];

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * it takes the referer url as argument and returns an
     * an Object as result which is an array containing the
     * values of the struct fields.
     *
     * @param arguments DeferredObject array
     * @return Object
     * @throws HiveException
     */
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert result != null : "Result object is not initialized, but evaluate is called";

        // result object has been initialized. So it's an array of objects of
        // the right length.

        String refererUrl = getStringValue(arguments, 0, converters);

        RefererClassifier.RefererData refererInfo = classifier.classifyReferer(refererUrl);

        if (refererInfo == null) {
            result[REFERER_CLASS_IDX] = "-";
            result[REFERER_NAME_IDX] = "-";
        } else {
            result[REFERER_CLASS_IDX] = refererInfo.getRefererClassLabel().getRefLabel();
            result[REFERER_NAME_IDX] = refererInfo.getRefererIdentified();
        }

        return result;
    }


    /**
     * Returns a string representation of the function's usage.
     * @param arguments
     * @return String
     */
    public String getDisplayString(String[] arguments) {
        String refererUrl;
        if (arguments == null) {
            refererUrl = "<arguments == null>";
        } else if (arguments.length == 1) {
            refererUrl = arguments[0];
        } else {
            refererUrl = "<arguments of length " + arguments.length + ">";
        }
        return "get_referer_data(" + refererUrl +")";

    }


}
