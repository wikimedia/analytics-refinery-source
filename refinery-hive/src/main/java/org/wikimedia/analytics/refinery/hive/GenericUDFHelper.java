package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

/**
 * Created by nuria on 9/8/15.
 *
 * Encapsulates methods to check arguments for UDFs
 *
 * For some reason these are all protected in GenericUDF.java
 * See: https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDF.java
 */
public class GenericUDFHelper {

    /**
     * @return String
     */
    protected String getFuncName(){
        return getClass().getSimpleName().substring(10).toLowerCase();
    }

    /*
     * Checks variable argument list, throws exception if not within bounds
     *
     * @param arguments
     * @param min
     * @param max
     * @throws UDFArgumentLengthException
     */
    protected void checkArgsSize(ObjectInspector[] arguments, int min, int max)
        throws UDFArgumentLengthException{
        if (arguments.length < min || arguments.length > max) {
            StringBuilder sb = new StringBuilder();
            sb.append(getFuncName());
            sb.append(" requires ");
            if (min == max) {
                sb.append(min);
            } else {
                sb.append(min).append("..").append(max);
            }
            sb.append(" argument(s), got ");
            sb.append(arguments.length);
            throw new UDFArgumentLengthException(sb.toString());
        }
    }

    /**
     * Checks argument category type is primitive
     *
     * @param arguments
     * @param i
     *
     * @throws UDFArgumentTypeException
     */
    protected void checkArgPrimitive(ObjectInspector[] arguments, int i)
        throws UDFArgumentTypeException{
        ObjectInspector.Category oiCat = arguments[i].getCategory();
        if (oiCat != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(i, getFuncName() + " Argument should be of primitive type");
        }
    }

    /**
     * Checks argument type
     *
     * @param arguments
     * @param i
     *
     * @throws UDFArgumentTypeException
     */
    protected void checkArgType(ObjectInspector[] arguments, int i, PrimitiveCategory type)
            throws UDFArgumentTypeException{

        PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory();

        if (primitiveCategory != type) {
            throw new UDFArgumentTypeException(0,
                "A string argument was expected for " + getFuncName() + " but an argument of type " +
                arguments[i].getTypeName() + " was given."
            );
        }
    }
}
