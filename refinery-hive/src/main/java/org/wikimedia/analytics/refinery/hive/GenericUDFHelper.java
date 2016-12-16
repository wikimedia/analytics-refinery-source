package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
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
        checkArgType(arguments, i, Category.PRIMITIVE);
    }

    /**
     * Checks argument category type is primitive
     *
     * @param oi
     * @param i
     *
     * @throws UDFArgumentTypeException
     */
    protected void checkArgPrimitive(ObjectInspector oi, int i) throws UDFArgumentTypeException {
        checkArgType(oi, i, Category.PRIMITIVE);
    }

    /**
     * Checks argument type
     *
     * @param arguments
     * @param i
     * @param type
     *
     * @throws UDFArgumentTypeException
     */
    protected void checkArgType(ObjectInspector[] arguments, int i, PrimitiveCategory type)
        throws UDFArgumentTypeException{
        checkArgType(arguments[i], i, type);
    }

    /**
     * Checks argument type
     *
     * @param oi
     * @param i
     * @param type
     *
     * @throws UDFArgumentTypeException
     */
    protected void checkArgType(ObjectInspector oi, int i, PrimitiveCategory type) throws UDFArgumentTypeException {
        checkArgPrimitive(oi, i);
        PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) oi).getPrimitiveCategory();

        if (primitiveCategory != type) {
            throw new UDFArgumentTypeException(0,
                "A " + type.getClass().getSimpleName() + " argument was expected for "
                + getFuncName() + " but an argument of type " + oi.getTypeName() + " was given.");
        }
    }

    /**
     * Checks argument type
     *
     * @param arguments
     * @param i
     * @param Category
     *
     * @throws UDFArgumentTypeException
     */
    protected void checkArgType(ObjectInspector[] arguments, int i, Category type)
        throws UDFArgumentTypeException{
        checkArgType(arguments[i], i, type);
    }

    protected void checkArgType(ObjectInspector oi, int i, Category type)
        throws UDFArgumentTypeException{
        Category oiCat = oi.getCategory();
        if (oiCat != type) {
            throw new UDFArgumentTypeException(i, getFuncName() + " Argument should be of "
                    + type.getClass().getSimpleName() + " type");
        }
    }

    /**
     * Checks argument type of list and it's elements
     *
     * @param arguments
     * @param i
     * @param Category
     *            Type of list elements
     *
     * @throws UDFArgumentTypeException
     */
    protected void checkListArgType(ObjectInspector[] arguments, int i, Category type)
        throws UDFArgumentTypeException {
        checkListArgType(arguments[i], i, type);
    }

    /**
     * Checks argument type of list and it's elements
     *
     * @param oi
     * @param i
     * @param type
     *            Type of list elements
     *
     * @throws UDFArgumentTypeException
     */
    protected void checkListArgType(ObjectInspector oi, int i, Category type)
        throws UDFArgumentTypeException {
        checkArgType(oi, i, Category.LIST);

        ListObjectInspector listOI = (ListObjectInspector) oi;
        if (listOI.getListElementObjectInspector().getCategory() != type) {
            throw new UDFArgumentTypeException(i,
                "An array<" + type.getClass().getSimpleName() + " argument was expected "
                + "for " + getFuncName() + " but an argument of type array<"
                + listOI.getListElementObjectInspector().getTypeName() + "> was given."
            );
        }
    }

    /**
     * Checks argument type of list and it's elements
     *
     * @param oi
     * @param i
     * @param type
     *            Type of list elements
     *
     * @throws UDFArgumentTypeException
     */
    protected void checkListArgType(ObjectInspector oi, int i, PrimitiveCategory type)
        throws UDFArgumentTypeException {
        checkListArgType(oi, i, Category.PRIMITIVE);
        ListObjectInspector listOI = (ListObjectInspector) oi;
        PrimitiveObjectInspector listElemOI = (PrimitiveObjectInspector) listOI.getListElementObjectInspector();
        if (listElemOI.getPrimitiveCategory() != type) {
            throw new UDFArgumentTypeException(i,
                "An array<" + type.getClass().getSimpleName() + " argument was expected "
                + "for " + getFuncName() + " but an argument of type array<"
                + listElemOI.getTypeName() + "> was given."
            );
        }
    }
}
