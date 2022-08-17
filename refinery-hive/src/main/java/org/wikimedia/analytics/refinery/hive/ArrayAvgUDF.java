package org.wikimedia.analytics.refinery.hive;

import java.math.BigDecimal;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * Copyright (C) 2022  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.01
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * A hive UDF to get the average an array of numerical values.
 * <p>
 * This is useful if one wishes to calculate the average of two columns.
 * columns `foo:int bar:int ` the hive expression  will result in a numerical result which can then be averaged using this UDF.
 * This udf is meant to ignore nulls should they exist as one of the array elements
 * <p>
 * This additionally adds a sigil value to ignore. This was added to support
 * avro schemas we use in production which were unable to use [int,null] unions
 * due to issues getting that mapping through camus and into the files stored
 * in hdfs.
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION array_avg as 'org.wikimedia.analytics.refinery.hive.ArrayAvgUDF';
 *   SELECT array_avg(array(1,2)) or SELECT array_avg(array(2.0,4.0)) or SELECT array_avg(array(null,2,null,3))
 */
@Description(name = "array_avg",
        value = "_FUNC_(array<numeric>, numeric) - returns the average of an array of numerical values with"
                + " an optional sigil value to ignore",
        extended = "")
public class ArrayAvgUDF extends GenericUDF {

    ListObjectInspector listOI;
    PrimitiveObjectInspector elemOI;
    PrimitiveObjectInspector sigilOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        checkArgsSize(arguments, 1, 2);

        // first argument must be an array of numeric values
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.LIST)) {
            throw new UDFArgumentException("Argument 1 of function "
                    + this.getClass().getCanonicalName() + " must be array but "
                    + arguments[0].getTypeName() + " was found");
        }

        listOI = (ListObjectInspector) arguments[0];

        if (!isNumericObjectInspector(listOI.getListElementObjectInspector())) {
            throw new UDFArgumentException("Argument 1 of function "
                    + this.getClass().getCanonicalName()
                    + " must be an array of numeric primitives but "
                    + elemOI.getTypeName() + " was provided.");
        }
        elemOI = (PrimitiveObjectInspector) listOI.getListElementObjectInspector();

        if (arguments.length == 2) {
            if (!isNumericObjectInspector(arguments[1])) {
                throw new UDFArgumentException("Argument 2 of function "
                        + this.getClass().getCanonicalName() + " must be numeric but "
                        + arguments[2].getTypeName() + " was found");
            }
            sigilOI = (PrimitiveObjectInspector) arguments[1];
        } else {
            sigilOI = null;
        }

        // return type is same as list element type.
        return elemOI;
    }

    private boolean isNumericObjectInspector(ObjectInspector oi) {
        if (!(oi.getCategory().equals(ObjectInspector.Category.PRIMITIVE))) {
            return false;
        }

        switch (((PrimitiveObjectInspector) oi).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }


    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        BigDecimal sigil;
        if (sigilOI == null) {
            sigil = null;
        } else {
            Object primitive = sigilOI.getPrimitiveJavaObject(arguments[1].get());
            sigil = new BigDecimal(primitive.toString());
        }

        BigDecimal sum = BigDecimal.ZERO;
        int countNoNulls = 0;


        for (Object inner : listOI.getList(arguments[0].get())) {
            if (inner == null)
            {
                continue;
            }

            Object primitive = elemOI.getPrimitiveJavaObject(inner);
            BigDecimal current = new BigDecimal(primitive.toString());
            if (sigil == null || current.compareTo(sigil) != 0)
            {
                countNoNulls += 1;
                sum = sum.add(current);
            }
        }
        Double otherResult = Double.NaN;

        if (countNoNulls > 0 ) {
            otherResult = sum.doubleValue() / countNoNulls;
        }


        if (otherResult.isInfinite() || otherResult.isNaN())
        {
            return null;
        }
        else
        {
            BigDecimal avg = BigDecimal.valueOf(otherResult);

            HiveDecimal result = HiveDecimal.create(avg);

            switch (elemOI.getPrimitiveCategory()) {
                case BYTE:
                    return result.byteValue();
                case SHORT:
                    return result.shortValue();
                case INT:
                    return result.intValue();
                case LONG:
                    return result.longValue();
                case FLOAT:
                    return result.floatValue();
                case DOUBLE:
                    return result.doubleValue();
                case DECIMAL:
                    return result;
                default:
                    throw new HiveException("Unknown primitive type for return value: "
                            + elemOI.getTypeName());
            }
        }
    }

    @Override
    public String getDisplayString(String[] errorInfo) {
        return "array_avg: " + errorInfo[0];
    }
}
