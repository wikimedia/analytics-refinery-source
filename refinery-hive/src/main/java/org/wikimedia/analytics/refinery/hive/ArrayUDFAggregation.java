/*
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
 */

package org.wikimedia.analytics.refinery.hive;

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Description(name = "array_aggregation",
        value = "_FUNC_(array<numeric>, numeric) - Base Class for all array related UDF functions",
        extended = "")
public abstract class ArrayUDFAggregation extends GenericUDF {

    private ListObjectInspector listOI;
    private PrimitiveObjectInspector elemOI;
    private PrimitiveObjectInspector sigilOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 1, 2);
        checkArgIsListOfPrimitives(arguments, 0);
        if (arguments.length == 2) checkArgNumeric(arguments, 1);

        listOI = (ListObjectInspector) arguments[0];
        elemOI = (PrimitiveObjectInspector) listOI.getListElementObjectInspector();
        sigilOI = arguments.length == 2 ? (PrimitiveObjectInspector) arguments[1] : null;

        return elemOI;
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        BigDecimal sigil = null;

        if (sigilOI != null) {
            Object primitive = sigilOI.getPrimitiveJavaObject(arguments[1].get());
            sigil = BigDecimal.valueOf(Double.parseDouble(primitive.toString()));
        }

        HiveDecimal result = calculateValue(listOI, sigil, elemOI, arguments[0].get());

        return convertToJavaType(elemOI, result);
    }

    protected abstract HiveDecimal calculateValue(ListObjectInspector listOI,
                                                  BigDecimal sigil,
                                                  PrimitiveObjectInspector elemOI,
                                                  Object toAggregate);

    private boolean isNumericObjectInspector(ObjectInspector oi) {
        if ((oi.getCategory() != ObjectInspector.Category.PRIMITIVE)) {
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

    private void checkArgNumeric(ObjectInspector[] arguments, int i) throws UDFArgumentException {
        ObjectInspector argument = arguments[i];
        if (!isNumericObjectInspector(argument)) {
            throw new UDFArgumentTypeException(i, getFuncName() + " only takes numeric as "
                    + getArgOrder(i) + " argument, got " + argument.getCategory());
        }
    }

    private void checkArgIsListOfPrimitives(ObjectInspector[] arguments, int i) throws UDFArgumentException {
        ObjectInspector argument = arguments[i];
        ObjectInspector.Category oiCat = argument.getCategory();
        if (oiCat != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(i, getFuncName() + " only takes list as "
                    + getArgOrder(i) + " argument, got " + oiCat);
        }
        ListObjectInspector listArgument = (ListObjectInspector) argument;
        ObjectInspector listOi = listArgument.getListElementObjectInspector();
        if (!isNumericObjectInspector(listOi)) {
            throw new UDFArgumentTypeException(i, getFuncName() + " only takes list of primitives as "
                    + getArgOrder(i) + " argument, got " + listOi.getCategory());
        }
    }


    @SuppressFBWarnings(value = "URV_CHANGE_RETURN_TYPE", justification = "Looks like a false positive")
    private Object convertToJavaType(PrimitiveObjectInspector elemOI, HiveDecimal result) throws HiveException {

        if (result == null) return null;

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
