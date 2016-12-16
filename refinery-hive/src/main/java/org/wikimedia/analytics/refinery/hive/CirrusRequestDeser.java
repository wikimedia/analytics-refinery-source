/*
 * Copyright (C) 2014  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * Helper class for dealing with the array<struct<...>> in
 * cirrussearchrequestset.
 *
 * Currently only extracts querytype and indices, but can be expanded as
 * necessary to meet our needs in various UDFs.
 */
class CirrusRequestDeser {
    private final StructObjectInspector oi;

    private final StructField queryTypeField;
    private final StringObjectInspector queryTypeOI;

    private final StructField indicesField;
    private final ListObjectInspector indicesOI;
    private final StringObjectInspector indicesElemOI;

    /**
     * @param argsHelper
     *          Helper object to verify hive structures match the expected formats.
     * @param i
     *          Argument index of oi (what position was oi in the argument list
     *          of the UDF)
     * @param oi
     *          Object inspector for the UDF arguments. This must match the
     *          CirrusSearchRequest structure in the CirrusSearchRequestSet
     *          Avro schema.
     *
     * @throws UDFArgumentTypeException
     *          Thrown when oi does not match the CirrusSearchRequest structure
     *          in the CirrusSearchRequestSet Avro schema.
     *  Thrown when 
     */
    public CirrusRequestDeser(GenericUDFHelper argsHelper, int i, StructObjectInspector oi)
        throws UDFArgumentTypeException {
        this.oi = oi;
        // verify the 'querytype' field is a string
        queryTypeField = oi.getStructFieldRef("querytype");
        if (queryTypeField == null) {
            throw new UDFArgumentTypeException(i,
                argsHelper.getFuncName() + " Argument should contain a struct with a 'querytype' field");
        }
        ObjectInspector queryTypeOI = queryTypeField.getFieldObjectInspector();
        argsHelper.checkArgType(queryTypeOI, i, PrimitiveCategory.STRING);

        this.queryTypeOI = (StringObjectInspector) queryTypeOI;

        // verify the 'indices' field is a list of strings
        indicesField = oi.getStructFieldRef("indices");
        if (indicesField == null) {
            throw new UDFArgumentTypeException(i,
                argsHelper.getFuncName() + " Argument should contain a struct with an 'indices' field");
        }
        argsHelper.checkListArgType(indicesField.getFieldObjectInspector(), i, PrimitiveCategory.STRING);
        indicesOI = (ListObjectInspector) indicesField.getFieldObjectInspector();
        this.indicesElemOI = (StringObjectInspector) indicesOI.getListElementObjectInspector();
    }

    public String getQueryType(Object struct) {
        Object type = oi.getStructFieldData(struct, queryTypeField);
        return queryTypeOI.getPrimitiveJavaObject(type);
    }

    public String[] getIndices(Object struct) {
        Object list = oi.getStructFieldData(struct, indicesField);
        if (list == null) {
            return null;
        }

        int len = indicesOI.getListLength(list);
        String[] indices = new String[len];
        int i = 0;
        for (Object inner : indicesOI.getList(list)) {
            indices[i++] = indicesElemOI.getPrimitiveJavaObject(inner);
        }

        return indices;
    }
}
