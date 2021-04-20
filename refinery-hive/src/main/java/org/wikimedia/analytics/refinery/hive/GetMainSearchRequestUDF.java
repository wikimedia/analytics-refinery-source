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

import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Returns the primary full text search request from provided list of requests.
 *
 * Operates on the specialized format of the `requests` field in the
 * wmf_raw.CirrusSearchRequestSets table to locate the primary full text search
 * request from a set of requests. When doing a full text search in mediawiki
 * there are a number of auxilliary requests performed: An initial near match
 * search for similar titles to be redirected to directly, the full text
 * searches for the current wiki and a number of it's sister wikis. There are
 * additionally second try searches that are performed if the first full text
 * search performs poorly. These may be a new full text search for a suggested
 * term, or a search against a wiki in the same project but in another
 * language. The search log contains an entry for every one of these individual
 * requests.
 *
 * Some analytics tasks only need to know about the primary request. This UDF
 * takes in the name of the current wiki and the list of requests made, and
 * returns a single reuqest from that list, which is the primary full text
 * search request.
 */
@Description(name = "get_main_search_request",
    value = "_FUNC_(wiki, requests) - Returns the primary full text search request from requests")
@UDFType(deterministic = true)
public class GetMainSearchRequestUDF extends GenericUDF {
    private Converter[] converters = new Converter[2];
    private PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];

    final private static String FULL_TEXT = "full_text";

    private ListObjectInspector listOI;
    private CirrusRequestDeser deser;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 2, 2);

        // first argument must be a string
        checkArgPrimitive(arguments, 0);
        checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);
        obtainStringConverter(arguments, 0, inputTypes, converters);

        GenericUDFHelper argsHelper = new GenericUDFHelper();
        // second argument must be an array of structs
        argsHelper.checkListArgType(arguments, 1, ObjectInspector.Category.STRUCT);
        listOI = (ListObjectInspector) arguments[1];
        StructObjectInspector elemOI = (StructObjectInspector) listOI.getListElementObjectInspector();
        // deser will do reset of the validation
        deser = new CirrusRequestDeser(argsHelper, 1, elemOI);

        // Return value is a struct from the list
        return elemOI;
    }

    @Override
    public String getDisplayString(String[] arguments) {
        return "GetMainSearchRequest(" + arguments[0] + ", " + arguments[1] + ")";
    }

    @Override
    public Object evaluate(DeferredObject[] dos) throws HiveException {
        String wiki = getStringValue(dos, 0, converters);
        if (wiki == null) {
            return null;
        }
        if (dos[1].get() == null) {
            return null;
        }
        if (listOI.getListLength(dos[1].get()) == 0) {
            return null;
        }

        List<?> requests = listOI.getList(dos[1].get());
        String prefix = wiki + "_";
        for (Object request : requests) {
            String[] indices = deser.getIndices(request);
            if (indices == null) {
                continue;
            }
            for (String index : indices) {
                // If the request was made against a different wiki
                // ignore it, it's likely part of interwiki search.
                if (index != null && (index.equals(wiki) || index.startsWith(prefix))
                // At least one request must be a full_text query
                    && FULL_TEXT.equals(deser.getQueryType(request))) {
                    return request;
                }
            }
        }

        return null;
    }
}
