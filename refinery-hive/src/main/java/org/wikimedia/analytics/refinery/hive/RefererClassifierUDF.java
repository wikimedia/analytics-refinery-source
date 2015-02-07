// Copyright 2014 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.wikimedia.analytics.refinery.core.Webrequest;
import org.wikimedia.analytics.refinery.core.Webrequest.RefererClassification;

import java.util.LinkedList;
import java.util.List;

@Description(name = "referer_classifier",
    value = "_FUNC_(url) - Returns a map with a classification of a referer",
    extended = "argument 0 is the url to analyze")
public class RefererClassifierUDF extends GenericUDF {
    private Object[] result;

    private StringObjectInspector inputOI;

    private int IDX_IS_UNKNOWN;
    private int IDX_IS_INTERNAL;
    private int IDX_IS_EXTERNAL;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        // We need exactly 1 parameter
        if (arguments == null || arguments.length != 1) {
            throw new UDFArgumentLengthException("The function "
                    + "RefererClassifierUDF expects exactly 1 parameter");
        }

        // ... and the parameter has to be a string
        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "The parameter to "
                    + "RefererClassifierUDF has to be a string");
        }

        inputOI = (StringObjectInspector) arguments[0];

        List<String> fieldNames = new LinkedList<String>();
        List<ObjectInspector> fieldOIs= new LinkedList<ObjectInspector>();
        int idx = 0;

        fieldNames.add("is_unknown");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
        IDX_IS_UNKNOWN=idx++;

        fieldNames.add("is_internal");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
        IDX_IS_INTERNAL=idx++;

        fieldNames.add("is_external");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
        IDX_IS_EXTERNAL=idx++;

        result = new Object[idx];

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert arguments != null : "Method 'evaluate' of RefererClassifierUDF "
                + "called with null arguments array";
        assert arguments.length == 1 : "Method 'evaluate' of "
                + "RefererClassifierUDF called arguments of length "
                + arguments.length + " (instead of 1)";
        // arguments is an array with exactly 1 entry.

        assert result != null : "Result object has not yet been initialized, "
                + "but evaluate called";
        // result object has been initialized. So it's an array of objects of
        // the right length.

        String url = inputOI.getPrimitiveJavaObject(arguments[0].get());

        RefererClassification kind = Webrequest.classify(url);

        result[IDX_IS_UNKNOWN] = kind == RefererClassification.UNKNOWN;
        result[IDX_IS_INTERNAL] = kind == RefererClassification.INTERNAL;
        result[IDX_IS_EXTERNAL] = kind == RefererClassification.EXTERNAL;

        return result;
    }

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
        return "referer_classifier(" + argument +")";

    }
}
