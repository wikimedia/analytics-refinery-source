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
 */

package org.wikimedia.analytics.refinery.hive;

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
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
public class ArrayAvgUDF extends ArrayUDFAggregation {

    protected HiveDecimal calculateValue(ListObjectInspector listOI,
                                         BigDecimal sigil,
                                         PrimitiveObjectInspector elemOI,
                                         Object toAggregate) {
        BigDecimal sum = BigDecimal.ZERO;
        int countNoNulls = 0;

        for (Object inner : listOI.getList(toAggregate)) {
            if (inner == null) continue;

            Object primitive = elemOI.getPrimitiveJavaObject(inner);
            BigDecimal current = new BigDecimal(primitive.toString());

            if (sigil == null || current.compareTo(sigil) != 0) {
                countNoNulls += 1;
                sum = sum.add(current);
            }
        }

        if (countNoNulls == 0) return null;

        return HiveDecimal.create(sum.divide(BigDecimal.valueOf(countNoNulls)));
    }

    @Override
    public String getDisplayString(String[] errorInfo) {
        return "array_avg: " + errorInfo[0];
    }
}
