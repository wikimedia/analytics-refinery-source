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

import java.math.BigDecimal;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * A hive UDF to sum an array of int's.
 * <p>
 * This is generally useful when dealing with an array of structs. Given a
 * column `foo array<struct<bar:int, baz:int>>` the hive expression foo.bar
 * will result in array<int>, which can then be summed using this UDF. This
 * UDF will accept any numeric type, not just ints.
 * <p>
 * This additionally adds a sigil value to ignore. This was added to support
 * avro schemas we use in production which were unable to use [int,null] unions
 * due to issues getting that mapping through camus and into the files stored
 * in hdfs.
 * <p>
 * Hive Usage:
 *   ADD JAR /path/to/refinery-hive.jar;
 *   CREATE TEMPORARY FUNCTION array_sum as 'org.wikimedia.analytics.refinery.hive.ArraySumUDF';
 *   SELECT array_sum(requests.hitstotal, -1) from cirrussearchrequestset where year=2015 limit 10;
 */
@Description(name = "array_sum",
	value="_FUNC_(array<numeric>, numeric) - returns the sum of an array of numbers with"
	      + " an optional sigil value to ignore",
	extended="")
public class ArraySumUDF extends GenericUDF {
	ListObjectInspector listOI;
	PrimitiveObjectInspector elemOI;
	PrimitiveObjectInspector sigilOI;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
		throws UDFArgumentException{
		GenericUDFHelper argsHelper = new GenericUDFHelper();
		argsHelper.checkArgsSize(arguments, 1, 2);

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

		switch(((PrimitiveObjectInspector) oi).getPrimitiveCategory()) {
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
		for (Object inner : listOI.getList(arguments[0].get())) {
			if (inner == null) {
				continue;
			}
			Object primitive = elemOI.getPrimitiveJavaObject(inner);
			BigDecimal current = new BigDecimal(primitive.toString());
			if (sigil == null || current.compareTo(sigil) != 0) {
				sum = sum.add(current);
			}
		}

		switch(elemOI.getPrimitiveCategory()) {
		case BYTE:
			return sum.byteValue();
		case SHORT:
			return sum.shortValue();
		case INT:
			return sum.intValue();
		case LONG:
			return sum.longValue();
		case FLOAT:
			return sum.floatValue();
		case DOUBLE:
			return sum.doubleValue();
		case DECIMAL:
			return HiveDecimal.create(sum);
		default:
			throw new HiveException("Unknown primitive type for return value: "
				+ elemOI.getTypeName());
		}
	}

	public String getDisplayString(String[] errorInfo) {
		return "array_sum: " + errorInfo[0];
	}
}
