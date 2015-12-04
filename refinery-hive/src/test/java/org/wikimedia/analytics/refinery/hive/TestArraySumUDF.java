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

import java.lang.Integer;
import java.util.List;
import java.util.ArrayList;
import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(JUnitParamsRunner.class)
public class TestArraySumUDF {
	ArraySumUDF udf = null;
	ObjectInspector[] initArguments = null;

	@Before
	public void setUp() throws HiveException {
		udf = new ArraySumUDF();

		ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(
			PrimitiveObjectInspectorFactory.javaIntObjectInspector
		);
		initArguments = new ObjectInspector[]{
			listOI,
			PrimitiveObjectInspectorFactory.javaIntObjectInspector
		};
		udf.initialize(initArguments);
	}

	@Test
	public void testSimpleIntegerSum() {
		List<Integer> list = new ArrayList<Integer>();
		list.add(5);
		list.add(0);
		list.add(10);

		GenericUDF.DeferredObject[] args = {
			new GenericUDF.DeferredJavaObject(list),
			new GenericUDF.DeferredJavaObject(-1)
		};

		try {
			assertEquals("should sum the arguments", 15, udf.evaluate(args));
		} catch (HiveException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDefaultIgnoresNulls() {
		List<Integer> list = new ArrayList<Integer>();
		list.add(5);
		list.add(null);
		list.add(20);

		GenericUDF.DeferredObject[] args = {
			new GenericUDF.DeferredJavaObject(list),
			new GenericUDF.DeferredJavaObject(-1)
		};

		try {
			assertEquals("should ignore null", 25, udf.evaluate(args));
		} catch (HiveException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testIgnoresProvidedSigil() {
		List<Integer> list = new ArrayList<Integer>();
		list.add(-1);
		list.add(7);

		GenericUDF.DeferredObject[] args = {
			new GenericUDF.DeferredJavaObject(list),
			new GenericUDF.DeferredJavaObject(-1)
		};

		try {
			assertEquals("should ignore sigil", 7, udf.evaluate(args));
		} catch (HiveException e) {
			e.printStackTrace();
		}
	}

	public void testSumsLongTypes() throws HiveException {
		udf = new ArraySumUDF();

		ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(
			PrimitiveObjectInspectorFactory.javaLongObjectInspector
		);
		udf.initialize(new ObjectInspector[]{listOI});

		List<Long> list = new ArrayList<Long>();
		list.add(8589934592l);
		list.add(8589934592l);

		GenericUDF.DeferredObject[] args = {
			new GenericUDF.DeferredJavaObject(list)
		};

		try {
			assertEquals("should sum long values", 17179869184l, udf.evaluate(args));
		} catch (HiveException e) {
			e.printStackTrace();
		}
	}

	public void testSumsFloatTypes() throws HiveException {
		udf = new ArraySumUDF();

		ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(
			PrimitiveObjectInspectorFactory.javaFloatObjectInspector
		);
		udf.initialize(new ObjectInspector[]{listOI});

		List<Float> list = new ArrayList<Float>();
		list.add(2.5f);
		list.add(2.5f);

		GenericUDF.DeferredObject[] args = {
			new GenericUDF.DeferredJavaObject(list)
		};

		try {
			assertEquals("should sum float values", 2.5f+2.5f, udf.evaluate(args));
		} catch (HiveException e) {
			e.printStackTrace();
		}
	}
}
