package org.wikimedia.analytics.refinery.hive;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;




@RunWith(JUnitParamsRunner.class)
public class TestArrayAvgUDF {

    ArrayAvgUDF udf;
    ObjectInspector[] initArguments;


    @Before
    public void setUp() throws HiveException {
        udf = new ArrayAvgUDF();

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
    public void testSimpleIntegerAvg() {
        List<Integer> list = new ArrayList<Integer>();
        list.add(5);
        list.add(0);
        list.add(10);

        GenericUDF.DeferredObject[] args = {
                new GenericUDF.DeferredJavaObject(list),
                new GenericUDF.DeferredJavaObject(-1)
        };

        try {
            assertEquals("should avg the arguments", 5, udf.evaluate(args));
        } catch (HiveException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDefaultIgnoresNulls() {
        List<Integer> list = new ArrayList<Integer>();
        list.add(10);
        list.add(null);
        list.add(20);

        GenericUDF.DeferredObject[] args = {
                new GenericUDF.DeferredJavaObject(list),
                new GenericUDF.DeferredJavaObject(-1)
        };

        try {
            assertEquals("should ignore null", 15, udf.evaluate(args));
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

    public void testAvgLongTypes() throws HiveException {
        udf = new ArrayAvgUDF();

        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaLongObjectInspector
        );
        udf.initialize(new ObjectInspector[]{listOI});

        List<Long> list = new ArrayList<Long>();
        list.add(8589934592L);
        list.add(8589934592L);

        GenericUDF.DeferredObject[] args = {
                new GenericUDF.DeferredJavaObject(list)
        };

        try {
            assertEquals("should avg long values", 8589934592L, udf.evaluate(args));
        } catch (HiveException e) {
            e.printStackTrace();
        }
    }

    public void testSumsFloatTypes() throws HiveException {
        udf = new ArrayAvgUDF();

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
            assertEquals("should avg float values", 2.5f, udf.evaluate(args));
        } catch (HiveException e) {
            e.printStackTrace();
        }
    }
}
