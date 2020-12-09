package org.wikimedia.analytics.refinery.hive;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
public class TestConvertEZProjectToStandard {
    @Test
    public void testProject() throws HiveException {
        ObjectInspector arg1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ConvertEZProjectToStandard udf =  new ConvertEZProjectToStandard();
        ObjectInspector[] initArguments = new ObjectInspector[]{arg1};
        udf.initialize(initArguments);
        String ezName = "en.z";
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(ezName) };
        Object result = udf.evaluate(args);
        assertEquals(result.toString(), "en.wikipedia");
    }
    @Test
    public void testNoDotProject() throws HiveException {
        ObjectInspector arg1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ConvertEZProjectToStandard udf =  new ConvertEZProjectToStandard();
        ObjectInspector[] initArguments = new ObjectInspector[]{arg1};
        udf.initialize(initArguments);
        String ezName = "es";
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(ezName) };
        Object result = udf.evaluate(args);
        assertEquals(result.toString(), "es.wikipedia");
    }
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidProject() throws HiveException {
        ObjectInspector arg1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ConvertEZProjectToStandard udf =  new ConvertEZProjectToStandard();
        ObjectInspector[] initArguments = new ObjectInspector[]{arg1};
        udf.initialize(initArguments);
        String invalidEZName = "en.z.ads";
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(invalidEZName) };

        Object result = udf.evaluate(args);
    }
}
