package org.wikimedia.analytics.refinery.hive;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class TestGetRefererDataUDF {
    @Test
    @FileParameters(
            value = "../refinery-core/src/test/resources/referer_classify_struct_test_data.csv",
            mapper = CsvWithHeaderMapper.class
    )
    public void testGetRefererStrutData(
            String test_description,
            String refererUrl,
            String referer_class,
            String referer_name
    ) throws HiveException, IOException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetRefererDataUDF getRefererDataUDF = new GetRefererDataUDF();
        getRefererDataUDF.initialize(initArguments);

        DeferredObject[] args = new DeferredObject[] { new DeferredJavaObject(refererUrl) };
        Object[] resultData = (Object[]) getRefererDataUDF.evaluate(args);

        assertEquals("Invalid length of Output array", 2, resultData.length);

        assertEquals(test_description + " - class of referer", referer_class, resultData[0]);
        assertEquals(test_description + " - name of referer", referer_name, resultData[1]);

        getRefererDataUDF.close();

    }
}
