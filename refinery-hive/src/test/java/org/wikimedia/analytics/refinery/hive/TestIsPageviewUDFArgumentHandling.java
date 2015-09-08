package org.wikimedia.analytics.refinery.hive;

import junitparams.JUnitParamsRunner;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.simple.parser.JSONParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by nuria on 9/8/15.
 * Tests that isPageview UDF can handle a variable number of arguments
 * and generalerrors in argument setting
 */
@RunWith(JUnitParamsRunner.class)
public class TestIsPageviewUDFArgumentHandling {


    IsPageviewUDF udf = null;
    JSONParser parser = null;

    @Before
    public void setUp() throws HiveException{
        udf = new IsPageviewUDF();
        parser = new JSONParser();

    }

    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArgumentsTooFew() throws HiveException{

        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArgumentsFew = new ObjectInspector[]{value1, value2};
        udf.initialize(initArgumentsFew);
    }


    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArgumentsTooMany() throws HiveException{

        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArgumentsTooMany = new ObjectInspector[]{value1, value2, value1, value2, value1, value2, value1, value2};
        udf.initialize(initArgumentsTooMany);
    }

    @Test(expected = UDFArgumentException.class)
    public void testBadArgumentType() throws HiveException{

        // the 7th argument should be a map not a string
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2, value1, value2, value1, value2, value1};
        udf.initialize(initArguments);
    }

    // UDF should work with variable arguments
    public void testGoodNumberOfArguments() throws HiveException{

        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value4 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value5 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value6 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2, value3, value4, value5, value6};
        udf.initialize(initArguments);

        ObjectInspector value7 = ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableIntObjectInspector
        );

        ObjectInspector[] initArgumentsMax = new ObjectInspector[]{value1, value2, value3, value4, value5, value6, value7};
        udf.initialize(initArguments);


    }

}
