package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.HashMap;

public class TestUAParserUDFArgumentHandling {


    @Test(expected = UDFArgumentLengthException.class)
    public void testBadNumberOfArguments() throws HiveException {

        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1, value2};
        GetUAPropertiesUDF getUAPropertiesUDF = new GetUAPropertiesUDF();
        getUAPropertiesUDF.initialize(initArguments);

    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testWrongTypeOfArguments() throws HiveException {

        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector[] initArguments = new ObjectInspector[]{value1};
        GetUAPropertiesUDF getUAPropertiesUDF = new GetUAPropertiesUDF();
        getUAPropertiesUDF.initialize(initArguments);

    }


}



