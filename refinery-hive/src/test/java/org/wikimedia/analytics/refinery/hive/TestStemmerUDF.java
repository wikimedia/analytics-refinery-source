/**
 * Copyright (C) 2015  Wikimedia Foundation
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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.FileParameters;
import junitparams.JUnitParamsRunner;
import junitparams.mappers.CsvWithHeaderMapper;

@RunWith(JUnitParamsRunner.class)
public class TestStemmerUDF {

    ObjectInspector[] initArguments = null;
    StemmerUDF udf = null;

    @Before
    public void setUp() throws HiveException {
        ObjectInspector value1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector value2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        initArguments = new ObjectInspector[] { value1, value2 };
        udf = new StemmerUDF();
        udf.initialize(initArguments);
    }

    @Test
    @FileParameters(value = "../refinery-core/src/test/resources/stemmer_test_data.csv", mapper = CsvWithHeaderMapper.class)
    public void testEvaluate(String text, String lang, String stemmed) throws HiveException {

        GenericUDF.DeferredJavaObject deferredText = new GenericUDF.DeferredJavaObject(text);
        GenericUDF.DeferredJavaObject deferredLang = new GenericUDF.DeferredJavaObject(lang);
        GenericUDF.DeferredObject[] args1 = { deferredText, deferredLang };

        assertEquals(stemmed, udf.evaluate(args1));
    }

    /**
     * @Test public void testNullLanguage() throws HiveException {
     *
     *       GenericUDF.DeferredJavaObject deferredText = new
     *       GenericUDF.DeferredJavaObject("example text");
     *       GenericUDF.DeferredJavaObject deferredLang = new
     *       GenericUDF.DeferredJavaObject(null); GenericUDF.DeferredObject[]
     *       args1 = {deferredText,deferredLang};
     *
     *       assertEquals("example text", udf.evaluate(args1)); }
     *
     * @Test public void testNullText() throws HiveException{
     *       GenericUDF.DeferredJavaObject deferredText = new
     *       GenericUDF.DeferredJavaObject(null); GenericUDF.DeferredJavaObject
     *       deferredLang = new GenericUDF.DeferredJavaObject(null);
     *       GenericUDF.DeferredObject[] args1 = {deferredText,deferredLang};
     *
     *       assertEquals("", udf.evaluate(args1)); }
     *
     * @Test public void testSingleArgument() throws HiveException {
     *
     *       ObjectInspector value1 =
     *       PrimitiveObjectInspectorFactory.javaStringObjectInspector;
     *       ObjectInspector[] initArgumentsSingle = new
     *       ObjectInspector[]{value1};
     *
     *       // override setup such we only have 1 argument ObjectInspector[]
     *       initArgumentsSingleArg = new ObjectInspector[]{value1}; StemmerUDF
     *       udfSingleArg = new StemmerUDF();
     *       udfSingleArg.initialize(initArgumentsSingleArg);
     *
     *       GenericUDF.DeferredJavaObject deferredText = new
     *       GenericUDF.DeferredJavaObject("this is only a test");
     *       GenericUDF.DeferredObject[] args1 = {deferredText};
     *
     *       assertEquals("onli test", udfSingleArg.evaluate(args1)); }
     **/
}
