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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class TestGetMainSearchRequestUDF {
    private GetMainSearchRequestUDF udf;

    @Before
    public void setUp() throws HiveException {
        udf = new GetMainSearchRequestUDF();

        List<String> fields = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        fields.add("querytype");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fields.add("indices");
        fieldOIs.add(ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));

        ObjectInspector wikiOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory
            .getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(fields, fieldOIs));

        udf.initialize(new ObjectInspector[] { wikiOI, listOI });
    }

    @Test
    @Parameters(method = "paramsForTest")
    public void testEvaluate(String message, int expectIndex, String wiki, Object[] requests) throws HiveException {
        GenericUDF.DeferredObject[] dois = {
            new GenericUDF.DeferredJavaObject(wiki), new GenericUDF.DeferredJavaObject(requests) };
        Object expect = null;
        if (expectIndex >= 0) {
            expect = requests[expectIndex];
        }
        assertEquals(message, expect, udf.evaluate(dois));
    }

    private Object[] paramsForTest() {
        return new Object[] {
            // Each 'new Object[]' at this level is a single test case
            // representing a plausible set of search requests that could be
            // logged to the `cirrussearchrequestset` table and the desired
            // index of the primary full text search request in that set of
            // requests.
            new Object[] {
                "simplest passing example", 0, "enwiki",
                new Object[] {
                    // This level is a list of requests made between mediawiki
                    // and elasticsearch in the process of answering a search
                    // request made of mediawiki. Multiple individual requests
                    // are issued between mediawiki and elasticsearch, with the
                    // goal of this UDF being to decide which one of those is
                    // the primary full text search.
                    new Object[] { "full_text", new String[] { "enwiki_content" } }
                } },
            new Object[] {
                // Example of search request to a mediawiki instance configured with
                // sister search. With sistersearch a full text request is made against
                // one or more wikis with the same language but in a different project.
                "including interwiki", 1, "enwiki",
                new Object[] {
                    new Object[] { "near_match", new String[] { "enwiki_content" } },
                    new Object[] { "full_text", new String[] { "enwiki_content" } },
                    new Object[] { "full_text", new String[] { "enwikibooks_content" } } } },
            new Object[] {
                // The order of sister searches doesn't have to be with the local wiki coming
                // first. Make sure the UDF appropriately finds the primary request even
                // if it comes after the sister search.
                "interwiki alternate order", 3, "zhwiki",
                new Object[] {
                    new Object[] { "near_match", new String[] { "zhwiki_content" } },
                    new Object[] { "full_text", new String[] { "zhwikisource_content" } },
                    new Object[] { "full_text", new String[] { "zhwiktionary_content" } },
                    new Object[] { "full_text", new String[] { "zhwiki_content" } } } },
            new Object[] {
                // Example of a search request that was run against multimedia
                // search. In this case the full text search was performed
                // against multiple indices, merging the results from frwiki
                // and commonswiki.
                "multimedia search", 1, "frwiki",
                new Object[] {
                    new Object[] { "near_match", new String[] { "frwiki_general" } },
                    new Object[] { "full_text", new String[] { "frwiki_general", "commonswiki_file" } } } },
            new Object[] {
                // Search requests that ask for the 'everything' profile don't
                // perform their search against a _content or _general index,
                // instead using a top level alias in elasticsearch that refers
                // to both. Ensure the UDF can still find the appropriate
                // search request.
                "search everything", 1, "enwiki",
                new Object[] {
                    new Object[] { "near_match", new String[] { "enwiki" } },
                    new Object[] { "full_text", new String[] { "enwiki", "commonswiki_file" } } } },
            new Object[] {
                // Autocomplete search requests are not considered full text.
                // As such the UDF should return NULL rather than the
                // autocomplete request.
                "completion suggester", -1, "itwiki",
                new Object[] { new Object[] { "comp_suggest", new String[] { "itwiki_titlesuggest" } } } },
            new Object[] {
                // Mediawiki's 'Go' feature takes full text searches submitted
                // in a particular way and looks for 'close enough' titles. If
                // there is a title or redirect very close to the submitted
                // string no full text search is performed and the user is
                // redirected. These requests must be detected as not having a
                // primary full text search
                "successfull 'go'", -1, "arwiki",
                new Object[] { new Object[] { "near_match", new String[] { "arwiki_content" } } } },
            new Object[] {
                // This type of request should generally not be recorded, but
                // test the UDF with some odd inputs to ensure it is robust to
                // the oddities that may occasionally arise.
                "invalid record with null querytype", -1, "zhwiki",
                new Object[] { new Object[] { null, new String[] { "zhwiki_content" } } } },
            new Object[] {
                // This type of request should generally not be recorded, but
                // test the UDF with some odd inputs to ensure it is robust to
                // the oddities that may occasionally arise.
                "invalid record null querytype later", -1, "thwiki",
                new Object[] {
                    new Object[] { "near_match", new String[] { "thwiki_content" } },
                    new Object[] { null, new String[] { "thwiki_content" } } } },
            new Object[] {
                // This type of request should generally not be recorded, but
                // test the UDF with some odd inputs to ensure it is robust to
                // the oddities that may occasionally arise.
                "invalid record with null indices", -1, "thwiki",
                new Object[] {
                    new Object[] { "near_match", new String[] { "thwiki_content" } },
                    new Object[] { "full_text", null } } } };
    }
}
