/**
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

import org.wikimedia.analytics.refinery.hive.EntropyUDAF.EntropyUDAFEvaluator;

import junitparams.JUnitParamsRunner;
import static org.junit.Assert.assertEquals;
import org.junit.runner.RunWith;
import org.junit.Test;

@RunWith(JUnitParamsRunner.class)
public class TestEntropyUDAF {

    // Used to assert floating point values.
    final double Epsylon = 0.000001;

    /**
     * Emulates a the execution flow of the EntropyUDAF.
     * Calls the UDAF's init(), iterate(), terminatePartial(),
     * merge() and terminate() functions with the corresponding inputs,
     * to mimic the flow of the UDAF.
     * TODO: Find a native way to execute the UDAF from here.
     * I couldn't find any solution or documentation related to this :'(
     */
    Double applyEntropyUDAF(Double[][] input) {
        EntropyUDAFEvaluator baseEvaluator = new EntropyUDAFEvaluator();
        baseEvaluator.init();
        for (int i = 0; i < input.length; i++) {
            EntropyUDAFEvaluator iEvaluator = new EntropyUDAFEvaluator();
            iEvaluator.init();
            for (int j = 0; j < input[i].length; j++) {
                Double count = input[i][j];
                iEvaluator.iterate(count);
            }
            baseEvaluator.merge(iEvaluator.terminatePartial());
        }
        return baseEvaluator.terminate();
    }

    @Test
    public void testShouldReturn0IfNoInput() {
        Double[][] input = {};
        Double result = applyEntropyUDAF(input);
        assertEquals(0.0, result.doubleValue(), Epsylon);
    }

    @Test
    public void testShouldReturnNullIfNegativeInput() {
        Double[][] input = {
            {new Double(-123)}
        };
        Double result = applyEntropyUDAF(input);
        assertEquals(null, result);
    }

    @Test
    public void testShouldReturn0IfSingleCount() {
        Double[][] input = {
            {new Double(123)}
        };
        Double result = applyEntropyUDAF(input);
        assertEquals(0.0, result.doubleValue(), Epsylon);
    }

    @Test
    public void testShouldReturn1IfTwoEqualCounts() {
        Double[][] input = {
            {new Double(123), new Double(123)}
        };
        Double result = applyEntropyUDAF(input);
        assertEquals(1.0, result.doubleValue(), Epsylon);
    }

    @Test
    public void testShouldComputeAcrossPartitions() {
        Double[][] input = {
            {new Double(1), new Double(2)},
            {new Double(3)},
            {},
            {new Double(4), new Double(5)}
        };
        Double result = applyEntropyUDAF(input);
        assertEquals(2.1492553971685, result.doubleValue(), Epsylon);
    }

    @Test
    public void testShouldIgnoreNullValues() {
        Double[][] input = {
            {new Double(1), new Double(2), null},
            {new Double(3), null},
            {null}
        };
        Double result = applyEntropyUDAF(input);
        assertEquals(1.459147917027245, result.doubleValue(), Epsylon);
    }
}
