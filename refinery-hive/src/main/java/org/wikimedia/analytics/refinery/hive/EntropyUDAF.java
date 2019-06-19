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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Computes the entropy of a series of counts.
 *
 * Example of use:
 *     SELECT ENTROPY(counts) FROM (
 *         SELECT category, COUNT(*) AS counts FROM table GROUP BY category
 *     ) AS aux;
 *
 * The input column needs to be of numerical type, and each value should
 * correspond to the number of times a certain outcome has been observed.
 * For example, if we execute the following query:
 *   SELECT category, COUNT(*) AS counts FROM table GROUP BY category;
 * The results we'd get would be something like:
 *   category  counts
 *   'cat1'    123
 *   'cat2'    234
 *   ...       ...
 *   'catN'    789
 * The counts column is an example of valid input: it's numeric and its
 * values correspond to how many times each category appears in the table.
 *
 * The value returned by this UDAF represents the entropy of the category
 * whose counts are passed in to the function. It will be a number greater
 * or equal than 0. NULL values in the input column are ignored.
 * Negative values in the input column do not make sense (negative counts),
 * and will render the calculation invalid, making the UDAF return NULL.
 * If there are no values in the input column, 0 will be returned.
 */

@Description(
    name = "entropy",
    value = "_FUNC_(counts) - Returns the entropy of the counts.")
public class EntropyUDAF extends UDAF {
    /**
     * Implementation notes.
     * 1) 1-pass optimization:
     *   The common formula for the entropy is: -H = sum(P(i) * log(P(i)))
     *   Where: i iterates over each possible value of a given field,
     *   and P(i) is the probability of that value to show up in a record.
     *   We can calculate P(i) with: f / T (where f=count(i) and T=total),
     *   but this implies that we'd have to know the total count of records
     *   before iterating, thus needing 2 passes: one for counting total
     *   records, and another one for iterating over the entropy formula.
     *   Now, we can apply some factoring out to the formula to be able to
     *   calculate everything in one pass:
     *     -H = sum(P(i) * log(P(i)))
     *     -H = sum(f / T * log(f / T))
     *     -H = sum(f * log(f / T)) / T
     *     -H = sum(f * (log(f) - log(T))) / T
     *     -H = sum(f * log(f) - f * log(T)) / T
     *     -H = (sum(f * log(f)) - sum(f * log(T))) / T
     *     -H = (sum(f * log(f)) - sum(f) * log(T)) / T
     *     -H = (sum(f * log(f)) - T * log(T)) / T
     *     -H = sum(f * log(f)) / T - log(T)
     *   And T is out of the iteration. Now we can accumulate both f * log(f)
     *   and T in a single pass, and in the end combine both to get the entropy.
     * 2) The entropy is computed using base-2 log.
     *   See division by Math.log(2) at the end of the formula.
     * 3) Calculations with floating point numbers are not accurate and can
     *   lead to negative values for very small entropies (very close to 0).
     *   As a quick fix, results smaller than 0 are replaced by 0.
     */

    public static class EntropyUDAFState {
        private double partial_T;
        private double partial_flogf;
        private boolean is_valid;
    }

    public static class EntropyUDAFEvaluator implements UDAFEvaluator {
        private EntropyUDAFState state;

        public EntropyUDAFEvaluator() {
            super();
            state = new EntropyUDAFState();
            init();
        }

        public void init() {
            state.partial_T = 0.0;
            state.partial_flogf = 0.0;
            state.is_valid = true;
        }

        public boolean iterate(Double f) {
            if (state.is_valid && f != null) {
                if (f > 0) {
                    state.partial_T += f;
                    state.partial_flogf += f * Math.log(f);
                } else if (f < 0) {
                    state.is_valid = false;
                }
            }
            return true;
        }

        public EntropyUDAFState terminatePartial() {
            return state;
        }

        public boolean merge(EntropyUDAFState other) {
            state.is_valid &= other.is_valid;
            state.partial_T += other.partial_T;
            state.partial_flogf += other.partial_flogf;
            return true;
        }

        public Double terminate() {
            if (!state.is_valid) {
                return null;
            }
            if (state.partial_T == 0) {
                return Double.valueOf(0.0);
            }
            double T = state.partial_T;
            double flogf = state.partial_flogf;
            double entropy = -flogf / T + Math.log(T);
            double floating_point_safe_entropy = Math.max(entropy, 0.0);
            double log_2_entropy = floating_point_safe_entropy / Math.log(2);
            return Double.valueOf(log_2_entropy);
        }
    }
}
