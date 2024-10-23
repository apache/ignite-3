/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.exec.coercion;

import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Check execution results for numeric comparisons. */
public class NumericComparisonExecutionTest extends BaseTypeCheckExecutionTest {
    @ParameterizedTest
    @EnumSource
    public void comparisonEq(NumericPair typePair) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process("SELECT c1 = c2 FROM t", checkReturnResult());
        }
    }

    @ParameterizedTest
    @EnumSource
    public void comparisonLessEq(NumericPair typePair) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process("SELECT c1 <= c2 FROM t", checkReturnResult());
        }
    }

    @ParameterizedTest
    @EnumSource
    public void comparisonGreatEq(NumericPair typePair) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process("SELECT c1 >= c2 FROM t", checkReturnResult());
        }
    }

    @ParameterizedTest
    @EnumSource
    public void comparisonLess(NumericPair typePair) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process("SELECT c1 < c2 FROM t", checkReturnResult());
        }
    }

    @ParameterizedTest
    @EnumSource
    public void comparisonGreat(NumericPair typePair) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process("SELECT c1 > c2 FROM t", checkReturnResult());
        }
    }
}
