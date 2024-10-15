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

import java.util.Set;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Check execution and return results for numeric arithmetics. */
public class NumericBinaryOperationsExecutionTest extends BaseTypeCheckExecutionTest {
    private static final Set<NativeType> APPROXIMATE_NUMERIC_TYPES = Set.of(NativeTypes.DOUBLE, NativeTypes.FLOAT);

    @ParameterizedTest
    @EnumSource
    public void sumOp(NumericPair typePair) throws Exception {
        String sql = "SELECT c1 + c2 FROM t";
        try (ClusterWrapper testCluster = testCluster(typePair, dataProviderReduced(typePair))) {
            testCluster.process(sql, checkReturnResult());
        }
    }

    @ParameterizedTest
    @EnumSource
    public void subtractOp(NumericPair typePair) throws Exception {
        String sql = "SELECT c1 - c2 FROM t";
        try (ClusterWrapper testCluster = testCluster(typePair, dataProviderReduced(typePair))) {
            testCluster.process(sql, checkReturnResult());
        }
    }

    @ParameterizedTest
    @EnumSource
    public void multOp(NumericPair typePair) throws Exception {
        String sql = "SELECT c1 * c2 FROM t";
        try (ClusterWrapper testCluster = testCluster(typePair, multDivDataProvider(typePair))) {
            testCluster.process(sql, checkReturnResult());
        }
    }

    @ParameterizedTest
    @EnumSource
    public void divOp(NumericPair typePair) throws Exception {
        String sql = "SELECT c1 / c2 FROM t";

        try (ClusterWrapper testCluster = testCluster(typePair, dataProviderStrict(typePair))) {
            testCluster.process(sql, checkReturnResult());
        }
    }

    @ParameterizedTest
    @EnumSource
    public void moduloOp(NumericPair typePair) throws Exception {
        // Need to be fixed after: https://issues.apache.org/jira/browse/IGNITE-23349
        if (APPROXIMATE_NUMERIC_TYPES.contains(typePair.first()) || APPROXIMATE_NUMERIC_TYPES.contains(typePair.second())) {
            return;
        }

        String sql = "SELECT c1 % c2 FROM t";

        try (ClusterWrapper testCluster = testCluster(typePair, dataProviderWithNonZeroSecondValue(typePair))) {
            testCluster.process(sql, checkReturnResult());
        }
    }
}
