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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matcher;

/** Base class for check execution results of numeric operations. */
class BaseTypeCheckExecutionTest {
    static Pair<Object, Object> generateDifferentValues(TypePair typePair) {
        Object objFirst = SqlTestUtils.generateValueByType(typePair.first());
        assert objFirst != null;
        Object objSecond;
        do {
            objSecond = SqlTestUtils.generateValueByType(typePair.second());

            assert objSecond != null;
        } while (objFirst.toString().equals(objSecond.toString()));

        return new Pair<>(objFirst, objSecond);
    }

    static Object generateConstantValueByType(NativeType type) {
        String numericBase = "9";
        ColumnType type0 = type.spec().asColumnType();
        switch (type0) {
            case INT8:
                return Byte.valueOf(numericBase);
            case INT16:
                return Short.valueOf(numericBase);
            case INT32:
                return Integer.valueOf(numericBase);
            case INT64:
                return Long.valueOf(numericBase);
            case FLOAT:
                return Float.valueOf(numericBase);
            case DOUBLE:
                return Double.valueOf(numericBase);
            case DECIMAL:
                int scale = ((DecimalNativeType) type).scale();
                int precision = ((DecimalNativeType) type).precision();
                assert precision >= scale : "unexpected precision\\scale";

                BigDecimal bd = new BigDecimal(numericBase);
                return bd.setScale(scale, RoundingMode.UNNECESSARY);
            default:
                throw new AssertionError("Unexpected type: " + type0);
        }
    }

    static ClusterWrapper testCluster(TypePair typePair, DataProvider<Object[]> dataProvider) {
        TestCluster cluster = TestBuilders.cluster().nodes("N1")
                .addTable().name("T")
                .addKeyColumn("id", NativeTypes.INT32)
                .addColumn("C1", typePair.first())
                .addColumn("C2", typePair.second())
                .end()
                .dataProvider("N1", "T", TestBuilders.tableScan(dataProvider))
                .build();

        return new ClusterWrapper(cluster);
    }

    static class ClusterWrapper implements AutoCloseable {
        private TestCluster cluster;

        ClusterWrapper(TestCluster cluster) {
            this.cluster = cluster;

            cluster.start();
        }

        void process(String sql, Matcher<Object> resultMatcher) {
            var gatewayNode = cluster.node("N1");
            var plan = gatewayNode.prepare(sql);

            for (var row : await(gatewayNode.executePlan(plan).requestNextAsync(10_000)).items()) {
                assertNotNull(row);
                assertThat(row.get(0), resultMatcher);
            }
        }

        @Override
        public void close() throws Exception {
            cluster.stop();
        }
    }
}
