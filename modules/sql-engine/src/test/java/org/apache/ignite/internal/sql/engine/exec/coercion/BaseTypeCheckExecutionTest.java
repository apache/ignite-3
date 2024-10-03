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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.util.CursorUtils;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.provider.Arguments;

/** Base class for check execution results of numeric operations. */
class BaseTypeCheckExecutionTest extends BaseIgniteAbstractTest {
    /** Data provider with reduced objets, helpful to avoid overflow on arithmetic operations. */
    static DataProvider<Object[]> dataProviderReduced(TypePair typePair) {
        Object val1;
        Object val2;

        val1 = generateReducedValueByType(typePair.first());
        val2 = generateReducedValueByType(typePair.second());

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    /** Data provider for multiplication operations. */
    static DataProvider<Object[]> multDivDataProvider(TypePair typePair) {
        Object val1;
        Object val2;

        val1 = generateReducedValueByType(typePair.first());
        val2 = generateConstantValueByType(typePair.second());

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    /** Data provider with constant second object. */
    static DataProvider<Object[]> dataProviderStrict(TypePair typePair) {
        Object val1;
        Object val2;

        val1 = SqlTestUtils.generateValueByType(typePair.first());
        val2 = generateConstantValueByType(typePair.second(), "1");

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    /** Data provider without any restrictions, call directly {@link SqlTestUtils#generateValueByType}. */
    static DataProvider<Object[]> dataProvider(TypePair typePair) {
        Object val1 = SqlTestUtils.generateValueByType(typePair.first());
        Object val2 = SqlTestUtils.generateValueByType(typePair.second());

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    static Stream<Arguments> allNumericPairs() {
        return Arrays.stream(NumericPair.values()).map(Arguments::of);
    }

    private static @Nullable Object generateReducedValueByType(NativeType nativeType) {
        ColumnType type = nativeType.spec().asColumnType();

        switch (type) {
            case INT8:
                return (byte) (((byte) SqlTestUtils.generateValueByType(type, 0, 0)) / 2);
            case INT16:
                return (short) (((short) SqlTestUtils.generateValueByType(type, 0, 0)) / 2);
            case INT32:
                return ((int) SqlTestUtils.generateValueByType(type, 0, 0)) / 2;
            case INT64:
                return (((long) SqlTestUtils.generateValueByType(type, 0, 0)) / 2);
            case FLOAT:
            case DOUBLE:
                return SqlTestUtils.generateValueByType(type, 0, 0);
            case DECIMAL:
                int scale = ((DecimalNativeType) nativeType).scale();
                int precision = ((DecimalNativeType) nativeType).precision();
                return ((BigDecimal) SqlTestUtils.generateValueByType(type, precision, scale))
                        .divide(BigDecimal.valueOf(2), RoundingMode.HALF_DOWN).setScale(scale, RoundingMode.HALF_DOWN);
            default:
                throw new IllegalArgumentException("unsupported type " + type);
        }
    }

    private static Object generateConstantValueByType(NativeType type, String numericBase) {
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
                assert precision >= scale : "unexpected precision/scale, precision=" + precision + ", scale=" + scale;

                BigDecimal bd = new BigDecimal(numericBase);
                return bd.setScale(scale, RoundingMode.UNNECESSARY);
            default:
                throw new AssertionError("Unexpected type: " + type0);
        }
    }

    private static Object generateConstantValueByType(NativeType type) {
        return generateConstantValueByType(type, "2");
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
        private final TestCluster cluster;

        ClusterWrapper(TestCluster cluster) {
            this.cluster = cluster;

            cluster.start();
        }

        void process(String sql, Matcher<Object> resultMatcher) {
            var gatewayNode = cluster.node("N1");
            var plan = gatewayNode.prepare(sql);
            ResultSetMetadata resultMeta = plan.metadata();
            ColumnMetadata colMeta = resultMeta.columns().get(0);

            for (var row : CursorUtils.getAllFromCursor(gatewayNode.executePlan(plan))) {
                assertNotNull(row);
                assertNotNull(row.get(0), "Await not null object");
                assertThat(new Pair<>(row.get(0), colMeta), resultMatcher);
            }
        }

        @Override
        public void close() throws Exception {
            cluster.stop();
        }
    }
}
