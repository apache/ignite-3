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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.CursorUtils;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;

/** Base class for check execution results of numeric operations. */
class BaseTypeCheckExecutionTest extends BaseIgniteAbstractTest {
    /** Data provider with reduced objets, helpful to avoid overflow on arithmetic operations. */
    static DataProvider<Object[]> dataProviderReduced(TypePair typePair) {
        Object val1 = generateReducedValueByType(typePair.first());
        Object val2 = generateReducedValueByType(typePair.second());

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    /** Data provider for multiplication operations. */
    static DataProvider<Object[]> multDivDataProvider(TypePair typePair) {
        Object val1 = generateReducedValueByType(typePair.first());
        Object val2 = generateConstantValueByType(typePair.second());

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    /** Data provider with constant second object. */
    static DataProvider<Object[]> dataProviderStrict(TypePair typePair) {
        Object val1 = SqlTestUtils.generateValueByType(typePair.first());
        Object val2 = generateConstantValueByType(typePair.second(), "1");

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    /** Data provider without any restrictions, call directly {@link SqlTestUtils#generateValueByType}. */
    static DataProvider<Object[]> dataProvider(TypePair typePair) {
        Object val1 = SqlTestUtils.generateValueByType(typePair.first());
        Object val2 = SqlTestUtils.generateValueByType(typePair.second());

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    /** Data provider with non zero for second value. */
    static DataProvider<Object[]> dataProviderWithNonZeroSecondValue(TypePair typePair) {
        Object val1 = SqlTestUtils.generateValueByType(typePair.first());
        Object val2 = generateNotZeroValueByType(typePair.second());

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    private static @Nullable Object generateReducedValueByType(NativeType nativeType) {
        ColumnType type = nativeType.spec();

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
        ColumnType type0 = type.spec();
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

    private static Object generateNotZeroValueByType(NativeType nativeType) {
        ColumnType type = nativeType.spec();

        switch (type) {
            case INT8: {
                byte res;

                do {
                    res = (((byte) SqlTestUtils.generateValueByType(type, 0, 0)));
                } while (res == 0);

                return res;
            }
            case INT16: {
                short res;

                do {
                    res = ((short) SqlTestUtils.generateValueByType(type, 0, 0));
                } while (res == 0);

                return res;
            }
            case INT32: {
                int res;

                do {
                    res = (int) SqlTestUtils.generateValueByType(type, 0, 0);
                } while (res == 0);

                return res;
            }
            case INT64: {
                long res;

                do {
                    res = (long) SqlTestUtils.generateValueByType(type, 0, 0);
                } while (res == 0);

                return res;
            }
            case FLOAT: {
                float res;

                do {
                    res = (float) SqlTestUtils.generateValueByType(type, 0, 0);
                } while (Math.signum(res) == 0);

                return res;
            }
            case DOUBLE: {
                double res;

                do {
                    res = (double) SqlTestUtils.generateValueByType(type, 0, 0);
                } while (Math.signum(res) == 0);

                return res;
            }
            case DECIMAL: {
                int scale = ((DecimalNativeType) nativeType).scale();
                int precision = ((DecimalNativeType) nativeType).precision();

                BigDecimal res;

                do {
                    res = ((BigDecimal) SqlTestUtils.generateValueByType(type, precision, scale))
                            .divide(BigDecimal.valueOf(2), RoundingMode.HALF_DOWN).setScale(scale, RoundingMode.HALF_DOWN);
                } while (res.compareTo(BigDecimal.ZERO) == 0);

                return res;
            }
            default:
                throw new IllegalArgumentException("unsupported type " + type);
        }
    }

    static ClusterWrapper testCluster(TypePair typePair, DataProvider<Object[]> dataProvider) {
        TestCluster cluster = TestBuilders.cluster().nodes("N1")
                .defaultAssignmentsProvider(tableName -> (partNum, includeBackups) -> IntStream.range(0, partNum)
                        .mapToObj(part -> List.of("N1"))
                        .collect(Collectors.toList())
                )
                .defaultDataProvider(tableName -> TestBuilders.tableScan(dataProvider))
                .build();

        cluster.start();
        cluster.node("N1").initSchema(
                format("CREATE TABLE t (id INT PRIMARY KEY, c1 {}, c2 {})",
                        native2relationalType(Commons.typeFactory(), typePair.first()),
                        native2relationalType(Commons.typeFactory(), typePair.second())
                )
        );

        return new ClusterWrapper(cluster);
    }

    static class ClusterWrapper implements AutoCloseable {
        private final TestCluster cluster;

        ClusterWrapper(TestCluster cluster) {
            this.cluster = cluster;
        }

        void process(String sql, Matcher<Object> resultMatcher) {
            TestNode gatewayNode = cluster.node("N1");

            AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(sql);
            ColumnMetadata colMeta = cursor.metadata().columns().get(0);

            for (InternalSqlRow row : CursorUtils.getAllFromCursor(cursor)) {
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

    /** Return results matcher, compare return type, precision and scale with analyzed object. */
    static Matcher<Object> checkReturnResult() {
        return new BaseMatcher<>() {
            private Object result;
            private ColumnMetadata meta;
            private int precision;
            private int scale;

            @Override
            public boolean matches(Object actual) {
                assert actual != null;

                Pair<Object, ColumnMetadata> pair = (Pair<Object, ColumnMetadata>) actual;

                result = pair.getFirst();
                meta = pair.getSecond();

                if (result instanceof BigDecimal) {
                    precision = ((BigDecimal) result).precision();
                    scale = ((BigDecimal) result).scale();
                }

                boolean checkPrecisionScale = (result.getClass() != Float.class)
                        && (result.getClass() != Double.class)
                        && (result.getClass() != Boolean.class);

                boolean precisionScaleMatched = true;

                if (checkPrecisionScale) {
                    // Expected that precision and scale of return result is satisfy the return metadata boundaries.
                    precisionScaleMatched = precision <= meta.precision() && scale <= meta.scale();
                }

                return meta.type().javaClass() == result.getClass() && precisionScaleMatched;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Column metadata: " + meta);
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText("Type: " + result.getClass() + ", precision=" + precision + ", scale=" + scale);
            }
        };
    }
}
