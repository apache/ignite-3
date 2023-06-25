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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Cast expression tests.
 */
public class CastExpressionTest {

    private static final String NUMERIC_OVERFLOW_ERROR = "Numeric overflow";

    private static final String NUMERIC_FORMAT_ERROR = "neither a decimal digit number";

    private final DataProvider<Object[]> dataProvider = DataProvider.fromRow(
            new Object[]{42, UUID.randomUUID().toString()}, 3_333
    );

    // @formatter:off
    private final TestCluster cluster = TestBuilders.cluster()
            .nodes("N1")
            .addTable()
            .name("T1")
            .distribution(IgniteDistributions.hash(List.of(0)))
            .addColumn("ID", NativeTypes.INT32)
            .addColumn("VAL", NativeTypes.stringOf(64))
            .defaultDataProvider(dataProvider)
            .end()
            .build();
    // @formatter:on

    private final TestNode gatewayNode = cluster.node("N1");

    /** Starts the cluster and prepares the plan of the query. */
    @BeforeEach
    public void setUp() {
        cluster.start();
    }

    /** Stops the cluster. */
    @AfterEach
    public void tearDown() throws Exception {
        cluster.stop();
    }

    /** varchar casts - literals. */
    @ParameterizedTest
    @MethodSource("varcharCasts")
    public void testVarcharCastsLiterals(String value, RelDataType type, String result) {
        String query = format("SELECT CAST('{}' AS {})", value, type);
        sql(query).returns(result).ok();
    }

    /** varchar casts - dynamic params. */
    @ParameterizedTest
    @MethodSource("varcharCasts")
    public void testVarcharCastsDynamicParams(String value, RelDataType type, String result) {
        String query = format("SELECT CAST(? AS {})", type);
        sql(query).withParams(value).returns(result).ok();
    }

    private static Stream<Arguments> varcharCasts() {
        return Stream.of(
                // varchar
                arguments("abcde", varcharType(3), "abc"),
                arguments("abcde", varcharType(5), "abcde"),
                arguments("abcde", varcharType(6), "abcde"),
                arguments("abcde", varcharType(), "abcde"),

                // char
                arguments("abcde", charType(), "a"),
                arguments("abcde", charType(3), "abc")
        );
    }

    /** decimal casts - cast literal to decimal. */
    @ParameterizedTest(name = "{2}:{1} AS {3} = {4}")
    @MethodSource("decimalCastFromLiterals")
    public void testDecimalCastsNumericLiterals(CaseStatus status, RelDataType inputType, Object input,
            RelDataType targetType, Result<BigDecimal> result) {

        Assumptions.assumeTrue(status == CaseStatus.RUN);

        String literal = asLiteral(input, inputType);
        String query = format("SELECT CAST({} AS {})", literal, targetType);

        sql(query).expect(result);
    }

    private static Stream<Arguments> decimalCastFromLiterals() {
        RelDataType varcharType = varcharType();
        // ignored
        RelDataType numeric = decimalType(4);

        return Stream.of(
                // String
                arguments(CaseStatus.RUN, varcharType, "100", decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.RUN, varcharType, "100.12", decimalType(5, 1), bigDecimalVal("100.1")),
                arguments(CaseStatus.RUN, varcharType, "lame", decimalType(5, 1), error(NUMERIC_FORMAT_ERROR)),
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, varcharType, "100.12", decimalType(1, 0), error(NUMERIC_OVERFLOW_ERROR)),

                // Numeric
                arguments(CaseStatus.RUN, numeric, "100", decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.RUN, numeric, "100", decimalType(3, 0), bigDecimalVal("100")),
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, numeric, "100.12", decimalType(5, 1), bigDecimalVal("100.1")),
                arguments(CaseStatus.SKIP, numeric, "100.12", decimalType(5, 0), bigDecimalVal("100")),
                arguments(CaseStatus.SKIP, numeric, "100", decimalType(2, 0), error(NUMERIC_OVERFLOW_ERROR)),
                arguments(CaseStatus.SKIP, numeric, "100.12", decimalType(5, 2), bigDecimalVal("100.12"))
        );
    }

    /** decimal casts - cast dynamic param to decimal. */
    @ParameterizedTest(name = "{2}:?{1} AS {3} = {4}")
    @MethodSource("decimalCasts")
    public void testDecimalCastsDynamicParams(CaseStatus ignore, RelDataType inputType, Object input,
            RelDataType targetType, Result<BigDecimal> result) {
        // We ignore status because every case should work for dynamic parameter.

        String query = format("SELECT CAST(? AS {})", targetType);

        sql(query).withParams(input).expect(result);
    }

    /** decimals casts - cast numeric literal to specific type then cast the result to decimal. */
    @ParameterizedTest(name = "{1}: {2}::{1} AS {3} = {4}")
    @MethodSource("decimalCasts")
    public void testDecimalCastsFromNumeric(CaseStatus status, RelDataType inputType, Object input,
            RelDataType targetType, Result<BigDecimal> result) {

        Assumptions.assumeTrue(status == CaseStatus.RUN);

        String literal = asLiteral(input, inputType);
        String query = format("SELECT CAST({}::{} AS {})", literal, inputType, targetType);

        sql(query).expect(result);
    }

    static String asLiteral(Object value, RelDataType type) {
        if (SqlTypeUtil.isCharacter(type)) {
            String str = (String) value;
            return format("'{}'", str);
        } else {
            return String.valueOf(value);
        }
    }

    /**
     * Indicates whether a test case should run or should be skipped.
     * We need this because the set of test cases is the same for both dynamic params
     * and numeric values.
     *
     * <p>TODO Should be removed after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
     */
    enum CaseStatus {
        /** Case should run. */
        RUN,
        /** Case should be skipped. */
        SKIP
    }

    private static Stream<Arguments> decimalCasts() {
        RelDataType varcharType = varcharType();
        RelDataType tinyIntType = sqlType(SqlTypeName.TINYINT);
        RelDataType smallIntType = sqlType(SqlTypeName.SMALLINT);
        RelDataType integerType = sqlType(SqlTypeName.INTEGER);
        RelDataType bigintType = sqlType(SqlTypeName.BIGINT);
        RelDataType realType = sqlType(SqlTypeName.REAL);
        RelDataType doubleType = sqlType(SqlTypeName.DOUBLE);

        return Stream.of(
                // String
                arguments(CaseStatus.RUN, varcharType, "100", decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.RUN, varcharType, "100", decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.RUN, varcharType, "100", decimalType(3, 0), bigDecimalVal("100")),
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, varcharType, "100", decimalType(4, 1), bigDecimalVal("100.0")),
                arguments(CaseStatus.SKIP, varcharType, "100", decimalType(2, 0), error(NUMERIC_OVERFLOW_ERROR)),

                // Tinyint
                arguments(CaseStatus.SKIP, tinyIntType, (byte) 100, decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.RUN, tinyIntType, (byte) 100, decimalType(3, 0), bigDecimalVal("100")),
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, tinyIntType, (byte) 100, decimalType(4, 1), bigDecimalVal("100.0")),
                arguments(CaseStatus.SKIP, tinyIntType, (byte) 100, decimalType(2, 0), error(NUMERIC_OVERFLOW_ERROR)),

                // Smallint
                arguments(CaseStatus.RUN, smallIntType, (short) 100, decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.RUN, smallIntType, (short) 100, decimalType(3, 0), bigDecimalVal("100")),
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, smallIntType, (short) 100, decimalType(4, 1), bigDecimalVal("100.0")),
                arguments(CaseStatus.SKIP, smallIntType, (short) 100, decimalType(2, 0), error(NUMERIC_OVERFLOW_ERROR)),

                // Integer
                arguments(CaseStatus.RUN, integerType, 100, decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.RUN, integerType, 100, decimalType(3, 0), bigDecimalVal("100")),
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, integerType, 100, decimalType(4, 1), bigDecimalVal("100.0")),
                arguments(CaseStatus.SKIP, integerType, 100, decimalType(2, 0), error(NUMERIC_OVERFLOW_ERROR)),

                // Bigint
                arguments(CaseStatus.RUN, bigintType, 100L, decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.RUN, bigintType, 100L, decimalType(3, 0), bigDecimalVal("100")),
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, bigintType, 100L, decimalType(4, 1), bigDecimalVal("100.0")),
                arguments(CaseStatus.SKIP, bigintType, 100L, decimalType(2, 0), error(NUMERIC_OVERFLOW_ERROR)),

                // Real
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, realType, 100.0f, decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.SKIP, realType, 100.0f, decimalType(3, 0), bigDecimalVal("100")),
                arguments(CaseStatus.SKIP, realType, 100.0f, decimalType(4, 1), bigDecimalVal("100.0")),
                arguments(CaseStatus.SKIP, realType, 100.0f, decimalType(2, 0), error(NUMERIC_OVERFLOW_ERROR)),
                arguments(CaseStatus.SKIP, realType, 0.1f, decimalType(1, 1), bigDecimalVal("0.1")),
                arguments(CaseStatus.SKIP, realType, 0.1f, decimalType(2, 2), bigDecimalVal("0.10")),
                arguments(CaseStatus.SKIP, realType, 10.12f, decimalType(2, 1), error(NUMERIC_OVERFLOW_ERROR)),
                arguments(CaseStatus.SKIP, realType, 0.12f, decimalType(1, 2), error(NUMERIC_OVERFLOW_ERROR)),

                // Double
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, doubleType, 100.0d, decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.SKIP, doubleType, 100.0d, decimalType(3, 0), bigDecimalVal("100")),
                arguments(CaseStatus.SKIP, doubleType, 100.0d, decimalType(4, 1), bigDecimalVal("100.0")),
                arguments(CaseStatus.SKIP, doubleType, 100.0d, decimalType(2, 0), error(NUMERIC_OVERFLOW_ERROR)),
                arguments(CaseStatus.SKIP, doubleType, 0.1d, decimalType(1, 1), bigDecimalVal("0.1")),
                arguments(CaseStatus.SKIP, doubleType, 0.1d, decimalType(2, 2), bigDecimalVal("0.10")),
                arguments(CaseStatus.SKIP, doubleType, 10.12d, decimalType(2, 1), error(NUMERIC_OVERFLOW_ERROR)),
                arguments(CaseStatus.SKIP, doubleType, 0.12d, decimalType(1, 2), error(NUMERIC_OVERFLOW_ERROR)),

                // Decimal
                arguments(CaseStatus.RUN, decimalType(1, 1), new BigDecimal("0.1"), decimalType(1, 1), bigDecimalVal("0.1")),
                arguments(CaseStatus.RUN, decimalType(3), new BigDecimal("100"), decimalType(3), bigDecimalVal("100")),
                arguments(CaseStatus.RUN, decimalType(3), new BigDecimal("100"), decimalType(3, 0), bigDecimalVal("100")),
                // TODO Uncomment these test cases after https://issues.apache.org/jira/browse/IGNITE-19822 is fixed.
                arguments(CaseStatus.SKIP, decimalType(3), new BigDecimal("100"), decimalType(4, 1), bigDecimalVal("100.0")),
                arguments(CaseStatus.SKIP, decimalType(3), new BigDecimal("100"), decimalType(2, 0), error(NUMERIC_OVERFLOW_ERROR)),
                arguments(CaseStatus.SKIP, decimalType(1, 1), new BigDecimal("0.1"), decimalType(2, 2), bigDecimalVal("0.10")),
                arguments(CaseStatus.SKIP, decimalType(4, 2), new BigDecimal("10.12"), decimalType(2, 1), error(NUMERIC_OVERFLOW_ERROR)),
                arguments(CaseStatus.SKIP, decimalType(2, 2), new BigDecimal("0.12"), decimalType(1, 2), error(NUMERIC_OVERFLOW_ERROR)),
                arguments(CaseStatus.SKIP, decimalType(1, 1), new BigDecimal("0.1"), decimalType(1, 1), bigDecimalVal("0.1"))
        );
    }

    private static RelDataType sqlType(SqlTypeName typeName) {
        return Commons.typeFactory().createSqlType(typeName);
    }

    private static RelDataType decimalType(int precision, int scale) {
        return Commons.typeFactory().createSqlType(SqlTypeName.DECIMAL, precision, scale);
    }

    private static RelDataType decimalType(int precision) {
        return Commons.typeFactory().createSqlType(SqlTypeName.DECIMAL, precision, RelDataType.SCALE_NOT_SPECIFIED);
    }

    private static RelDataType varcharType(int length) {
        return Commons.typeFactory().createSqlType(SqlTypeName.VARCHAR, length);
    }

    private static RelDataType varcharType() {
        return Commons.typeFactory().createSqlType(SqlTypeName.VARCHAR);
    }

    private static RelDataType charType(int length) {
        return Commons.typeFactory().createSqlType(SqlTypeName.CHAR, length);
    }

    private static RelDataType charType() {
        return Commons.typeFactory().createSqlType(SqlTypeName.CHAR);
    }

    private Checker sql(String query) {
        return new Checker(query, gatewayNode);
    }

    /**
     * Result contains a {@code BigDecimal} value represented by the given string.
     */
    private static Result<BigDecimal> bigDecimalVal(String value) {
        return new Result<>(new BigDecimal(value), null);
    }

    /** Result contains an error which message contains the following substring. */
    private static <T> Result<T> error(String error) {
        return new Result<>(null, error);
    }

    /**
     * Contains result of a test case. It can either be a value or an error.
     *
     * @param <T> Value type.
     */
    private static class Result<T> {
        final T value;
        final String error;

        Result(T value, String error) {
            if (error != null && value != null) {
                throw new IllegalArgumentException("Both error and value have been specified");
            }
            if (error == null && value == null) {
                throw new IllegalArgumentException("Neither error nor value have been specified");
            }
            this.value = value;
            this.error = error;
        }

        @Override
        public String toString() {
            if (value != null) {
                return "VAL:" + value;
            } else {
                return "ERR:" + error;
            }
        }
    }

    private static class Checker {
        final String query;

        final TestNode node;

        final List<List<Object>> expectedRows = new ArrayList<>();

        Object[] params;

        private Checker(String query, TestNode node) {
            this.query = query;
            this.node = node;
        }

        Checker withParams(Object... params) {
            this.params = params;
            return this;
        }

        Checker returns(Object... row) {
            this.expectedRows.add(Arrays.asList(row));
            return this;
        }

        void expect(Result<?> result) {
            if (result.error != null) {
                fails(result.error);
            } else {
                returns(result.value).ok();
            }
        }

        void ok() {
            QueryPlan plan = node.prepare(query, params);
            AsyncCursor<List<Object>> cursor = node.executePlan(plan, params);

            List<List<Object>> actualRows = collectRows(cursor);

            assertEquals(expectedRows.size(), actualRows.size());

            for (int i = 0; i < expectedRows.size(); i++) {
                List<Object> expectedRow = expectedRows.get(i);
                List<Object> actualRow = actualRows.get(i);

                assertEquals(expectedRow, actualRow, "Row#" + i);
            }
        }

        void fails(String message) {
            List<List<Object>> rows;
            try {
                QueryPlan plan = node.prepare(query, params);
                AsyncCursor<List<Object>> cursor = node.executePlan(plan, params);
                rows = collectRows(cursor);
            } catch (Exception e) {
                assertThat("Error: " + e, e.getMessage(), containsString(message));
                return;
            }

            fail("Expected to fail but returned: " + rows);
        }

        private static List<List<Object>> collectRows(AsyncCursor<List<Object>> cursor) {
            List<List<Object>> actualRows = new ArrayList<>();
            BatchedResult<List<Object>> result;

            do {
                result = cursor.requestNextAsync(10_000).join();
                actualRows.addAll(result.items());
            } while (result.hasMore());

            return actualRows;
        }
    }
}
