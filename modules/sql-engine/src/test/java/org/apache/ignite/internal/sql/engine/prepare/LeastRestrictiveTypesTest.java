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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests the behaviour of {@link IgniteTypeFactory#leastRestrictive(List)}.
 */
public class LeastRestrictiveTypesTest {

    private static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();

    private static final RelDataType TINYINT = TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT);

    private static final RelDataType SMALLINT = TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT);

    private static final RelDataType INTEGER = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    private static final RelDataType FLOAT = TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT);

    private static final RelDataType DOUBLE = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);

    private static final RelDataType REAL = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);

    private static final RelDataType BIGINT = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);

    private static final RelDataType DECIMAL = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 1000, 10);

    private static final RelDataType VARCHAR = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 36);

    // ANY produced by the default implementation of leastRestrictiveType has nullability = true
    private static final RelDataType ANY = TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.ANY), true);

    @ParameterizedTest
    @MethodSource("tinyIntTests")
    public void testTinyInt(RelDataType t1, RelDataType t2, LeastRestrictiveType leastRestrictiveType) {
        expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
        expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
    }

    private static Stream<Arguments> tinyIntTests() {
        List<Arguments> tests = new ArrayList<>();

        tests.add(Arguments.arguments(TINYINT, TINYINT, new LeastRestrictiveType(TINYINT)));
        tests.add(Arguments.arguments(TINYINT, SMALLINT, new LeastRestrictiveType(SMALLINT)));
        tests.add(Arguments.arguments(TINYINT, INTEGER, new LeastRestrictiveType(INTEGER)));
        tests.add(Arguments.arguments(TINYINT, FLOAT, new LeastRestrictiveType(FLOAT)));
        tests.add(Arguments.arguments(TINYINT, REAL, new LeastRestrictiveType(REAL)));
        tests.add(Arguments.arguments(TINYINT, DOUBLE, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(TINYINT, DECIMAL, new LeastRestrictiveType(DECIMAL)));
        tests.add(Arguments.arguments(TINYINT, BIGINT, new LeastRestrictiveType(BIGINT)));

        return tests.stream();
    }

    @ParameterizedTest
    @MethodSource("smallIntTests")
    public void testSmallInt(RelDataType t1, RelDataType t2, LeastRestrictiveType leastRestrictiveType) {
        expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
        expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
    }

    private static Stream<Arguments> smallIntTests() {
        List<Arguments> tests = new ArrayList<>();

        tests.add(Arguments.arguments(SMALLINT, TINYINT, new LeastRestrictiveType(SMALLINT)));
        tests.add(Arguments.arguments(SMALLINT, SMALLINT, new LeastRestrictiveType(SMALLINT)));
        tests.add(Arguments.arguments(SMALLINT, INTEGER, new LeastRestrictiveType(INTEGER)));
        tests.add(Arguments.arguments(SMALLINT, FLOAT, new LeastRestrictiveType(FLOAT)));
        tests.add(Arguments.arguments(SMALLINT, REAL, new LeastRestrictiveType(REAL)));
        tests.add(Arguments.arguments(SMALLINT, DOUBLE, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(SMALLINT, DECIMAL, new LeastRestrictiveType(DECIMAL)));
        tests.add(Arguments.arguments(SMALLINT, BIGINT, new LeastRestrictiveType(BIGINT)));

        return tests.stream();
    }

    @ParameterizedTest
    @MethodSource("intTests")
    public void testInteger(RelDataType t1, RelDataType t2, LeastRestrictiveType leastRestrictiveType) {
        expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
        expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
    }

    private static Stream<Arguments> intTests() {
        List<Arguments> tests = new ArrayList<>();

        tests.add(Arguments.arguments(INTEGER, TINYINT, new LeastRestrictiveType(INTEGER)));
        tests.add(Arguments.arguments(INTEGER, SMALLINT, new LeastRestrictiveType(INTEGER)));
        tests.add(Arguments.arguments(INTEGER, INTEGER, new LeastRestrictiveType(INTEGER)));
        tests.add(Arguments.arguments(INTEGER, FLOAT, new LeastRestrictiveType(FLOAT)));
        tests.add(Arguments.arguments(INTEGER, REAL, new LeastRestrictiveType(REAL)));
        tests.add(Arguments.arguments(INTEGER, DOUBLE, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(INTEGER, DECIMAL, new LeastRestrictiveType(DECIMAL)));
        tests.add(Arguments.arguments(INTEGER, BIGINT, new LeastRestrictiveType(BIGINT)));

        return tests.stream();
    }

    @ParameterizedTest
    @MethodSource("floatTests")
    public void testFloat(RelDataType t1, RelDataType t2, LeastRestrictiveType leastRestrictiveType) {
        expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
        expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
    }

    private static Stream<Arguments> floatTests() {
        List<Arguments> tests = new ArrayList<>();

        tests.add(Arguments.arguments(FLOAT, TINYINT, new LeastRestrictiveType(FLOAT)));
        tests.add(Arguments.arguments(FLOAT, SMALLINT, new LeastRestrictiveType(FLOAT)));
        tests.add(Arguments.arguments(FLOAT, INTEGER, new LeastRestrictiveType(FLOAT)));
        tests.add(Arguments.arguments(FLOAT, FLOAT, new LeastRestrictiveType(FLOAT)));
        tests.add(Arguments.arguments(FLOAT, REAL, new LeastRestrictiveType(FLOAT)));
        tests.add(Arguments.arguments(FLOAT, DOUBLE, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(FLOAT, DECIMAL, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(FLOAT, BIGINT, new LeastRestrictiveType(FLOAT)));

        return tests.stream();
    }

    @ParameterizedTest
    @MethodSource("doubleTests")
    public void testDouble(RelDataType t1, RelDataType t2, LeastRestrictiveType leastRestrictiveType) {
        expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
        expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
    }

    private static Stream<Arguments> doubleTests() {
        List<Arguments> tests = new ArrayList<>();

        tests.add(Arguments.arguments(DOUBLE, TINYINT, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DOUBLE, SMALLINT, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DOUBLE, INTEGER, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DOUBLE, FLOAT, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DOUBLE, REAL, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DOUBLE, DOUBLE, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DOUBLE, DECIMAL, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DOUBLE, BIGINT, new LeastRestrictiveType(DOUBLE)));

        return tests.stream();
    }

    @ParameterizedTest
    @MethodSource("decimalTests")
    public void testDecimal(RelDataType t1, RelDataType t2, LeastRestrictiveType leastRestrictiveType) {
        expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
        expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
    }

    private static Stream<Arguments> decimalTests() {
        List<Arguments> tests = new ArrayList<>();

        tests.add(Arguments.arguments(DECIMAL, TINYINT, new LeastRestrictiveType(DECIMAL)));
        tests.add(Arguments.arguments(DECIMAL, SMALLINT, new LeastRestrictiveType(DECIMAL)));
        tests.add(Arguments.arguments(DECIMAL, INTEGER, new LeastRestrictiveType(DECIMAL)));
        tests.add(Arguments.arguments(DECIMAL, FLOAT, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DECIMAL, REAL, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DECIMAL, DOUBLE, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(DECIMAL, DECIMAL, new LeastRestrictiveType(DECIMAL)));
        tests.add(Arguments.arguments(DECIMAL, BIGINT, new LeastRestrictiveType(DECIMAL)));

        return tests.stream();
    }

    @ParameterizedTest
    @MethodSource("bigIntTests")
    public void testBigInt(RelDataType t1, RelDataType t2, LeastRestrictiveType leastRestrictiveType) {
        expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
        expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
    }

    private static Stream<Arguments> bigIntTests() {
        List<Arguments> tests = new ArrayList<>();

        tests.add(Arguments.arguments(BIGINT, TINYINT, new LeastRestrictiveType(BIGINT)));
        tests.add(Arguments.arguments(BIGINT, SMALLINT, new LeastRestrictiveType(BIGINT)));
        tests.add(Arguments.arguments(BIGINT, INTEGER, new LeastRestrictiveType(BIGINT)));
        tests.add(Arguments.arguments(BIGINT, FLOAT, new LeastRestrictiveType(FLOAT)));
        tests.add(Arguments.arguments(BIGINT, REAL, new LeastRestrictiveType(REAL)));
        tests.add(Arguments.arguments(BIGINT, DOUBLE, new LeastRestrictiveType(DOUBLE)));
        tests.add(Arguments.arguments(BIGINT, DECIMAL, new LeastRestrictiveType(DECIMAL)));
        tests.add(Arguments.arguments(BIGINT, BIGINT, new LeastRestrictiveType(BIGINT)));

        return tests.stream();
    }

    @ParameterizedTest
    @MethodSource("anyTests")
    public void testAny(RelDataType t1, RelDataType t2, LeastRestrictiveType leastRestrictiveType) {
        expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
        expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
    }

    private static Stream<Arguments> anyTests() {
        List<Arguments> tests = new ArrayList<>();
        LeastRestrictiveType anyType = new LeastRestrictiveType(ANY);

        tests.add(Arguments.arguments(ANY, TINYINT, anyType));
        tests.add(Arguments.arguments(ANY, SMALLINT, anyType));
        tests.add(Arguments.arguments(ANY, INTEGER, anyType));
        tests.add(Arguments.arguments(ANY, FLOAT, anyType));
        tests.add(Arguments.arguments(ANY, REAL, anyType));
        tests.add(Arguments.arguments(ANY, DOUBLE, anyType));
        tests.add(Arguments.arguments(ANY, DECIMAL, anyType));
        tests.add(Arguments.arguments(ANY, BIGINT, anyType));
        tests.add(Arguments.arguments(ANY, VARCHAR, anyType));

        return tests.stream();
    }

    @ParameterizedTest
    @MethodSource("types")
    public void testLeastRestrictiveTypeForAnyAndMoreThanTwoTypes(RelDataType type) {
        // Behaves the same as two argument version.
        // Compatibility with default implementation.
        assertEquals(ANY, TYPE_FACTORY.leastRestrictive(List.of(type, type, ANY)));
        assertEquals(ANY, TYPE_FACTORY.leastRestrictive(List.of(type, ANY, type)));
        assertEquals(ANY, TYPE_FACTORY.leastRestrictive(List.of(ANY, type, type)));
    }

    private static Stream<Arguments> types() {
        List<Arguments> tests = new ArrayList<>();

        tests.add(Arguments.arguments(TINYINT));
        tests.add(Arguments.arguments(SMALLINT));
        tests.add(Arguments.arguments(INTEGER));
        tests.add(Arguments.arguments(FLOAT));
        tests.add(Arguments.arguments(REAL));
        tests.add(Arguments.arguments(DOUBLE));
        tests.add(Arguments.arguments(DECIMAL));
        tests.add(Arguments.arguments(BIGINT));
        tests.add(Arguments.arguments(VARCHAR));

        return tests.stream();
    }

    private static final class LeastRestrictiveType {
        final RelDataType relDataType;

        private LeastRestrictiveType(@Nullable RelDataType relDataType) {
            this.relDataType = relDataType;
        }

        private static LeastRestrictiveType none() {
            return new LeastRestrictiveType(null);
        }

        @Override
        public String toString() {
            return relDataType != null ? relDataType.toString() : "<none>";
        }
    }

    private static void expectLeastRestrictiveType(RelDataType type1, RelDataType type2, LeastRestrictiveType expectedType) {
        RelDataType actualType = TYPE_FACTORY.leastRestrictive(Arrays.asList(type1, type2));
        assertEquals(expectedType.relDataType, actualType, "leastRestrictive(" + type1 + "," + type2 + ")");
    }
}
