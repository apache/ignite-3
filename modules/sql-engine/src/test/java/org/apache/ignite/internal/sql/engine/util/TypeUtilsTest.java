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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.exec.row.BaseTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchemaTypes;
import org.apache.ignite.internal.sql.engine.exec.row.RowType;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeSpec;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link TypeUtils}.
 */
@ExtendWith(MockitoExtension.class)
public class TypeUtilsTest extends BaseIgniteAbstractTest {

    private static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();

    private static final BaseTypeSpec BOOLEAN = RowSchemaTypes.nativeType(NativeTypes.BOOLEAN);
    private static final BaseTypeSpec INT8 = RowSchemaTypes.nativeType(NativeTypes.INT8);
    private static final BaseTypeSpec INT16 = RowSchemaTypes.nativeType(NativeTypes.INT16);
    private static final BaseTypeSpec INT32 = RowSchemaTypes.nativeType(NativeTypes.INT32);
    private static final BaseTypeSpec INT64 = RowSchemaTypes.nativeType(NativeTypes.INT64);
    private static final BaseTypeSpec FLOAT = RowSchemaTypes.nativeType(NativeTypes.FLOAT);
    private static final BaseTypeSpec DOUBLE = RowSchemaTypes.nativeType(NativeTypes.DOUBLE);
    private static final BaseTypeSpec STRING = RowSchemaTypes.nativeType(NativeTypes.STRING);
    private static final BaseTypeSpec BYTES = RowSchemaTypes.nativeType(NativeTypes.BYTES);
    private static final BaseTypeSpec UUID = RowSchemaTypes.nativeType(NativeTypes.UUID);

    @Test
    public void testValidateCharactersOverflowAndTrimIfPossible() {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        {
            RelDataType rowType = typeFactory.builder()
                    .add("c1", typeFactory.createSqlType(SqlTypeName.VARCHAR, 3))
                    .add("c2", typeFactory.createSqlType(SqlTypeName.VARCHAR, 6))
                    .build();

            Object[] input = {"123    ", "12345    "};
            Object[] expected = {"123", "12345 "};

            expectOutputRow(rowType, input, expected);
        }

        {
            RelDataType rowType = typeFactory.builder()
                    .add("c1", typeFactory.createSqlType(SqlTypeName.VARCHAR, 4))
                    .build();

            Object[] input = {" 12  "};
            Object[] expected = {" 12 "};

            expectOutputRow(rowType, input, expected);
        }

        {
            RelDataType rowType = typeFactory.builder()
                    .add("c1", typeFactory.createSqlType(SqlTypeName.VARCHAR, 6))
                    .add("c2", typeFactory.createSqlType(SqlTypeName.VARCHAR, 5))
                    .build();

            Object[] input = {null, "12345    "};
            Object[] expected = {null, "12345"};

            expectOutputRow(rowType, input, expected);
        }

        {
            RelDataType rowType = typeFactory.builder()
                    .add("c1", typeFactory.createSqlType(SqlTypeName.VARCHAR, 6))
                    .add("c2", typeFactory.createSqlType(SqlTypeName.VARCHAR, 5))
                    .build();

            Object[] input = {"12345    ", null};
            Object[] expected = {"12345", null};

            expectOutputRow(rowType, input, expected);
        }

        {
            RelDataType rowType = typeFactory.builder()
                    .add("c1", typeFactory.createSqlType(SqlTypeName.VARCHAR, 6))
                    .add("c2", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();

            Object[] input = {"12345    ", null};
            Object[] expected = {"12345 ", null};

            expectOutputRow(rowType, input, expected);
        }

        {
            RelDataType rowType = typeFactory.builder()
                    .add("c1", typeFactory.createSqlType(SqlTypeName.VARCHAR, 6))
                    .add("c2", typeFactory.createSqlType(SqlTypeName.VARCHAR, 7))
                    .add("c3", typeFactory.createSqlType(SqlTypeName.VARCHAR, 3))
                    .build();

            Object[] input = {"12345    ", null, "123 "};
            Object[] expected = {"12345 ", null, "123"};

            expectOutputRow(rowType, input, expected);
        }

        {
            RelDataType rowType = typeFactory.builder()
                    .add("c1", typeFactory.createSqlType(SqlTypeName.VARCHAR, 6))
                    .add("c2", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("c3", typeFactory.createSqlType(SqlTypeName.VARCHAR, 3))
                    .build();

            Object[] input = {"12345    ", null, "123 "};
            Object[] expected = {"12345 ", null, "123"};

            expectOutputRow(rowType, input, expected);
        }


        {
            RelDataType rowType = typeFactory.builder()
                    .add("c1", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();

            Object[] input = {2};
            Object[] expected = {2};

            expectOutputRow(rowType, input, expected);
        }

        {
            RelDataType rowType = typeFactory.builder()
                    .add("c1", typeFactory.createSqlType(SqlTypeName.VARCHAR, 3))
                    .add("c2", typeFactory.createSqlType(SqlTypeName.VARCHAR, 6))
                    .build();

            Object[] input = {"123", "12345 6"};

            assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "", () -> buildTrimmedRow(rowType, input));
        }
    }

    private static void expectOutputRow(RelDataType rowType, Object[] input, Object[] expected) {
        Object[] newRow = buildTrimmedRow(rowType, input);

        assertArrayEquals(expected, newRow, "Unexpected row after validate/trim whitespace");
    }

    private static Object[] buildTrimmedRow(RelDataType rowType, Object[] input) {
        List<RelDataType> columnTypes = rowType.getFieldList().stream().map(RelDataTypeField::getType).collect(Collectors.toList());
        RowSchema rowSchema = TypeUtils.rowSchemaFromRelTypes(columnTypes);

        Object[] newRow = TypeUtils.validateCharactersOverflowAndTrimIfPossible(rowType,
                ArrayRowHandler.INSTANCE,
                input,
                () -> rowSchema
        );
        return newRow;
    }

    /**
     * Checks that conversions to and from internal types is consistent.
     *
     * @see TypeUtils#toInternal(Object, Type) to internal.
     * @see TypeUtils#fromInternal(Object, Type) from internal.
     */
    @ParameterizedTest
    @MethodSource("valueAndType")
    public void testToFromInternalMatch(Object value, Class<?> type) {
        Object internal = TypeUtils.toInternal(value, type);
        assertNotNull(internal, "Conversion to internal has produced null");

        Object original = TypeUtils.fromInternal(internal, type);
        assertEquals(value, original, "toInternal -> fromInternal");
        assertNotNull(original, "Conversion from internal has produced null");
    }

    private static Stream<Arguments> valueAndType() {
        return Stream.of(
                Arguments.of((byte) 1, Byte.class),
                Arguments.of((short) 1, Short.class),
                Arguments.of(1, Integer.class),
                Arguments.of(1L, Long.class),
                Arguments.of(1.0F, Float.class),
                Arguments.of(1.0D, Double.class),
                Arguments.of("hello", String.class),
                Arguments.of(LocalDate.of(1970, 1, 1), LocalDate.class),
                Arguments.of(LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0), LocalDateTime.class),
                Arguments.of(LocalTime.NOON, LocalTime.class),
                Arguments.of(new UUID(1, 1), UUID.class)
        );
    }

    /** Families of the same types are always compatible. */
    @TestFactory
    public Stream<DynamicTest> testSameTypesAreCompatible() {
        return supportedTypes().map(t -> expectCompatible(t, t));
    }

    /**
     * Type nullability is ignored by
     * {@link TypeUtils#typeFamiliesAreCompatible(RelDataTypeFactory, RelDataType, RelDataType)}.
     */
    @TestFactory
    public Stream<DynamicTest> testTypeCompatibilityDoesNotTakeNullabilityIntoAccount() {
        return supportedTypes().flatMap(t ->  {
            RelDataType nullable = TYPE_FACTORY.createTypeWithNullability(t, false);
            RelDataType notNullable = TYPE_FACTORY.createTypeWithNullability(t, false);

            return Stream.of(
                    expectCompatible(notNullable, nullable),
                    expectCompatible(nullable, notNullable)
            );
        });
    }

    /** NULL is compatible with all types. */
    @TestFactory
    public Stream<DynamicTest> testNullCompatibility() {
        RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);

        return supportedTypes().map(t -> expectCompatible(t, nullType));
    }

    /** Type compatibility rules for custom data types. */
    @TestFactory
    public Stream<DynamicTest> testCustomDataTypeCompatibility() {
        IgniteCustomType type1 = new TestCustomType("type1");
        IgniteCustomType type2 = new TestCustomType("type2");
        RelDataType someType = TYPE_FACTORY.createSqlType(SqlTypeName.ANY);

        return Stream.of(
                // types with same custom type name are compatible.
                expectCompatible(type1, new TestCustomType(type1.getCustomTypeName())),

                // different custom types are never compatible.
                expectIncompatible(type1, type2),
                expectIncompatible(type2, type1),

                // custom types are not compatible with other data types.
                expectIncompatible(someType, type1),
                expectIncompatible(type1, someType)
        );
    }

    private static Stream<RelDataType> supportedTypes() {
        List<SqlTypeName> types = new ArrayList<>();

        types.add(SqlTypeName.NULL);
        types.addAll(SqlTypeName.BOOLEAN_TYPES);
        types.addAll(SqlTypeName.NUMERIC_TYPES);
        types.addAll(SqlTypeName.STRING_TYPES);
        types.addAll(SqlTypeName.DATETIME_TYPES);
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
        // types.addAll(SqlTypeName.INTERVAL_TYPES);

        List<RelDataType> relDataTypes = new ArrayList<>();
        types.forEach(typeName -> {
            relDataTypes.add(TYPE_FACTORY.createSqlType(typeName));
        });

        for (String typeName : TYPE_FACTORY.getCustomTypeSpecs().keySet()) {
            relDataTypes.add(TYPE_FACTORY.createCustomType(typeName));
        }

        return relDataTypes.stream();
    }

    /** Types from different type families are not compatible. */
    @TestFactory
    public Stream<DynamicTest> testTypesFromDifferentFamiliesAreNotCompatible() {
        RelDataType type1 = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
        RelDataType type2 = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

        return Stream.of(
                expectIncompatible(type2, type1),
                expectIncompatible(type1, type2)
        );
    }

    private static DynamicTest expectCompatible(RelDataType from, RelDataType target) {
        return DynamicTest.dynamicTest(from + " is compatible with " + target, () -> {
            boolean compatible = TypeUtils.typeFamiliesAreCompatible(TYPE_FACTORY, target, from);

            assertTrue(compatible, format("{} {} should be compatible", from, target));
        });
    }

    private static DynamicTest expectIncompatible(RelDataType from, RelDataType target) {
        return DynamicTest.dynamicTest(from + " is incompatible with " + target, () -> {
            boolean compatible = TypeUtils.typeFamiliesAreCompatible(TYPE_FACTORY, target, from);

            assertFalse(compatible, format("{} {} should not be compatible", from, target));
        });
    }

    /** Conversion to base types + their NULLABLE variants from rel types. */
    @TestFactory
    public Stream<DynamicTest> testSimpleTypesConversion() {
        List<RelToExecTestCase> testCaseList = new ArrayList<>();

        testCaseList.add(new RelToExecTestCase(SqlTypeName.BOOLEAN, BOOLEAN));

        testCaseList.add(new RelToExecTestCase(SqlTypeName.TINYINT, INT8));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.SMALLINT, INT16));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.INTEGER, INT32));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.BIGINT, INT64));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.FLOAT, FLOAT));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.REAL, FLOAT));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.DOUBLE, DOUBLE));

        testCaseList.add(new RelToExecTestCase(SqlTypeName.DECIMAL, RowSchemaTypes.nativeType(NativeTypes.decimalOf(32767, 0))));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.DECIMAL, 10, RowSchemaTypes.nativeType(NativeTypes.decimalOf(10, 0))));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.DECIMAL, 10, 4, RowSchemaTypes.nativeType(NativeTypes.decimalOf(10, 4))));

        testCaseList.add(new RelToExecTestCase(SqlTypeName.CHAR, RowSchemaTypes.nativeType(NativeTypes.stringOf(1))));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.CHAR, 8, RowSchemaTypes.nativeType(NativeTypes.stringOf(8))));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.VARCHAR, STRING));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.VARCHAR, 8, RowSchemaTypes.nativeType(NativeTypes.stringOf(8))));

        testCaseList.add(new RelToExecTestCase(SqlTypeName.BINARY, RowSchemaTypes.nativeType(NativeTypes.blobOf(1))));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.BINARY, 8, RowSchemaTypes.nativeType(NativeTypes.blobOf(8))));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.VARBINARY, BYTES));
        testCaseList.add(new RelToExecTestCase(SqlTypeName.VARBINARY, 8, RowSchemaTypes.nativeType(NativeTypes.blobOf(8))));

        testCaseList.add(new RelToExecTestCase(SqlTypeName.DATE, RowSchemaTypes.nativeType(NativeTypes.DATE)));

        testCaseList.add(new RelToExecTestCase(SqlTypeName.TIME, 4, RowSchemaTypes.nativeType(NativeTypes.time(4))));

        testCaseList.add(new RelToExecTestCase(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, 4, RowSchemaTypes.nativeType(NativeTypes.time(4))));

        testCaseList.add(new RelToExecTestCase(SqlTypeName.TIMESTAMP, 4, RowSchemaTypes.nativeType(NativeTypes.datetime(4))));

        testCaseList.add(new RelToExecTestCase(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 4,
                RowSchemaTypes.nativeType(NativeTypes.timestamp(4))));

        // Year intervals are stored as days (int)
        for (SqlTypeName interval : SqlTypeName.YEAR_INTERVAL_TYPES) {
            SqlIntervalQualifier yearMonth = new SqlIntervalQualifier(interval.getStartUnit(), interval.getEndUnit(), SqlParserPos.ZERO);
            testCaseList.add(new RelToExecTestCase(TYPE_FACTORY.createSqlIntervalType(yearMonth), INT32));
        }

        // Day intervals are stored as nanoseconds (long)
        for (SqlTypeName interval : SqlTypeName.DAY_INTERVAL_TYPES) {
            SqlIntervalQualifier dayTime = new SqlIntervalQualifier(interval.getStartUnit(), interval.getEndUnit(), SqlParserPos.ZERO);
            testCaseList.add(new RelToExecTestCase(TYPE_FACTORY.createSqlIntervalType(dayTime), INT64));
        }

        // IgniteCustomTypes
        testCaseList.add(new RelToExecTestCase(TYPE_FACTORY.createCustomType(UuidType.NAME), UUID));

        // Add test cases for nullable variants
        for (RelToExecTestCase testCase : new ArrayList<>(testCaseList)) {
            RelDataType nullableRelType = TYPE_FACTORY.createTypeWithNullability(testCase.input, true);
            BaseTypeSpec typeSpec = (BaseTypeSpec) testCase.expected;

            testCaseList.add(new RelToExecTestCase(nullableRelType, new BaseTypeSpec(typeSpec.nativeType(), true)));
        }

        return testCaseList.stream().map(RelToExecTestCase::toTest);
    }

    /** NULL type conversion. */
    @TestFactory
    public Stream<DynamicTest> testNullTypeConversion() {
        RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);

        return Stream.of(
                        new RelToExecTestCase(nullType, RowSchemaTypes.NULL),
                        new RelToExecTestCase(TYPE_FACTORY.createTypeWithNullability(nullType, true), RowSchemaTypes.NULL)
                )
                .map(RelToExecTestCase::toTest);
    }

    /** Conversion to row types from struct rel types. */
    @TestFactory
    public Stream<DynamicTest> testRowTypesConversion() {
        List<RelToExecTestCase> testCaseList = new ArrayList<>();

        // basic row type

        RelDataType relType1 = new Builder(TYPE_FACTORY)
                .add("f1", TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN))
                .add("f2", TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), true))
                .build();

        RowType expected1 = new RowType(List.of(BOOLEAN, new BaseTypeSpec(INT32.nativeType(), true)), false);

        RelToExecTestCase simpleRow = new RelToExecTestCase(relType1, expected1);
        testCaseList.add(simpleRow);

        // Row type with nested rows

        RelDataType relType2 = new Builder(TYPE_FACTORY)
                .add("f1", TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN))
                .add("f2", TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), true))
                .add("f3",
                        new Builder(TYPE_FACTORY)
                                .add("f3_f1", TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), true))
                                .add("f3_f2", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
                                .build()
                )
                .build();

        RowType expected2 = new RowType(List.of(
                BOOLEAN,
                new BaseTypeSpec(INT32.nativeType(), true),
                new RowType(List.of(new BaseTypeSpec(INT64.nativeType(), true), STRING), false)),
                false);

        testCaseList.add(new RelToExecTestCase(relType2, expected2));

        return testCaseList.stream().map(RelToExecTestCase::toTest);
    }

    private static final class TestCustomType extends IgniteCustomType {

        private TestCustomType(String typeName) {
            super(new IgniteCustomTypeSpec(typeName,
                    NativeTypes.INT8, ColumnType.INT8, Byte.class,
                    IgniteCustomTypeSpec.getCastFunction(TestCustomType.class, "cast")), false, -1);
        }

        @Override
        protected void generateTypeString(StringBuilder sb, boolean withDetail) {
            sb.append(getCustomTypeName());
        }

        @Override
        public IgniteCustomType createWithNullability(boolean nullable) {
            throw new AssertionError();
        }

        @SuppressWarnings("unused")
        public static byte cast(Object ignore) {
            throw new AssertionError();
        }
    }

    static class RelToExecTestCase {

        final RelDataType input;

        final TypeSpec expected;

        RelToExecTestCase(SqlTypeName input, TypeSpec expected) {
            this.input = TYPE_FACTORY.createSqlType(input);
            this.expected = expected;
        }

        RelToExecTestCase(SqlTypeName input, int precision, TypeSpec expected) {
            this.input = TYPE_FACTORY.createSqlType(input, precision);
            this.expected = expected;
        }

        RelToExecTestCase(SqlTypeName input, int precision, int scale, TypeSpec expected) {
            this.input = TYPE_FACTORY.createSqlType(input, precision, scale);
            this.expected = expected;
        }

        RelToExecTestCase(RelDataType input, TypeSpec expected) {
            this.input = input;
            this.expected = expected;
        }

        DynamicTest toTest() {
            return DynamicTest.dynamicTest((input.isNullable() ? "NULLABLE " : "") + input, () -> {
                RowSchema schema = TypeUtils.rowSchemaFromRelTypes(List.of(input));

                TypeSpec actualType = schema.fields().get(0);
                assertEquals(expected, actualType, input.getFullTypeString());
            });
        }
    }
}
