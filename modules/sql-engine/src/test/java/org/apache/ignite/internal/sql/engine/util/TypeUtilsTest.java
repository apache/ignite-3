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

import static org.apache.ignite.lang.IgniteStringFormatter.format;
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
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeSpec;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.DynamicTest;
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
public class TypeUtilsTest {

    private static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();

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

        Object internal2 = TypeUtils.toInternal(original);
        assertEquals(internal, internal2, "toInternal w/o type parameter");
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
        //TODO: https://issues.apache.org/jira/browse/IGNITE-17373
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
}
