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

package org.apache.ignite.internal.sql.engine.exec.row;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** Tests for {@link RowSchemaTypes}. */
public class RowSchemaTypesTest {

    /** NULL type. */
    @Test
    public void testNullType() {
        assertEquals(new NullTypeSpec(), RowSchemaTypes.NULL);
        assertTrue(RowSchemaTypes.NULL.isNullable(), "NULL should be nullable");
    }

    /** {@link RowType} equality. */
    @Test
    public void testRowType() {
        RowType rowType1 = new RowType(List.of(RowSchemaTypes.nativeType(NativeTypes.INT32)), false);
        RowType rowType2 = new RowType(List.of(RowSchemaTypes.nativeType(NativeTypes.INT32)), false);

        RowType rowType3 = new RowType(List.of(
                RowSchemaTypes.nativeType(NativeTypes.INT32),
                RowSchemaTypes.nativeType(NativeTypes.INT32)),
                false);

        assertEquals(rowType1, rowType2);
        assertNotEquals(rowType1, rowType3);
    }

    /** Creating {@link RowType} with no fields should not be possible. */
    @Test
    public void testEmptyRowTypeIsIllegal() {
        assertThrows(IllegalArgumentException.class, () -> new RowType(List.of(), false));
    }

    /** Conversion from {@link NativeType} to {@link BaseTypeSpec}. */
    @TestFactory
    public Stream<DynamicTest> testFromNativeType() {
        List<NativeTypeToBaseTestCase> testCaseList = new ArrayList<>();

        for (ConversionTest testCase : ConversionTest.values()) {
            NativeType nativeType = testCase.nativeType;
            Supplier<BaseTypeSpec> fromNotNull = () -> RowSchemaTypes.nativeType(nativeType);

            if (testCase.interned) {
                Pair<BaseTypeSpec, BaseTypeSpec> typePair = RowSchemaTypes.INTERNED_TYPES.get(nativeType);
                testCaseList.add(new NativeTypeToBaseTestCase(nativeType, true, typePair.getFirst(), false, fromNotNull));

            } else {
                BaseTypeSpec notNull = new BaseTypeSpec(nativeType, false);
                testCaseList.add(new NativeTypeToBaseTestCase(nativeType, false, notNull, false, fromNotNull));
            }
        }

        return testCaseList.stream().map(NativeTypeToBaseTestCase::toTest);
    }

    /** Conversion from {@link NativeType} to {@link BaseTypeSpec}. */
    @TestFactory
    public Stream<DynamicTest> testFromNativeTypeWithNullability() {
        List<NativeTypeToBaseTestCase> testCaseList = new ArrayList<>();

        for (ConversionTest testCase : ConversionTest.values()) {
            NativeType nativeType = testCase.nativeType;
            Supplier<BaseTypeSpec> fromNotNull = () -> RowSchemaTypes.nativeTypeWithNullability(nativeType, false);
            Supplier<BaseTypeSpec> fromNullable = () -> RowSchemaTypes.nativeTypeWithNullability(nativeType, true);

            if (testCase.interned) {
                Pair<BaseTypeSpec, BaseTypeSpec> typePair = RowSchemaTypes.INTERNED_TYPES.get(nativeType);

                testCaseList.add(new NativeTypeToBaseTestCase(nativeType, true, typePair.getFirst(), false, fromNotNull));
                testCaseList.add(new NativeTypeToBaseTestCase(nativeType, true, typePair.getSecond(), true, fromNullable));
            } else {
                BaseTypeSpec notNull = new BaseTypeSpec(nativeType, false);
                BaseTypeSpec nullable = new BaseTypeSpec(nativeType, true);

                testCaseList.add(new NativeTypeToBaseTestCase(nativeType, false, notNull, false, fromNotNull));
                testCaseList.add(new NativeTypeToBaseTestCase(nativeType, false, nullable, true, fromNullable));
            }
        }

        // Check for missed types add fail if such types present.

        Set<Entry<NativeTypeSpec, Boolean>> nativeTypeSpecs = new CopyOnWriteArraySet<>();
        for (NativeTypeSpec spec : NativeTypeSpec.values()) {
            nativeTypeSpecs.add(Map.entry(spec, true));
            nativeTypeSpecs.add(Map.entry(spec, false));
        }

        DynamicTest dynamicTest = DynamicTest.dynamicTest("Ensure all native types were checked", () -> {
            for (NativeTypeToBaseTestCase test : testCaseList) {
                nativeTypeSpecs.remove(Map.entry(test.nativeType.spec(), test.nullable));
            }
            List<String> typePairs = nativeTypeSpecs.stream()
                    .map(p -> format("<{}, nullable={}>", p.getKey().name(), p.getValue()))
                    .collect(Collectors.toList());

            assertTrue(nativeTypeSpecs.isEmpty(), typePairs.toString());
        });

        return Streams.concat(testCaseList.stream().map(NativeTypeToBaseTestCase::toTest), Stream.of(dynamicTest));
    }

    enum ConversionTest {
        // type, interned or not.
        BOOLEAN(NativeTypes.BOOLEAN, true),

        INT8(NativeTypes.INT8, true),
        INT16(NativeTypes.INT16, true),
        INT32(NativeTypes.INT32, true),
        INT64(NativeTypes.INT64, true),
        FLOAT(NativeTypes.FLOAT, true),
        DOUBLE(NativeTypes.DOUBLE, true),
        DECIMAL_10_0(NativeTypes.decimalOf(10, 0), false),
        DECIMAL_10_4(NativeTypes.decimalOf(10, 4), false),
        NUMBER_10(NativeTypes.numberOf(10), false),

        STRING(NativeTypes.INT16, true),
        STRING_8(NativeTypes.stringOf(8), false),
        BYTES(NativeTypes.BYTES, true),
        BYTES_8(NativeTypes.blobOf(8), false),

        UUID(NativeTypes.UUID, true),

        DATE(NativeTypes.DATE, true),
        TIME_2(NativeTypes.time(2), false),
        DATETIME_2(NativeTypes.datetime(2), false),
        TIMESTAMP_2(NativeTypes.timestamp(2), false),
        BITMASK_8(NativeTypes.bitmaskOf(8), false),
        ;

        final NativeType nativeType;
        final boolean interned;

        ConversionTest(NativeType nativeType, boolean interned) {
            this.nativeType = nativeType;
            this.interned = interned;
        }
    }

    private static final class NativeTypeToBaseTestCase {

        final NativeType nativeType;

        final boolean internedType;

        final BaseTypeSpec expected;

        final boolean nullable;

        final Supplier<BaseTypeSpec> result;

        NativeTypeToBaseTestCase(NativeType nativeType, boolean sameType,
                BaseTypeSpec expected, boolean nullable, Supplier<BaseTypeSpec> result) {
            this.internedType = sameType;
            this.nativeType = nativeType;
            this.nullable = nullable;
            this.expected = expected;
            this.result = result;
        }

        DynamicTest toTest() {
            String name = format("{} {} {}", (nullable ? "NULLABLE " : ""), nativeType, (internedType ? "SAME" : "EQUAL"));

            return DynamicTest.dynamicTest(name, () -> {
                BaseTypeSpec actual = result.get();

                if (internedType) {
                    assertSame(expected, actual, "descriptors should point to the same object");
                } else {
                    assertEquals(expected, actual, "descriptors does not match");
                }

                assertEquals(nullable, actual.isNullable(), "Nullability does not match: " + actual);
            });
        }
    }
}
