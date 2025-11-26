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

package org.apache.ignite.internal.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Base test class for all potential implementation of comparators for binary tuples. */
public abstract class BinaryTupleComparatorBaseTest {
    private static List<Arguments> singleColumnTuples() {
        return List.of(
                Arguments.of(NativeTypes.BOOLEAN,
                        new BinaryTupleBuilder(1).appendBoolean(false).build(),
                        new BinaryTupleBuilder(1).appendBoolean(true).build()),
                Arguments.of(NativeTypes.INT8,
                        new BinaryTupleBuilder(1).appendByte((byte) -1).build(),
                        new BinaryTupleBuilder(1).appendByte(Byte.MAX_VALUE).build()),
                Arguments.of(NativeTypes.INT16,
                        new BinaryTupleBuilder(1).appendShort((short) -1).build(),
                        new BinaryTupleBuilder(1).appendShort(Short.MAX_VALUE).build()),
                Arguments.of(NativeTypes.INT32,
                        new BinaryTupleBuilder(1).appendInt(-1).build(),
                        new BinaryTupleBuilder(1).appendInt(Integer.MAX_VALUE).build()),
                Arguments.of(NativeTypes.INT64,
                        new BinaryTupleBuilder(1).appendLong(-1).build(),
                        new BinaryTupleBuilder(1).appendLong(Long.MAX_VALUE).build()),
                Arguments.of(NativeTypes.FLOAT,
                        new BinaryTupleBuilder(1).appendFloat(-1.69f).build(),
                        new BinaryTupleBuilder(1).appendFloat(Float.MAX_VALUE).build()),
                Arguments.of(NativeTypes.DOUBLE,
                        new BinaryTupleBuilder(1).appendDouble(-1.69).build(),
                        new BinaryTupleBuilder(1).appendDouble(Double.MAX_VALUE).build()),

                Arguments.of(NativeTypes.BYTES,
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, 2, 3}).build(),
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, 5, 1}).build()),
                Arguments.of(NativeTypes.BYTES,
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, 2, 3}).build(),
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, 2, 3, 4}).build()),
                Arguments.of(NativeTypes.BYTES,
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, 2, 3, 4}).build(),
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, 2, 4}).build()),
                Arguments.of(NativeTypes.BYTES,
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{}).build(),
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1}).build()),
                Arguments.of(NativeTypes.BYTES,
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, 2, 127}).build(),
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, 2, (byte) 128}).build()),
                Arguments.of(NativeTypes.BYTES,
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, (byte) 200, 2}).build(),
                        new BinaryTupleBuilder(1).appendBytes(new byte[]{1, (byte) 255, 1}).build()),

                Arguments.of(NativeTypes.decimalOf(20, 3),
                        new BinaryTupleBuilder(1).appendDecimal(BigDecimal.valueOf(-1), 0).build(),
                        new BinaryTupleBuilder(1).appendDecimal(BigDecimal.valueOf(123456789.12), 2).build()),

                Arguments.of(NativeTypes.UUID,
                        new BinaryTupleBuilder(1).appendUuid(UUID.fromString("00000000-0000-0000-0000-000000000001")).build(),
                        new BinaryTupleBuilder(1).appendUuid(UUID.fromString("00000000-0000-0000-0000-000000000002")).build()),
                Arguments.of(NativeTypes.UUID,
                        new BinaryTupleBuilder(1).appendUuid(UUID.fromString("00000000-0000-0000-0000-000000000001")).build(),
                        new BinaryTupleBuilder(1).appendUuid(UUID.fromString("0FFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")).build()),
                Arguments.of(NativeTypes.UUID,
                        new BinaryTupleBuilder(1).appendUuid(UUID.fromString("F0000000-0000-0000-0000-000000000000")).build(),
                        new BinaryTupleBuilder(1).appendUuid(UUID.fromString("00000000-0000-0000-0000-000000000001")).build()),

                Arguments.of(NativeTypes.STRING,
                        new BinaryTupleBuilder(1).appendString("foobar").build(),
                        new BinaryTupleBuilder(1).appendString("foobaz").build()),
                Arguments.of(NativeTypes.STRING,
                        new BinaryTupleBuilder(1).appendString("foo").build(),
                        new BinaryTupleBuilder(1).appendString("foobar").build()),
                Arguments.of(NativeTypes.STRING,
                        new BinaryTupleBuilder(1).appendString("barbaz").build(),
                        new BinaryTupleBuilder(1).appendString("foo").build()),

                Arguments.of(NativeTypes.timestamp(6),
                        new BinaryTupleBuilder(1).appendTimestamp(Instant.ofEpochMilli(1)).build(),
                        new BinaryTupleBuilder(1).appendTimestamp(Instant.ofEpochMilli(2)).build()),

                Arguments.of(NativeTypes.DATE,
                        new BinaryTupleBuilder(1).appendDate(LocalDate.of(2021, 1, 1)).build(),
                        new BinaryTupleBuilder(1).appendDate(LocalDate.of(2021, 1, 2)).build()),
                Arguments.of(NativeTypes.DATE,
                        new BinaryTupleBuilder(1).appendDate(LocalDate.of(2020, 12, 31)).build(),
                        new BinaryTupleBuilder(1).appendDate(LocalDate.of(2021, 1, 1)).build()),

                Arguments.of(NativeTypes.time(6),
                        new BinaryTupleBuilder(1).appendTime(LocalTime.of(1, 2, 3)).build(),
                        new BinaryTupleBuilder(1).appendTime(LocalTime.of(3, 2, 1)).build()),
                Arguments.of(NativeTypes.time(6),
                        new BinaryTupleBuilder(1).appendTime(LocalTime.of(11, 12, 13)).build(),
                        new BinaryTupleBuilder(1).appendTime(LocalTime.of(13, 12, 11)).build()),

                Arguments.of(NativeTypes.datetime(6),
                        new BinaryTupleBuilder(1).appendDateTime(LocalDateTime.of(2021, 1, 1, 1, 2, 3)).build(),
                        new BinaryTupleBuilder(1).appendDateTime(LocalDateTime.of(2021, 1, 1, 4, 5, 6)).build()),
                Arguments.of(NativeTypes.datetime(6),
                        new BinaryTupleBuilder(1).appendDateTime(LocalDateTime.of(2020, 12, 31, 4, 5, 6)).build(),
                        new BinaryTupleBuilder(1).appendDateTime(LocalDateTime.of(2021, 1, 1, 1, 2, 3)).build())
        );
    }

    protected static void validate(Comparator<ByteBuffer> comparator, ByteBuffer lesserOne, ByteBuffer greaterOne) {
        assertThat(comparator.compare(lesserOne, greaterOne), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(lesserOne, lesserOne), is(0));
        assertThat(comparator.compare(greaterOne, greaterOne), is(0));
        assertThat(comparator.compare(greaterOne, lesserOne), is(greaterThanOrEqualTo(1)));
    }

    protected static void setEqualityFlag(ByteBuffer buffer) {
        byte flags = buffer.get(0);

        buffer.put(0, (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG));
    }

    @ParameterizedTest
    @MethodSource("singleColumnTuples")
    public void compareSingleColumnTuples(NativeType type, ByteBuffer lesserOne, ByteBuffer greaterOne) {
        var comparator = createSingleColumnComparator(type, CatalogColumnCollation.ASC_NULLS_LAST);
        var reversedComparator = createSingleColumnComparator(type, CatalogColumnCollation.DESC_NULLS_FIRST);

        validate(comparator, lesserOne, greaterOne);
        validate(reversedComparator, greaterOne, lesserOne);
    }

    @Test
    public void compareSingleColumnTuplesWithNulls() {
        var ascNullFirstComparator = createSingleColumnComparator(NativeTypes.INT32, CatalogColumnCollation.ASC_NULLS_FIRST);
        var ascNullLastComparator = createSingleColumnComparator(NativeTypes.INT32, CatalogColumnCollation.ASC_NULLS_LAST);
        var descNullFirstComparator = createSingleColumnComparator(NativeTypes.INT32, CatalogColumnCollation.DESC_NULLS_FIRST);
        var descNullLastComparator = createSingleColumnComparator(NativeTypes.INT32, CatalogColumnCollation.DESC_NULLS_LAST);

        ByteBuffer tuple1 = new BinaryTupleBuilder(1)
                .appendNull()
                .build();
        ByteBuffer tuple2 = new BinaryTupleBuilder(1)
                .appendInt(0)
                .build();

        validate(ascNullFirstComparator, tuple1, tuple2);
        validate(descNullFirstComparator, tuple1, tuple2);
        validate(ascNullLastComparator, tuple2, tuple1);
        validate(descNullLastComparator, tuple2, tuple1);
    }

    @Test
    public void testCompareMultipleColumnTuples() {
        var comparator = newComparator(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST, CatalogColumnCollation.DESC_NULLS_FIRST),
                List.of(NativeTypes.INT32, NativeTypes.STRING)
        );

        ByteBuffer tuple1 = new BinaryTupleBuilder(2)
                .appendInt(0)
                .appendString("foobar")
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(2)
                .appendInt(1)
                .appendString("foobar")
                .build();

        ByteBuffer tuple3 = new BinaryTupleBuilder(2)
                .appendInt(0)
                .appendString("foobaa")
                .build();

        validate(comparator, tuple1, tuple2);
        validate(comparator, tuple1, tuple3);
        validate(comparator, tuple3, tuple2);

        var reversedComparator = newComparator(
                List.of(CatalogColumnCollation.DESC_NULLS_FIRST, CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.INT32, NativeTypes.STRING)
        );

        validate(reversedComparator, tuple2, tuple1);
        validate(reversedComparator, tuple3, tuple1);
        validate(reversedComparator, tuple2, tuple3);
    }

    @Test
    public void testCompareMultipleColumnTuplesWithNulls() {
        var comparator = newComparator(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST, CatalogColumnCollation.DESC_NULLS_FIRST),
                List.of(NativeTypes.INT32, NativeTypes.STRING)
        );

        ByteBuffer tuple1 = new BinaryTupleBuilder(2)
                .appendInt(null)
                .appendString("foobar")
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(2)
                .appendInt(1)
                .appendString("foobar")
                .build();

        ByteBuffer tuple3 = new BinaryTupleBuilder(2)
                .appendInt(null)
                .appendString("foobaa")
                .build();

        ByteBuffer tuple4 = new BinaryTupleBuilder(2)
                .appendInt(null)
                .appendString(null)
                .build();

        validate(comparator, tuple2, tuple1);
        validate(comparator, tuple1, tuple3);
        validate(comparator, tuple4, tuple1);
    }

    @Test
    public void testCompareWithPrefix() {
        var comparator = newComparator(
                // Second collation is not used in test.
                List.of(CatalogColumnCollation.ASC_NULLS_LAST, CatalogColumnCollation.ASC_NULLS_FIRST),
                List.of(NativeTypes.INT32, NativeTypes.STRING)
        );

        ByteBuffer tuple = new BinaryTupleBuilder(2)
                .appendInt(1)
                .appendString("foobar")
                .build();

        ByteBuffer prefix1 = new BinaryTuplePrefixBuilder(1, 2)
                .appendInt(2)
                .build();

        ByteBuffer prefix2 = new BinaryTuplePrefixBuilder(1, 2)
                .appendInt(0)
                .build();

        ByteBuffer prefix3 = new BinaryTuplePrefixBuilder(1, 2)
                .appendInt(1)
                .build();

        assertThat(comparator.compare(tuple, prefix1), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(prefix1, tuple), is(greaterThanOrEqualTo(1)));

        assertThat(comparator.compare(prefix2, tuple), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple, prefix2), is(greaterThanOrEqualTo(1)));

        assertThat(comparator.compare(prefix3, tuple), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple, prefix3), is(greaterThanOrEqualTo(1)));

        setEqualityFlag(prefix3);

        assertThat(comparator.compare(prefix3, tuple), is(lessThanOrEqualTo(1)));
        assertThat(comparator.compare(tuple, prefix3), is(greaterThanOrEqualTo(-1)));
    }

    @Test
    public void testCompareWithPrefixWithNulls() {
        var comparator = newComparator(
                // Second collation is not used in test.
                List.of(CatalogColumnCollation.ASC_NULLS_LAST, CatalogColumnCollation.ASC_NULLS_FIRST),
                List.of(NativeTypes.INT32, NativeTypes.STRING)
        );

        ByteBuffer tuple1 = new BinaryTupleBuilder(2)
                .appendInt(null)
                .appendString("foobar")
                .build();

        ByteBuffer tuple2 = new BinaryTuplePrefixBuilder(1, 2)
                .appendInt(0)
                .build();

        assertThat(comparator.compare(tuple1, tuple2), is(greaterThanOrEqualTo(1)));
        assertThat(comparator.compare(tuple2, tuple1), is(lessThanOrEqualTo(-1)));

        tuple2 = new BinaryTuplePrefixBuilder(1, 2)
                .appendInt(null)
                .build();

        assertThat(comparator.compare(tuple2, tuple1), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple1, tuple2), is(greaterThanOrEqualTo(1)));

        setEqualityFlag(tuple2);

        assertThat(comparator.compare(tuple2, tuple1), is(lessThanOrEqualTo(1)));
        assertThat(comparator.compare(tuple1, tuple2), is(greaterThanOrEqualTo(-1)));
    }

    @Test
    public void partialComparatorAsciiTest() {
        assumeTrue(supportsPartialComparison());

        Comparator<ByteBuffer> comparator = newComparator(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.STRING)
        );

        ByteBuffer tupleReference = new BinaryTupleBuilder(1)
                .appendString("qwertyuiop".repeat(10))
                .build();

        ByteBuffer tupleWithEmptyStringReference = new BinaryTupleBuilder(1)
                .appendString("")
                .build();

        ByteBuffer tuple1 = new BinaryTupleBuilder(1)
                .appendString("qwertyuiop")
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(1)
                .appendString("rxfsuzvjpq".repeat(2))
                .build();

        ByteBuffer tuple3 = new BinaryTupleBuilder(1)
                .appendString("qwertyuiop".repeat(15))
                .build();

        assertTrue(comparator.compare(tuple1, tupleReference) < 0);
        assertTrue(comparator.compare(tuple2, tupleReference) > 0);
        assertTrue(comparator.compare(tuple3, tupleReference) > 0);
        assertEquals(0, comparator.compare(tuple1.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(comparator.compare(tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertTrue(comparator.compare(
                tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN),
                tupleWithEmptyStringReference
        ) > 0);
        assertEquals(0, comparator.compare(tuple3.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(comparator.compare(tuple3.limit(120).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertTrue(comparator.compare(
                tuple3.limit(120).slice().order(ByteOrder.LITTLE_ENDIAN),
                tupleWithEmptyStringReference
        ) > 0);
    }

    @Test
    public void partialComparatorUnicodeTest() {
        assumeTrue(supportsPartialComparison());

        Comparator<ByteBuffer> comparator = newComparator(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.STRING)
        );

        ByteBuffer tupleReference = new BinaryTupleBuilder(1)
                .appendString("йцукенгшщз".repeat(10))
                .build();

        ByteBuffer tuple1 = new BinaryTupleBuilder(1)
                .appendString("йцукенгшщз")
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(1)
                .appendString("кчфлёодщъи".repeat(2))
                .build();

        ByteBuffer tuple3 = new BinaryTupleBuilder(1)
                .appendString("йцукенгшщз".repeat(15))
                .build();

        assertTrue(comparator.compare(tuple1, tupleReference) < 0);
        assertTrue(comparator.compare(tuple2, tupleReference) > 0);
        assertTrue(comparator.compare(tuple3, tupleReference) > 0);
        assertEquals(0, comparator.compare(tuple1.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(comparator.compare(tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertEquals(0, comparator.compare(tuple3.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(comparator.compare(tuple3.limit(220).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
    }

    @Test
    public void partialComparatorBytesTest() {
        assumeTrue(supportsPartialComparison());

        Comparator<ByteBuffer> comparator = newComparator(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.BYTES)
        );

        byte[] bytes = new byte[150];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i % 10);
        }

        ByteBuffer tupleReference = new BinaryTupleBuilder(1)
                .appendBytes(Arrays.copyOfRange(bytes, 0, 100))
                .build();

        ByteBuffer tupleWithEmptyArrayReference = new BinaryTupleBuilder(1)
                .appendBytes(new byte[0])
                .build();

        ByteBuffer tuple1 = new BinaryTupleBuilder(1)
                .appendBytes(Arrays.copyOfRange(bytes, 0, 10))
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(1)
                .appendBytes(Arrays.copyOfRange(bytes, 1, 21))
                .build();

        ByteBuffer tuple3 = new BinaryTupleBuilder(1)
                .appendBytes(bytes)
                .build();

        assertTrue(comparator.compare(tuple1, tupleReference) < 0);
        assertTrue(comparator.compare(tuple2, tupleReference) > 0);
        assertTrue(comparator.compare(tuple3, tupleReference) > 0);
        assertEquals(0, comparator.compare(tuple1.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(comparator.compare(tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertTrue(comparator.compare(
                tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN),
                tupleWithEmptyArrayReference
        ) > 0);
        assertEquals(0, comparator.compare(tuple3.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(comparator.compare(tuple3.limit(120).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertTrue(comparator.compare(
                tuple3.limit(120).slice().order(ByteOrder.LITTLE_ENDIAN),
                tupleWithEmptyArrayReference
        ) > 0);
    }

    protected final Comparator<ByteBuffer> createSingleColumnComparator(NativeType type, CatalogColumnCollation collation) {
        return newComparator(List.of(collation), List.of(type));
    }

    protected abstract Comparator<ByteBuffer> newComparator(
            List<CatalogColumnCollation> columnCollations,
            List<NativeType> columnTypes
    );

    protected abstract boolean supportsPartialComparison();
}
