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

package org.apache.ignite.internal.storage.index;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.SortedIndexColumnDescriptor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the {@link BinaryTupleComparator} class.
 */
public class BinaryTupleComparatorTest {

    @ParameterizedTest
    @MethodSource("allTypes")
    public void testCompareSingleColumnTuples(NativeType type) {
        var columnDescriptor = new SortedIndexColumnDescriptor("column", type, false, true);

        var descriptor = new SortedIndexDescriptor(UUID.randomUUID(), List.of(columnDescriptor));

        var comparator = new BinaryTupleComparator(descriptor);

        IgniteBiTuple<ByteBuffer, ByteBuffer> tuples = createTestValues(type);

        assertThat(comparator.compare(tuples.get1(), tuples.get2()), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuples.get1(), tuples.get1()), is(0));
        assertThat(comparator.compare(tuples.get2(), tuples.get2()), is(0));
        assertThat(comparator.compare(tuples.get2(), tuples.get1()), is(greaterThanOrEqualTo(1)));
    }

    private static List<NativeType> allTypes() {
        return List.of(
                NativeTypes.INT8,
                NativeTypes.INT16,
                NativeTypes.INT32,
                NativeTypes.INT64,
                NativeTypes.FLOAT,
                NativeTypes.DOUBLE,
                NativeTypes.BYTES,
                NativeTypes.bitmaskOf(42),
                NativeTypes.decimalOf(20, 3),
                NativeTypes.UUID,
                NativeTypes.STRING,
                NativeTypes.numberOf(20),
                NativeTypes.timestamp(),
                NativeTypes.DATE,
                NativeTypes.datetime()
        );
    }

    private static IgniteBiTuple<ByteBuffer, ByteBuffer> createTestValues(NativeType type) {
        ByteBuffer tuple1;
        ByteBuffer tuple2;

        switch (type.spec()) {
            case INT8: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendByte((byte) -1)
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendByte(Byte.MAX_VALUE)
                        .build();

                break;
            }

            case INT16: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendShort((short) -1)
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendShort(Short.MAX_VALUE)
                        .build();

                break;
            }

            case INT32: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendInt(-1)
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendInt(Integer.MAX_VALUE)
                        .build();

                break;
            }

            case INT64: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendLong(-1)
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendLong(Long.MAX_VALUE)
                        .build();

                break;
            }

            case FLOAT: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendFloat(-1.69f)
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendFloat(Float.MAX_VALUE)
                        .build();

                break;
            }

            case DOUBLE: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendDouble(-1.69)
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendDouble(Double.MAX_VALUE)
                        .build();

                break;
            }

            case BYTES: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendBytes(new byte[] {1, 2, 3})
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendBytes(new byte[] {1, 2, 6})
                        .build();

                break;
            }

            case BITMASK: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendBitmask(BitSet.valueOf(new byte[] { 1, 2, 3 }))
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendBitmask(BitSet.valueOf(new byte[] {-1, -1, -1}))
                        .build();

                break;
            }

            case DECIMAL: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendDecimal(BigDecimal.valueOf(-1), 4)
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendDecimal(BigDecimal.valueOf(123456789.1234), 4)
                        .build();

                break;
            }

            case UUID: {
                UUID uuid1 = UUID.randomUUID();
                UUID uuid2 = UUID.randomUUID();

                if (uuid1.compareTo(uuid2) > 0) {
                    UUID t = uuid1;
                    uuid1 = uuid2;
                    uuid2 = t;
                }

                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendUuid(uuid1)
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendUuid(uuid2)
                        .build();

                break;
            }

            case STRING: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendString("foobar")
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendString("foobaz")
                        .build();

                break;
            }

            case NUMBER: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendNumber(BigInteger.valueOf(-1))
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendNumber(BigInteger.TEN)
                        .build();

                break;
            }

            case TIMESTAMP: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendTimestamp(Instant.ofEpochSecond(1))
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendTimestamp(Instant.ofEpochSecond(42))
                        .build();

                break;
            }

            case DATE: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendDate(LocalDate.of(2000, 4, 10))
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendDate(LocalDate.of(2000, 4, 15))
                        .build();

                break;
            }

            case TIME: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendTime(LocalTime.of(10, 0, 0))
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendTime(LocalTime.of(10, 0, 1))
                        .build();

                break;
            }

            case DATETIME: {
                tuple1 = new BinaryTupleBuilder(1, false)
                        .appendDateTime(LocalDateTime.of(2000, 4, 10, 10, 0, 0))
                        .build();

                tuple2 = new BinaryTupleBuilder(1, false)
                        .appendDateTime(LocalDateTime.of(2000, 4, 10, 10, 0, 1))
                        .build();

                break;
            }

            default:
                throw new AssertionError(type.toString());
        }

        return new IgniteBiTuple<>(tuple1, tuple2);
    }

    @Test
    public void testCompareMultipleColumnTuples() {
        List<SortedIndexColumnDescriptor> columnDescriptors = List.of(
                new SortedIndexColumnDescriptor("column", NativeTypes.INT32, false, true),
                new SortedIndexColumnDescriptor("column", NativeTypes.STRING, false,  false)
        );

        var descriptor = new SortedIndexDescriptor(UUID.randomUUID(), columnDescriptors);

        var comparator = new BinaryTupleComparator(descriptor);

        ByteBuffer tuple1 = new BinaryTupleBuilder(2, false)
                .appendInt(0)
                .appendString("foobar")
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(2, false)
                .appendInt(1)
                .appendString("foobar")
                .build();

        assertThat(comparator.compare(tuple1, tuple2), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple1, tuple1), is(0));
        assertThat(comparator.compare(tuple2, tuple1), is(greaterThanOrEqualTo(1)));

        tuple2 = new BinaryTupleBuilder(2, false)
                .appendInt(0)
                .appendString("foobaa")
                .build();

        assertThat(comparator.compare(tuple1, tuple2), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple2, tuple1), is(greaterThanOrEqualTo(1)));
    }

    @Test
    public void testCompareMultipleColumnTuplesWithNulls() {
        List<SortedIndexColumnDescriptor> columnDescriptors = List.of(
                new SortedIndexColumnDescriptor("column", NativeTypes.INT32, true, true),
                new SortedIndexColumnDescriptor("column", NativeTypes.STRING, true,  false)
        );

        var descriptor = new SortedIndexDescriptor(UUID.randomUUID(), columnDescriptors);

        var comparator = new BinaryTupleComparator(descriptor);

        ByteBuffer tuple1 = new BinaryTupleBuilder(2, true)
                .appendInt(null)
                .appendString("foobar")
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(2, true)
                .appendInt(1)
                .appendString("foobar")
                .build();

        assertThat(comparator.compare(tuple1, tuple2), is(greaterThanOrEqualTo(1)));
        assertThat(comparator.compare(tuple1, tuple1), is(0));
        assertThat(comparator.compare(tuple2, tuple1), is(lessThanOrEqualTo(-1)));

        tuple2 = new BinaryTupleBuilder(2, true)
                .appendInt(null)
                .appendString("foobaa")
                .build();

        assertThat(comparator.compare(tuple1, tuple2), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple2, tuple1), is(greaterThanOrEqualTo(1)));

        tuple2 = new BinaryTupleBuilder(2, true)
                .appendInt(null)
                .appendString(null)
                .build();

        assertThat(comparator.compare(tuple1, tuple2), is(greaterThanOrEqualTo(1)));
        assertThat(comparator.compare(tuple2, tuple1), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple2, tuple2), is(0));
    }

    @Test
    public void testCompareWithPrefix() {
        List<SortedIndexColumnDescriptor> columnDescriptors = List.of(
                new SortedIndexColumnDescriptor("column", NativeTypes.INT32, false, true),
                new SortedIndexColumnDescriptor("column", NativeTypes.STRING, false,  false)
        );

        var descriptor = new SortedIndexDescriptor(UUID.randomUUID(), columnDescriptors);

        var comparator = new BinaryTupleComparator(descriptor);

        ByteBuffer tuple1 = new BinaryTupleBuilder(2, false)
                .appendInt(1)
                .appendString("foobar")
                .build();

        ByteBuffer tuple2 = new BinaryTuplePrefixBuilder(1, 2)
                .appendInt(2)
                .build();

        assertThat(comparator.compare(tuple1, tuple2), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple2, tuple1), is(greaterThanOrEqualTo(1)));

        tuple2 = new BinaryTuplePrefixBuilder(1, 2)
                .appendInt(0)
                .build();

        assertThat(comparator.compare(tuple2, tuple1), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple1, tuple2), is(greaterThanOrEqualTo(1)));

        tuple2 = new BinaryTuplePrefixBuilder(1, 2)
                .appendInt(1)
                .build();

        assertThat(comparator.compare(tuple2, tuple1), is(lessThanOrEqualTo(-1)));
        assertThat(comparator.compare(tuple1, tuple2), is(greaterThanOrEqualTo(1)));

        setEqualityFlag(tuple2);

        assertThat(comparator.compare(tuple2, tuple1), is(lessThanOrEqualTo(1)));
        assertThat(comparator.compare(tuple1, tuple2), is(greaterThanOrEqualTo(-1)));
    }

    @Test
    public void testCompareWithPrefixWithNulls() {
        List<SortedIndexColumnDescriptor> columnDescriptors = List.of(
                new SortedIndexColumnDescriptor("column", NativeTypes.INT32, true, true),
                new SortedIndexColumnDescriptor("column", NativeTypes.STRING, false,  false)
        );

        var descriptor = new SortedIndexDescriptor(UUID.randomUUID(), columnDescriptors);

        var comparator = new BinaryTupleComparator(descriptor);

        ByteBuffer tuple1 = new BinaryTupleBuilder(2, true)
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

    private static void setEqualityFlag(ByteBuffer buffer) {
        byte flags = buffer.get(0);

        buffer.put(0, (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG));
    }
}
