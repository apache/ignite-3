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

package org.apache.ignite.internal.binarytuple;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.logger.Loggers;
import org.junit.jupiter.api.Test;

/**
 * Tests for BinaryTuple (IEP-92) support.
 */
public class BinaryTupleTest {
    /**
     * Test reading NULL values.
     */
    @Test
    public void nullValueTest() {
        // Header: 1 byte with null map flag.
        // NullMap: 1 byte with first bit set.
        // Offset table: 1 zero byte
        byte[] bytes = { BinaryTupleCommon.NULLMAP_FLAG, 1, 0 };

        var reader = new BinaryTupleReader(1, bytes);
        assertEquals(bytes.length, reader.size());
        assertEquals(1, reader.elementCount());
        assertTrue(reader.hasNullMap());

        assertTrue(reader.hasNullValue(0));
        assertNull(reader.byteValueBoxed(0));
        assertNull(reader.shortValueBoxed(0));
        assertNull(reader.intValueBoxed(0));
        assertNull(reader.longValueBoxed(0));
        assertNull(reader.floatValueBoxed(0));
        assertNull(reader.doubleValueBoxed(0));
        assertNull(reader.numberValue(0));
        assertNull(reader.decimalValue(0, 0));
        assertNull(reader.stringValue(0));
        assertNull(reader.bytesValue(0));
        assertNull(reader.uuidValue(0));
        assertNull(reader.bitmaskValue(0));
        assertNull(reader.dateValue(0));
        assertNull(reader.timeValue(0));
        assertNull(reader.dateTimeValue(0));
        assertNull(reader.timestampValue(0));
    }

    /**
     * Test reading default values.
     */
    @Test
    public void defaultTest() {
        // Header: 1 zero byte.
        // Offset table: 1 zero byte.
        byte[] bytes1 = { 0, 0 };

        // Header: 1 byte with null map flag.
        // NullMap: 1 byte with no bit set.
        // Offset table: 1 zero byte
        byte[] bytes2 = { BinaryTupleCommon.NULLMAP_FLAG, 0, 0 };

        byte[][] bytesArray = { bytes1, bytes2 };

        for (byte[] bytes : bytesArray) {
            var reader = new BinaryTupleReader(1, bytes);
            assertEquals(bytes.length, reader.size());
            assertEquals(1, reader.elementCount());
            if (bytes.length == bytes1.length) {
                assertFalse(reader.hasNullMap());
            } else {
                assertTrue(reader.hasNullMap());
            }

            assertFalse(reader.hasNullValue(0));
            assertEquals(0, reader.byteValue(0));
            assertEquals(Byte.valueOf((byte) 0), reader.byteValueBoxed(0));
            assertEquals(0, reader.shortValue(0));
            assertEquals(Short.valueOf((short) 0), reader.shortValueBoxed(0));
            assertEquals(0, reader.intValue(0));
            assertEquals(Integer.valueOf(0), reader.intValueBoxed(0));
            assertEquals(0L, reader.longValue(0));
            assertEquals(Long.valueOf(0L), reader.longValueBoxed(0));
            assertEquals(0.0F, reader.floatValue(0));
            assertEquals(Float.valueOf(0.0F), reader.floatValueBoxed(0));
            assertEquals(0.0, reader.doubleValue(0));
            assertEquals(Double.valueOf(0.0), reader.doubleValueBoxed(0));
            /* We do not support default values for big numbers:
            assertEquals(_NO_SUCH_VALUE_, reader.numberValue(0));
            assertEquals(_NO_SUCH_VALUE_, reader.decimalValue(0, 0));
            */
            assertEquals("", reader.stringValue(0));
            assertArrayEquals(new byte[0], reader.bytesValue(0));
            assertEquals(new UUID(0, 0), reader.uuidValue(0));
            assertEquals(new BitSet(0), reader.bitmaskValue(0));
            assertEquals(LocalDate.of(0, 1, 1), reader.dateValue(0));
            assertEquals(LocalTime.of(0, 0, 0), reader.timeValue(0));
            assertEquals(LocalDateTime.of(0, 1, 1, 0, 0, 0), reader.dateTimeValue(0));
            assertEquals(Instant.ofEpochSecond(0), reader.timestampValue(0));
        }
    }

    /**
     * Test byte value encoding.
     */
    @Test
    public void byteTest() {
        byte[] values = {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE};
        for (byte value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 1);
            ByteBuffer bytes = builder.appendByte(value).build();
            assertEquals(value != 0 ? 1 : 0, bytes.get(1));
            assertEquals(value != 0 ? 3 : 2, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.byteValue(0));
        }
    }

    /**
     * Test short value encoding.
     */
    @Test
    public void shortTest() {
        short[] values = {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE};
        for (short value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 1);
            ByteBuffer bytes = builder.appendShort(value).build();
            assertEquals(value != 0 ? 1 : 0, bytes.get(1));
            assertEquals(value != 0 ? 3 : 2, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.shortValue(0));
        }

        values = new short[]{Short.MIN_VALUE, Byte.MIN_VALUE - 1, Byte.MAX_VALUE + 1, Short.MAX_VALUE};
        for (short value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 2);
            ByteBuffer bytes = builder.appendShort(value).build();
            assertEquals(2, bytes.get(1));
            assertEquals(4, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.shortValue(0));
        }
    }

    /**
     * Test int value encoding.
     */
    @Test
    public void intTest() {
        int[] values = {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE};
        for (int value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 1);
            ByteBuffer bytes = builder.appendInt(value).build();
            assertEquals(value != 0 ? 1 : 0, bytes.get(1));
            assertEquals(value != 0 ? 3 : 2, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.intValue(0));
        }

        values = new int[]{Short.MIN_VALUE, Byte.MIN_VALUE - 1, Byte.MAX_VALUE + 1, Short.MAX_VALUE};
        for (int value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 2);
            ByteBuffer bytes = builder.appendInt(value).build();
            assertEquals(2, bytes.get(1));
            assertEquals(4, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.intValue(0));
        }

        values = new int[]{Integer.MIN_VALUE, Short.MIN_VALUE - 1, Short.MAX_VALUE + 1, Integer.MAX_VALUE};
        for (int value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 4);
            ByteBuffer bytes = builder.appendInt(value).build();
            assertEquals(4, bytes.get(1));
            assertEquals(6, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.intValue(0));
        }
    }

    /**
     * Test long value encoding.
     */
    @Test
    public void longTest() {
        long[] values = {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE};
        for (long value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 1);
            ByteBuffer bytes = builder.appendLong(value).build();
            assertEquals(value != 0 ? 1 : 0, bytes.get(1));
            assertEquals(value != 0 ? 3 : 2, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.longValue(0));
        }

        values = new long[]{Short.MIN_VALUE, Byte.MIN_VALUE - 1, Byte.MAX_VALUE + 1, Short.MAX_VALUE};
        for (long value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 2);
            ByteBuffer bytes = builder.appendLong(value).build();
            assertEquals(2, bytes.get(1));
            assertEquals(4, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.longValue(0));
        }

        values = new long[]{Integer.MIN_VALUE, Short.MIN_VALUE - 1, Short.MAX_VALUE + 1, Integer.MAX_VALUE};
        for (long value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 4);
            ByteBuffer bytes = builder.appendLong(value).build();
            assertEquals(4, bytes.get(1));
            assertEquals(6, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.longValue(0));
        }

        values = new long[]{Long.MIN_VALUE, Integer.MIN_VALUE - 1L, Integer.MAX_VALUE + 1L, Long.MAX_VALUE};
        for (long value : values) {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 8);
            ByteBuffer bytes = builder.appendLong(value).build();
            assertEquals(8, bytes.get(1));
            assertEquals(10, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.longValue(0));
        }
    }

    /**
     * Test float value encoding.
     */
    @Test
    public void floatTest() {
        {
            float value = 0.0F;

            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 0);
            ByteBuffer bytes = builder.appendFloat(value).build();
            assertEquals(0, bytes.get(1));
            assertEquals(2, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.floatValue(0));
        }
        {
            float value = 0.5F;

            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 4);
            ByteBuffer bytes = builder.appendFloat(value).build();
            assertEquals(4, bytes.get(1));
            assertEquals(6, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.floatValue(0));
        }
    }

    /**
     * Test double value encoding.
     */
    @Test
    public void doubleTest() {
        {
            double value = 0.0;

            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 0);
            ByteBuffer bytes = builder.appendDouble(value).build();
            assertEquals(0, bytes.get(1));
            assertEquals(2, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.doubleValue(0));
        }
        {
            double value = 0.5;

            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 4);
            ByteBuffer bytes = builder.appendDouble(value).build();
            assertEquals(4, bytes.get(1));
            assertEquals(6, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.doubleValue(0));
        }
        {
            double value = 0.1;

            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false, 8);
            ByteBuffer bytes = builder.appendDouble(value).build();
            assertEquals(8, bytes.get(1));
            assertEquals(10, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.doubleValue(0));
        }
    }

    /**
     * Test big integer value encoding.
     */
    @Test
    public void numberTest() {
        BigInteger value = BigInteger.valueOf(12345);

        BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
        ByteBuffer bytes = builder.appendNumber(value).build();

        BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
        assertEquals(value, reader.numberValue(0));
    }

    /**
     * Test big decimal value encoding.
     */
    @Test
    public void decimalTest() {
        BigDecimal value = new BigDecimal(BigInteger.valueOf(12345), 100);

        BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
        ByteBuffer bytes = builder.appendDecimal(value, 100).build();

        BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
        assertEquals(value, reader.decimalValue(0, 100));
    }

    /**
     * Test big decimal value encoding with different scale in value and schema.
     */
    @Test
    public void decimalScaleTest() {
        int schemaScale = 10;
        int valueScale = 3;
        BigDecimal value = BigDecimal.valueOf(123456, valueScale);

        BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
        ByteBuffer bytes = builder.appendDecimal(value, schemaScale).build();

        BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
        BigDecimal res = reader.decimalValue(0, schemaScale);

        assertEquals(value.doubleValue(), res.doubleValue());
    }

    /**
     * Test string value encoding.
     */
    @Test
    public void stringTest() {
        String[] values = {"ascii", "我愛Java", "", "a string with a bit more characters"};

        BinaryTupleBuilder builder = new BinaryTupleBuilder(values.length, false);
        for (String value : values) {
            builder.appendString(value);
        }
        ByteBuffer bytes = builder.build();

        BinaryTupleReader reader = new BinaryTupleReader(values.length, bytes);
        reader.parse((i, b, e) -> assertEquals(values[i].getBytes(StandardCharsets.UTF_8).length, e - b));
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], reader.stringValue(i));
        }
    }

    /**
     * Test binary value encoding.
     */
    @Test
    public void binaryTest() {
        Random rnd = getRng();

        for (int n = 1; n < 100_000; n = n < 100 ? n + 1 : n * 10) {
            byte[][] values = new byte[n][];
            for (int i = 0; i < n; i++) {
                values[i] = generateBytes(rnd);
            }

            BinaryTupleBuilder builder = new BinaryTupleBuilder(values.length, true);
            for (byte[] value : values) {
                builder.appendBytes(value);
            }
            ByteBuffer bytes = builder.build();

            BinaryTupleReader reader = new BinaryTupleReader(values.length, bytes);
            reader.parse((i, b, e) -> {
                if (values[i] == null) {
                    assertEquals(0, b);
                    assertEquals(0, e);
                } else {
                    assertEquals(values[i].length, e - b);
                }
            });
            for (int i = 0; i < values.length; i++) {
                assertArrayEquals(values[i], reader.bytesValue(i));
            }
        }
    }

    /**
     * Test UUID value encoding.
     */
    @Test
    public void uuidTest() {
        UUID value = UUID.randomUUID();

        BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
        ByteBuffer bytes = builder.appendUuid(value).build();

        BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
        assertEquals(value, reader.uuidValue(0));
    }

    /**
     * Test bitmask value encoding.
     */
    @Test
    public void bitmaskTest() {
        Random rnd = getRng();

        for (int i = 0; i < 100; i++) {
            byte[] valueBytes = generateBytes(rnd);
            if (valueBytes != null) {
                BitSet value = BitSet.valueOf(valueBytes);

                BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
                ByteBuffer bytes = builder.appendBitmask(value).build();

                BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
                assertEquals(value, reader.bitmaskValue(0));
            }
        }
    }

    /**
     * Test Date value encoding.
     */
    @Test
    public void dateTest() {
        LocalDate value = LocalDate.now();

        BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
        ByteBuffer bytes = builder.appendDate(value).build();
        assertEquals(3, bytes.get(1));
        assertEquals(5, bytes.limit());

        BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
        assertEquals(value, reader.dateValue(0));
    }

    /**
     * Test Time value encoding.
     */
    @Test
    public void timeTest() {
        LocalTime value = LocalTime.now();

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendTime(value).build();

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.timeValue(0));
        }

        value = LocalTime.of(value.getHour(), value.getMinute(), value.getSecond(), 1_000_000);

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendTime(value).build();
            assertEquals(4, bytes.get(1));
            assertEquals(6, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.timeValue(0));
        }

        value = LocalTime.of(value.getHour(), value.getMinute(), value.getSecond(), 1_001_000);

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendTime(value).build();
            assertEquals(5, bytes.get(1));
            assertEquals(7, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.timeValue(0));
        }

        value = LocalTime.of(value.getHour(), value.getMinute(), value.getSecond(), 1_001_001);

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendTime(value).build();
            assertEquals(6, bytes.get(1));
            assertEquals(8, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.timeValue(0));
        }
    }

    /**
     * Test DateTime value encoding.
     */
    @Test
    public void dateTimeTest() {
        LocalDateTime value = LocalDateTime.now();

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendDateTime(value).build();

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.dateTimeValue(0));
        }

        value = LocalDateTime.of(value.toLocalDate(),
                LocalTime.of(value.getHour(), value.getMinute(), value.getSecond(), 1_000_000));

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendDateTime(value).build();
            assertEquals(7, bytes.get(1));
            assertEquals(9, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.dateTimeValue(0));
        }

        value = LocalDateTime.of(value.toLocalDate(),
                LocalTime.of(value.getHour(), value.getMinute(), value.getSecond(), 1_001_000));

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendDateTime(value).build();
            assertEquals(8, bytes.get(1));
            assertEquals(10, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.dateTimeValue(0));
        }

        value = LocalDateTime.of(value.toLocalDate(),
                LocalTime.of(value.getHour(), value.getMinute(), value.getSecond(), 1_001_001));

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendDateTime(value).build();
            assertEquals(9, bytes.get(1));
            assertEquals(11, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.dateTimeValue(0));
        }

    }

    /**
     * Test Timestamp value encoding.
     */
    @Test
    public void timestampTest() {
        Instant value = Instant.now();

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendTimestamp(value).build();

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.timestampValue(0));
        }

        value = Instant.ofEpochSecond(value.getEpochSecond());

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendTimestamp(value).build();
            assertEquals(8, bytes.get(1));
            assertEquals(10, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.timestampValue(0));
        }

        value = Instant.ofEpochSecond(value.getEpochSecond(), 1);

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendTimestamp(value).build();
            assertEquals(12, bytes.get(1));
            assertEquals(14, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.timestampValue(0));
        }
    }

    /**
     * Test Duration value encoding.
     */
    @Test
    public void durationTest() {
        Instant now = Instant.now();
        Duration value = Duration.ofSeconds(now.getEpochSecond(), now.getNano());

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendDuration(value).build();

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.durationValue(0));
        }

        value = Duration.ofSeconds(now.getEpochSecond());

        {
            BinaryTupleBuilder builder = new BinaryTupleBuilder(1, false);
            ByteBuffer bytes = builder.appendDuration(value).build();
            assertEquals(8, bytes.get(1));
            assertEquals(10, bytes.limit());

            BinaryTupleReader reader = new BinaryTupleReader(1, bytes);
            assertEquals(value, reader.durationValue(0));
        }
    }

    /** Get a pseudo-random number generator. */
    private Random getRng() {
        long seed = System.currentTimeMillis();
        Loggers.forClass(BinaryTupleTest.class).info("Using seed: " + seed + "L; //");
        return new Random(seed);
    }

    /** Generate a random byte array. */
    private byte[] generateBytes(Random rnd) {
        byte[] value = null;
        int choice = rnd.nextInt(2000);
        if (choice < 1000) {
            value = new byte[choice + 1];
            rnd.nextBytes(value);
        } else if (choice < 1500) {
            value = new byte[0];
        }
        return value;
    }
}
