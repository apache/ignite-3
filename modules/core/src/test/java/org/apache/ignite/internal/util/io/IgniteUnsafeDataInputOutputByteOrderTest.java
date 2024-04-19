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

package org.apache.ignite.internal.util.io;

import static org.apache.ignite.internal.util.io.IgniteTestIoUtils.getCharByByteLittleEndian;
import static org.apache.ignite.internal.util.io.IgniteTestIoUtils.getDoubleByByteLittleEndian;
import static org.apache.ignite.internal.util.io.IgniteTestIoUtils.getFloatByByteLittleEndian;
import static org.apache.ignite.internal.util.io.IgniteTestIoUtils.getIntByByteLittleEndian;
import static org.apache.ignite.internal.util.io.IgniteTestIoUtils.getLongByByteLittleEndian;
import static org.apache.ignite.internal.util.io.IgniteTestIoUtils.getShortByByteLittleEndian;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Ignite unsafe data input/output byte order sanity tests.
 */
class IgniteUnsafeDataInputOutputByteOrderTest extends BaseIgniteAbstractTest {
    /** Array length. */
    private static final int ARR_LEN = 16;

    /** Length bytes. */
    private static final int LEN_BYTES = 0;

    private static final long SEED = System.nanoTime();

    /** Rnd. */
    private static final Random RND = new Random(SEED);

    @BeforeEach
    public void setup() {
        log.info("Seed: {}", SEED);
    }

    /** Out. */
    private IgniteUnsafeDataOutput out;

    /** In. */
    private IgniteUnsafeDataInput in;

    @BeforeEach
    public void setUp() throws Exception {
        out = new IgniteUnsafeDataOutput(16 * 8 + LEN_BYTES);
        in = new IgniteUnsafeDataInput();
        in.inputStream(new ByteArrayInputStream(out.internalArray()));
    }

    @AfterEach
    public void tearDown() throws Exception {
        in.close();
        out.close();
    }

    @Test
    public void testShort() throws Exception {
        short val = (short) RND.nextLong();

        out.writeShort(val);

        assertEquals(val, getShortByByteLittleEndian(out.internalArray()));
        assertEquals(val, in.readShort());
    }

    @Test
    public void testShortArray() throws Exception {
        short[] arr = new short[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++) {
            arr[i] = (short) RND.nextLong();
        }

        out.writeShortArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++) {
            assertEquals(arr[i], getShortByByteLittleEndian(outArr, i * 2 + LEN_BYTES));
        }

        assertArrayEquals(arr, in.readShortArray(ARR_LEN));
    }

    @Test
    public void testChar() throws Exception {
        char val = (char) RND.nextLong();

        out.writeChar(val);

        assertEquals(val, getCharByByteLittleEndian(out.internalArray()));
        assertEquals(val, in.readChar());
    }

    @Test
    public void testCharArray() throws Exception {
        char[] arr = new char[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++) {
            arr[i] = (char) RND.nextLong();
        }

        out.writeCharArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++) {
            assertEquals(arr[i], getCharByByteLittleEndian(outArr, i * 2 + LEN_BYTES));
        }

        assertArrayEquals(arr, in.readCharArray(ARR_LEN));
    }

    @Test
    public void testInt() throws Exception {
        int val = RND.nextInt();

        out.writeInt(val);

        assertEquals(val, getIntByByteLittleEndian(out.internalArray()));
        assertEquals(val, in.readInt());
    }

    @Test
    public void testIntArray() throws Exception {
        int[] arr = new int[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++) {
            arr[i] = RND.nextInt();
        }

        out.writeIntArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++) {
            assertEquals(arr[i], getIntByByteLittleEndian(outArr, i * 4 + LEN_BYTES));
        }

        assertArrayEquals(arr, in.readIntArray(ARR_LEN));
    }

    @Test
    public void testLong() throws Exception {
        long val = RND.nextLong();

        out.writeLong(val);

        assertEquals(val, getLongByByteLittleEndian(out.internalArray()));
        assertEquals(val, in.readLong());
    }

    @Test
    public void testLongArray() throws Exception {
        long[] arr = new long[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++) {
            arr[i] = RND.nextLong();
        }

        out.writeLongArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++) {
            assertEquals(arr[i], getLongByByteLittleEndian(outArr, i * 8 + LEN_BYTES));
        }

        assertArrayEquals(arr, in.readLongArray(ARR_LEN));
    }

    @Test
    public void testFloat() throws Exception {
        float val = RND.nextFloat();

        out.writeFloat(val);

        assertEquals(val, getFloatByByteLittleEndian(out.internalArray()), 0);
        assertEquals(val, in.readFloat(), 0);
    }

    @Test
    public void testFloatArray() throws Exception {
        float[] arr = new float[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++) {
            arr[i] = RND.nextFloat();
        }

        out.writeFloatArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++) {
            assertEquals(arr[i], getFloatByByteLittleEndian(outArr, i * 4 + LEN_BYTES), 0);
        }

        assertArrayEquals(arr, in.readFloatArray(ARR_LEN), 0);
    }

    @Test
    public void testDouble() throws Exception {
        double val = RND.nextDouble();

        out.writeDouble(val);

        assertEquals(val, getDoubleByByteLittleEndian(out.internalArray()), 0);
        assertEquals(val, in.readDouble(), 0);
    }

    @Test
    public void testDoubleArray() throws Exception {
        double[] arr = new double[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++) {
            arr[i] = RND.nextDouble();
        }

        out.writeDoubleArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++) {
            assertEquals(arr[i], getDoubleByByteLittleEndian(outArr, i * 8 + LEN_BYTES), 0);
        }

        assertArrayEquals(arr, in.readDoubleArray(ARR_LEN), 0);
    }

    @Test
    public void testLocalTime() throws IOException {
        LocalTime val = LocalTime.of(RND.nextInt(24), RND.nextInt(60), RND.nextInt(60), RND.nextInt(10000));

        out.writeLocalTime(val);

        assertEquals(val, in.readLocalTime());
    }

    @Test
    public void testLocalDate() throws IOException {
        LocalDate val = LocalDate.of(RND.nextInt(4000) - 1000, RND.nextInt(12) + 1, 1 + RND.nextInt(27));

        out.writeLocalDate(val);

        assertEquals(val, in.readLocalDate());
    }

    @Test
    public void testLocalDateTime() throws IOException {
        LocalTime time = LocalTime.of(RND.nextInt(24), RND.nextInt(60), RND.nextInt(60), RND.nextInt(10000));
        LocalDate date = LocalDate.of(RND.nextInt(4000) - 1000, RND.nextInt(12) + 1, 1 + RND.nextInt(27));
        LocalDateTime val = LocalDateTime.of(date, time);

        out.writeLocalDateTime(val);

        assertEquals(val, in.readLocalDateTime());
    }

    @Test
    public void testInstant() throws IOException {
        Instant val = Instant.ofEpochMilli(Math.abs(RND.nextLong()));

        out.writeInstant(val);

        assertEquals(val, in.readInstant());
    }

    @Test
    public void testBigInteger() throws IOException {
        BigInteger val = BigInteger.valueOf(RND.nextLong());

        out.writeBigInteger(val);

        assertEquals(val, in.readBigInteger());
    }

    @Test
    public void testBigDecimal() throws IOException {
        BigDecimal val = new BigDecimal(BigInteger.valueOf(RND.nextLong()), RND.nextInt(120));

        out.writeBigDecimal(val);

        assertEquals(val, in.readBigDecimal());
    }

    @Test
    public void testPeriod() throws IOException {
        Period val = Period.of(RND.nextInt(10_000), RND.nextInt(10_000), RND.nextInt(10_000));

        out.writePeriod(val);

        assertEquals(val, in.readPeriod());
    }

    @Test
    public void testDuration() throws IOException {
        Duration val = Duration.ofSeconds(RND.nextInt(100_000), RND.nextInt(100_000));

        out.writeDuration(val);

        assertEquals(val, in.readDuration());
    }

    @Test
    public void testUuid() throws IOException {
        UUID val = UUID.randomUUID();

        out.writeUuid(val);

        assertEquals(val, in.readUuid());
    }

    @Test
    public void testBitSet() throws IOException {
        BitSet val = new BitSet();

        for (int i = 0; i < RND.nextInt(100); i++) {
            int b = RND.nextInt(256);
            val.set(Math.abs(b));
        };

        out.writeBitSet(val);

        assertEquals(val, in.readBitSet());
    }
}
