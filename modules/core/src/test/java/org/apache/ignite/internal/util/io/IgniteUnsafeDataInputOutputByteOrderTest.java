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
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Ignite unsafe data input/output byte order sanity tests.
 */
class IgniteUnsafeDataInputOutputByteOrderTest {
    /** Array length. */
    private static final int ARR_LEN = 16;

    /** Length bytes. */
    private static final int LEN_BYTES = 0;

    /** Rnd. */
    private static final Random RND = new Random();

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
}
