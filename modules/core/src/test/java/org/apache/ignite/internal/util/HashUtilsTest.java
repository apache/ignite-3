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

package org.apache.ignite.internal.util;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.util.HashUtils.hash32;
import static org.apache.ignite.internal.util.HashUtils.hash64;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.ignite.internal.logger.Loggers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link HashUtils}.
 */
class HashUtilsTest {
    /** Iterations. */
    private static final int ITERS = 10;

    private static Random rnd;

    /**
     * Initialization.
     */
    @BeforeAll
    static void initRandom() {
        long seed = System.currentTimeMillis();

        Loggers.forClass(HashUtilsTest.class).info("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    /**
     * Check hash one byte.
     */
    @Test
    void hashByte() {
        long h0 = 0;
        long h1 = 0;

        for (int i = 0; i < ITERS; ++i) {
            byte b = (byte) rnd.nextInt(Byte.MAX_VALUE);
            h0 = hash64(b, h0 & 0xffffffffL);
            h1 = hash64(new byte[]{b}, 0, 1, (int) h1);

            assertEquals(h0, h1);
        }
    }

    /**
     * Check hash short.
     */
    @Test
    void hashShort() {
        long h0 = 0;
        long h1 = 0;

        for (int i = 0; i < ITERS; ++i) {
            short d = (short) rnd.nextInt(Short.MAX_VALUE);

            h0 = hash64(d, h0 & 0xffffffffL);
            h1 = hash64(new byte[]{(byte) (d & 0xff), (byte) ((d >> 8) & 0xff)}, 0, 2, (int) h1);

            assertEquals(h0, h1);
        }
    }

    /**
     * Check hash integer.
     */
    @Test
    void hashInteger() {
        long h0 = 0;
        long h1 = 0;

        for (int i = 0; i < ITERS; ++i) {
            int d = rnd.nextInt();

            h0 = hash64(d, h0 & 0xffffffffL);
            h1 = hash64(
                    new byte[]{
                            (byte) (d & 0xff),
                            (byte) ((d >> 8) & 0xff),
                            (byte) ((d >> 16) & 0xff),
                            (byte) ((d >> 24) & 0xff)
                    },
                    0, 4, (int) h1);

            assertEquals(h0, h1);
        }
    }

    /**
     * Check hash long.
     */
    @Test
    void hashLong() {
        long h0 = 0;
        long h1 = 0;

        for (int i = 0; i < ITERS; ++i) {
            long d = rnd.nextLong();

            h0 = hash64(d, h0 & 0xffffffffL);
            h1 = hash64(
                    new byte[]{
                            (byte) (d & 0xff),
                            (byte) ((d >> 8) & 0xff),
                            (byte) ((d >> 16) & 0xff),
                            (byte) ((d >> 24) & 0xff),
                            (byte) ((d >> 32) & 0xff),
                            (byte) ((d >> 40) & 0xff),
                            (byte) ((d >> 48) & 0xff),
                            (byte) ((d >> 56) & 0xff),
                    },
                    0, 8, (int) h1);

            assertEquals(h0, h1);
        }
    }

    /**
     * Tests that methods operating with ByteBuffers are equivalent to their array-based counterparts.
     */
    @RepeatedTest(ITERS)
    void testByteBufferEquivalence() {
        // Byte array length is chosen to be 63 in order to test both the "block" part of the algorithm (which operates in 16 byte chunks)
        // and the "tail" part (for the remaining bytes).
        byte[] bytes = randomBytes(rnd, 63);

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        assertEquals(hash32(bytes), hash32(buffer));
        assertEquals(hash64(bytes), hash64(buffer));

        int seed = rnd.nextInt();

        int randomOffset = rnd.nextInt(bytes.length - 1);

        int randomLength = rnd.nextInt(bytes.length - randomOffset);

        assertEquals(hash64(bytes, randomOffset, randomLength, seed), hash64(buffer, randomOffset, randomLength, seed));
    }
}
