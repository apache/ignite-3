/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import org.apache.ignite.lang.IgniteLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link HashUtils}.
 */
class HashUtilsTest {
    /** Iterations. */
    private static final int ITERS = 10;

    /** Random. */
    private Random rnd;

    /**
     * Initialization.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        IgniteLogger.forClass(HashUtilsTest.class).info("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    /**
     * Tests for hash one byte.
     */
    @Test
    void testByte() {
        long h0 = 0;
        long h1 = 0;

        for (int i = 0; i < ITERS; ++i) {
            byte b = (byte) rnd.nextInt(Byte.MAX_VALUE);
            h0 = HashUtils.hash64(b, h0 & 0xffffffffL);
            h1 = HashUtils.hash64(new byte[]{b}, 0, 1, (int) h1);

            assertEquals(h0, h1);
        }
    }

    /**
     * Tests for hash one byte.
     */
    @Test
    void testShort() {
        long h0 = 0;
        long h1 = 0;

        for (int i = 0; i < ITERS; ++i) {
            short d = (short) rnd.nextInt(Short.MAX_VALUE);
            h0 = HashUtils.hash64(d, h0 & 0xffffffffL);
            h1 = HashUtils.hash64(new byte[]{(byte) (d & 0xff), (byte) ((d >> 8) & 0xff)}, 0, 2, (int) h1);

            assertEquals(h0, h1);
        }
    }
}
