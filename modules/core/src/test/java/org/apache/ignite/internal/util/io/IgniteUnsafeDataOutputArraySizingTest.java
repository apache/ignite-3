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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link IgniteUnsafeDataOutput} concerning internal buffer sizing.
 */
class IgniteUnsafeDataOutputArraySizingTest {
    /** Small array. */
    private static final byte[] SMALL = new byte[32];

    /** Big array. */
    private static final byte[] BIG = new byte[2048];

    /** Buffer timeout. */
    private static final long BUFFER_TIMEOUT = 1000;

    /** Wait timeout is bigger then buffer timeout to prevent failures due to time measurement error. */
    private static final long WAIT_BUFFER_TIMEOUT = BUFFER_TIMEOUT + BUFFER_TIMEOUT / 2;

    @Test
    public void testShrink() throws Exception {
        final IgniteUnsafeDataOutput out = new IgniteUnsafeDataOutput(512, BUFFER_TIMEOUT);

        assertTrue(IgniteTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 256), WAIT_BUFFER_TIMEOUT));
        assertTrue(IgniteTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 128), WAIT_BUFFER_TIMEOUT));
        assertTrue(IgniteTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 64), WAIT_BUFFER_TIMEOUT));
        assertFalse(IgniteTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 32), WAIT_BUFFER_TIMEOUT));
        assertTrue(IgniteTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 64), WAIT_BUFFER_TIMEOUT));
    }

    @Test
    public void testGrow() throws Exception {
        IgniteUnsafeDataOutput out = new IgniteUnsafeDataOutput(512);

        out.write(BIG);
        out.cleanup();

        assertEquals(4096, out.internalArray().length);
    }

    @Test
    public void testChanged1() throws Exception {
        IgniteUnsafeDataOutput out = new IgniteUnsafeDataOutput(512, BUFFER_TIMEOUT);

        for (int i = 0; i < 100; i++) {
            Thread.sleep(100);

            out.write(SMALL);
            out.cleanup();
            out.write(BIG);
            out.cleanup();
        }

        assertEquals(4096, out.internalArray().length);
    }

    @Test
    public void testChanged2() throws Exception {
        final IgniteUnsafeDataOutput out = new IgniteUnsafeDataOutput(512, BUFFER_TIMEOUT);

        assertTrue(IgniteTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 256), WAIT_BUFFER_TIMEOUT));

        out.write(BIG);
        out.cleanup();
        assertEquals(4096, out.internalArray().length);

        assertTrue(IgniteTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 4096), WAIT_BUFFER_TIMEOUT));
        assertTrue(IgniteTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 2048), 2 * WAIT_BUFFER_TIMEOUT));
    }

    private static class WriteAndCheckPredicate implements BooleanSupplier {
        final IgniteUnsafeDataOutput out;

        final byte[] bytes;

        final int len;

        WriteAndCheckPredicate(IgniteUnsafeDataOutput out, byte[] bytes, int len) {
            this.out = out;
            this.bytes = bytes;
            this.len = len;
        }

        /** {@inheritDoc} */
        @Override
        public boolean getAsBoolean() {
            try {
                out.write(bytes);
                out.cleanup();

                return out.internalArray().length == len;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
