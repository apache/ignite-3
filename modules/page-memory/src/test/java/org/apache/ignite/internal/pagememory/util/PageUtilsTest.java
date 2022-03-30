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

package org.apache.ignite.internal.pagememory.util;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getBytes;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putByteBuffer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.apache.ignite.internal.util.GridUnsafe;
import org.junit.jupiter.api.Test;

/**
 * For {@link PageUtils} testing.
 */
public class PageUtilsTest {
    @Test
    void testPutByteBufferIndirect() {
        byte[] bytes = randomBytes(1024);

        ByteBuffer buf = ByteBuffer.wrap(bytes);

        assertFalse(buf.isDirect());

        // Checking that the entire buffer will be written.

        assertEquals(0, buf.position());

        allocateNativeMemory(2048, addr -> {
            putByteBuffer(addr, 256, buf);

            assertArrayEquals(bytes, getBytes(addr, 256, bytes.length));
        });

        // Checking that the buffer will be written from a certain position.

        int pos = 128;

        buf.position(pos);

        allocateNativeMemory(2048, addr -> {
            putByteBuffer(addr, 256, buf);

            assertArrayEquals(
                    Arrays.copyOfRange(bytes, pos, bytes.length),
                    getBytes(addr, 256, bytes.length - pos));
        });
    }

    @Test
    void testPutByteBufferIndirectSlice() {
        byte[] bytes = randomBytes(1024);

        int pos = 256;
        int len = 512;

        ByteBuffer buf = ByteBuffer.wrap(bytes).position(pos).limit(pos + len).slice();

        assertFalse(buf.isDirect());

        // Checking that the entire buffer will be written.

        assertEquals(0, buf.position());

        // Checking that the entire buffer will be written.

        assertEquals(0, buf.position());

        allocateNativeMemory(2048, addr -> {
            putByteBuffer(addr, 256, buf);

            assertArrayEquals(
                    Arrays.copyOfRange(bytes, pos, pos + len),
                    getBytes(addr, 256, len));
        });

        // Checking that the buffer will be written from a certain position.

        int newPos = 128;

        buf.position(newPos);

        allocateNativeMemory(2048, addr -> {
            putByteBuffer(addr, 256, buf);

            assertArrayEquals(
                    Arrays.copyOfRange(bytes, pos + newPos, pos + len),
                    getBytes(addr, 256, len - newPos));
        });
    }

    @Test
    void testPutByteBufferDirect() {
        final byte[] bytes = randomBytes(1024);

        // Checking that the entire buffer will be written.

        allocateDirectBuffer(1024, buf -> {
            assertTrue(buf.isDirect());

            buf.put(bytes).rewind();

            assertEquals(0, buf.position());

            allocateNativeMemory(2048, addr -> {
                putByteBuffer(addr, 256, buf);

                assertArrayEquals(bytes, getBytes(addr, 256, bytes.length));
            });
        });

        // Checking that the buffer will be written from a certain position.

        int pos = 128;

        allocateDirectBuffer(1024, buf -> {
            assertTrue(buf.isDirect());

            buf.put(bytes).position(pos);

            allocateNativeMemory(2048, addr -> {
                putByteBuffer(addr, 256, buf);

                byte[] exp = new byte[bytes.length - pos];
                System.arraycopy(bytes, pos, exp, 0, exp.length);

                assertArrayEquals(exp, getBytes(addr, 256, exp.length));
            });
        });
    }

    private byte[] randomBytes(int len) {
        byte[] bytes = new byte[len];

        ThreadLocalRandom.current().nextBytes(bytes);

        return bytes;
    }

    private void allocateNativeMemory(int size, LongConsumer adderConsumer) {
        long addr = GridUnsafe.allocateMemory(size);

        try {
            adderConsumer.accept(addr);
        } finally {
            GridUnsafe.freeMemory(addr);
        }
    }

    private void allocateDirectBuffer(int size, Consumer<ByteBuffer> directBufConsumer) {
        ByteBuffer buf = GridUnsafe.allocateBuffer(size);

        try {
            directBufConsumer.accept(buf);
        } finally {
            GridUnsafe.freeBuffer(buf);
        }
    }
}
