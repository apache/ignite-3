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

package org.apache.ignite.internal.pagememory.util;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getBytes;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putByteBuffer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongConsumer;
import org.apache.ignite.internal.util.GridUnsafe;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * For {@link PageUtils} testing.
 */
public class PageUtilsTest {
    @ParameterizedTest(name = "position = {0}")
    @ValueSource(ints = {0, 128})
    void testPutByteBufferIndirect(int position) {
        byte[] bytes = randomBytes(1024);

        ByteBuffer buf = ByteBuffer.wrap(bytes).position(position);

        assertFalse(buf.isDirect());

        allocateNativeMemory(2048, addr -> {
            putByteBuffer(addr, 256, buf);

            assertArrayEquals(
                    Arrays.copyOfRange(bytes, position, bytes.length),
                    getBytes(addr, 256, bytes.length - position)
            );
        });
    }

    @ParameterizedTest(name = "newPosition = {0}")
    @ValueSource(ints = {0, 128})
    void testPutByteBufferIndirectSlice(int newPosition) {
        byte[] bytes = randomBytes(1024);

        int pos = 256;
        int len = 512;

        ByteBuffer buf = ByteBuffer.wrap(bytes).position(pos).limit(pos + len).slice().position(newPosition);

        assertFalse(buf.isDirect());

        allocateNativeMemory(2048, addr -> {
            putByteBuffer(addr, 256, buf);

            assertArrayEquals(
                    Arrays.copyOfRange(bytes, pos + newPosition, pos + len),
                    getBytes(addr, 256, len - newPosition)
            );
        });
    }

    @ParameterizedTest(name = "position = {0}")
    @ValueSource(ints = {0, 128})
    void testPutByteBufferDirect(int position) {
        final byte[] bytes = randomBytes(1024);

        ByteBuffer buf = ByteBuffer.allocateDirect(1024).put(bytes).position(position);

        assertTrue(buf.isDirect());

        allocateNativeMemory(2048, addr -> {
            putByteBuffer(addr, 256, buf);

            assertArrayEquals(
                    Arrays.copyOfRange(bytes, position, bytes.length),
                    getBytes(addr, 256, bytes.length - position)
            );
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
}
