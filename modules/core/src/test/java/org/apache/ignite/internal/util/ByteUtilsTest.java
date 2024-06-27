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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.util.ByteUtils.bytesToIntKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.intToBytesKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.stringFromBytes;
import static org.apache.ignite.internal.util.ByteUtils.stringToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.ByteUtils.toByteArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

/** For {@link ByteUtils} testing. */
public class ByteUtilsTest {
    @Test
    void testStringToBytes() {
        assertNull(stringToBytes(null));

        assertArrayEquals("".getBytes(UTF_8), stringToBytes(""));
        assertArrayEquals("abc".getBytes(UTF_8), stringToBytes("abc"));
    }

    @Test
    void testStringFromBytes() {
        assertNull(stringFromBytes(null));

        assertEquals("", stringFromBytes(stringToBytes("")));
        assertEquals("abc", stringFromBytes(stringToBytes("abc")));
    }

    @Test
    void testBytesToLongKeepingOrder() {
        assertEquals(1234567890L, bytesToLongKeepingOrder(longToBytesKeepingOrder(1234567890L)));
    }

    @Test
    void testBytesToLongKeepingOrderComparison() {
        for (int i = 0; i < Long.SIZE - 4; i++) {
            ByteBuffer bufLo = ByteBuffer.wrap(longToBytesKeepingOrder(3L << i));
            ByteBuffer bufHi = ByteBuffer.wrap(longToBytesKeepingOrder(3L << (i + 1)));

            assertThat(bufLo, lessThan(bufHi));
        }
    }

    @Test
    void testBytesToLongKeepingOrderLongOverflow() {
        ByteBuffer bufLo = ByteBuffer.wrap(longToBytesKeepingOrder(Long.MIN_VALUE));
        ByteBuffer bufHi = ByteBuffer.wrap(longToBytesKeepingOrder(Long.MAX_VALUE));

        assertThat(bufLo, lessThan(bufHi));
    }

    @Test
    void testBytesToIntKeepingOrder() {
        assertEquals(1234567890, bytesToIntKeepingOrder(intToBytesKeepingOrder(1234567890)));
    }

    @Test
    void testBytesToIntKeepingOrderComparison() {
        for (int i = 0; i < Integer.SIZE - 4; i++) {
            ByteBuffer bufLo = ByteBuffer.wrap(intToBytesKeepingOrder(3 << i));
            ByteBuffer bufHi = ByteBuffer.wrap(intToBytesKeepingOrder(3 << (i + 1)));

            assertThat(bufLo, lessThan(bufHi));
        }
    }

    @Test
    void testBytesToIntKeepingOrderLongOverflow() {
        ByteBuffer bufLo = ByteBuffer.wrap(intToBytesKeepingOrder(Integer.MIN_VALUE));
        ByteBuffer bufHi = ByteBuffer.wrap(intToBytesKeepingOrder(Integer.MAX_VALUE));

        assertThat(bufLo, lessThan(bufHi));
    }

    @Test
    void testToByteArray() {
        assertArrayEquals(new byte[0], toByteArray(ByteBuffer.allocate(0)));
        assertArrayEquals(new byte[0], toByteArray(ByteBuffer.wrap(new byte[0])));

        assertArrayEquals(new byte[1], toByteArray(ByteBuffer.allocate(1)));
        assertArrayEquals(new byte[1], toByteArray(ByteBuffer.wrap(new byte[1])));

        assertArrayEquals(new byte[]{9, 7}, toByteArray(ByteBuffer.allocate(2).put((byte) 9).put((byte) 7).flip()));
        assertArrayEquals(new byte[]{9, 7}, toByteArray(ByteBuffer.wrap(new byte[]{9, 7})));

        assertArrayEquals(
                new byte[]{9, 7},
                toByteArray(ByteBuffer.wrap(new byte[]{6, 9, 7}).position(1))
        );
    }

    @Test
    void testToByteArrayList() {
        assertEquals(List.of(), toByteArrayList(List.of()));

        List<byte[]> bytes = toByteArrayList(List.of(ByteBuffer.allocate(2).put((byte) 6).put((byte) 8).flip()));

        assertEquals(1, bytes.size());
        assertArrayEquals(new byte[]{6, 8}, bytes.get(0));
    }
}
