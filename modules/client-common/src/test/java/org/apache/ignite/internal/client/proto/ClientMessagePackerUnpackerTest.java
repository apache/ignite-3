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

package org.apache.ignite.internal.client.proto;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests Ignite-specific MsgPack extensions.
 */
public class ClientMessagePackerUnpackerTest {
    /** Random. */
    private final Random rnd = new Random();

    /** Args of all types. */
    private final Object[] argsAllTypes = new Object[]{(byte) 4, (short) 8, 15, 16L, 23.0f, 42.0d, "TEST_STRING", null, UUID.randomUUID(),
            LocalTime.now(), LocalDate.now(), LocalDateTime.now(), Instant.now(), Period.of(1, 2, 3),
            Duration.of(1, ChronoUnit.DAYS)};

    @Test
    public void testPackerCloseReleasesPooledBuffer() {
        var buf = PooledByteBufAllocator.DEFAULT.directBuffer();
        var packer = new ClientMessagePacker(buf);

        assertEquals(1, buf.refCnt());

        packer.close();

        assertEquals(0, buf.refCnt());
    }

    @Test
    public void testPackerIncludesFourByteMessageLength() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packInt(1); // 1 byte
            packer.packString("Foo"); // 4 bytes

            var buf = packer.getBuffer();
            var len = buf.readInt();

            assertEquals(5, len);
            assertEquals(9, buf.writerIndex());
            assertEquals(Integer.MAX_VALUE, buf.maxCapacity());
        }
    }

    @Test
    public void testEmptyPackerReturnsFourZeroBytes() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            var buf = packer.getBuffer();
            var len = buf.readInt();

            assertEquals(0, len);
            assertEquals(4, buf.writerIndex());
        }
    }

    @Test
    public void testUuid() {
        testUuid(UUID.randomUUID());
        testUuid(new UUID(0, 0));
    }

    private void testUuid(UUID u) {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packUuid(u);

            var buf = packer.getBuffer();
            var len = buf.readInt();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                var res = unpacker.unpackUuid();

                assertEquals(18, len); // 1 ext + 1 ext type + 16 UUID data
                assertEquals(u, res);
            }
        }
    }

    @Test
    public void testBitSet() {
        testBitSet(BitSet.valueOf(new byte[0]));
        testBitSet(BitSet.valueOf(randomBytes(rnd, 1)));
        testBitSet(BitSet.valueOf(randomBytes(rnd, 100)));
        testBitSet(BitSet.valueOf(randomBytes(rnd, 1000)));
    }

    private void testBitSet(BitSet val) {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packBitSet(val);

            var buf = packer.getBuffer();
            buf.readInt();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                var res = unpacker.unpackBitSet();

                assertEquals(val, res);
            }
        }
    }

    @Test
    public void testIntegerArray() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            int[] arr = new int[]{4, 8, 15, 16, 23, 42};

            packer.packIntArray(arr);

            var buf = packer.getBuffer();

            byte[] data = new byte[buf.readableBytes()];

            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                unpacker.skipValues(4);
                int[] res = unpacker.unpackIntArray();
                assertArrayEquals(arr, res);
            }
        }
    }

    @Test
    public void testObjectArrayAsBinaryTuple() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packObjectArrayAsBinaryTuple(argsAllTypes);
            packer.packObjectArrayAsBinaryTuple(null);
            packer.packObjectArrayAsBinaryTuple(new Object[0]);

            byte[] data = ByteBufUtil.getBytes(packer.getBuffer());

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data, 4, data.length - 4))) {
                Object[] res1 = unpacker.unpackObjectArrayFromBinaryTuple();
                Object[] res2 = unpacker.unpackObjectArrayFromBinaryTuple();
                Object[] res3 = unpacker.unpackObjectArrayFromBinaryTuple();

                assertArrayEquals(argsAllTypes, res1);
                assertNull(res2);
                assertEquals(0, res3.length);
            }
        }
    }

    @Test
    public void testObjectAsBinaryTuple() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            for (Object arg : argsAllTypes) {
                packer.packObjectAsBinaryTuple(arg);
            }

            byte[] data = ByteBufUtil.getBytes(packer.getBuffer());

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data, 4, data.length - 4))) {
                for (Object arg : argsAllTypes) {
                    var res = unpacker.unpackObjectFromBinaryTuple();

                    assertEquals(arg, res);
                }
            }
        }
    }

    @Test
    public void testTryUnpackInt() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packInt(1);
            packer.packInt(Byte.MAX_VALUE);
            packer.packInt(Short.MAX_VALUE);
            packer.packInt(Integer.MAX_VALUE);
            packer.packString("s");

            var buf = packer.getBuffer();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                unpacker.skipValues(4);

                assertEquals(1, unpacker.tryUnpackInt(-1));
                assertEquals(Byte.MAX_VALUE, unpacker.tryUnpackInt(-1));
                assertEquals(Short.MAX_VALUE, unpacker.tryUnpackInt(-1));
                assertEquals(Integer.MAX_VALUE, unpacker.tryUnpackInt(-1));

                assertEquals(-2, unpacker.tryUnpackInt(-2));
                assertEquals("s", unpacker.unpackString());
            }
        }
    }
}
