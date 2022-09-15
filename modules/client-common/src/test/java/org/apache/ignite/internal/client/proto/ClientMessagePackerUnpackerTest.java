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

package org.apache.ignite.internal.client.proto;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests Ignite-specific MsgPack extensions.
 */
public class ClientMessagePackerUnpackerTest {
    /** Random. */
    private final Random rnd = new Random();

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
    public void testNumber() {
        testNumber(BigInteger.ZERO);
        testNumber(BigInteger.valueOf(Long.MIN_VALUE));
        testNumber(BigInteger.valueOf(Long.MAX_VALUE));

        testNumber(new BigInteger(randomBytes(rnd, 100)));
        testNumber(new BigInteger(randomBytes(rnd, 250)));
        testNumber(new BigInteger(randomBytes(rnd, 1000)));
    }

    private void testNumber(BigInteger val) {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packNumber(val);

            var buf = packer.getBuffer();
            //noinspection unused
            var len = buf.readInt();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                var res = unpacker.unpackNumber();

                assertEquals(val, res);
            }
        }
    }

    @Test
    public void testDecimal() {
        testDecimal(BigDecimal.ZERO);
        testDecimal(BigDecimal.valueOf(Long.MIN_VALUE));
        testDecimal(BigDecimal.valueOf(Long.MAX_VALUE));

        testDecimal(new BigDecimal(new BigInteger(randomBytes(rnd, 100)), 50));
        testDecimal(new BigDecimal(new BigInteger(randomBytes(rnd, 250)), 200));
        testDecimal(new BigDecimal(new BigInteger(randomBytes(rnd, 1000)), 500));
    }

    private void testDecimal(BigDecimal val) {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packDecimal(val);

            var buf = packer.getBuffer();
            //noinspection unused
            var len = buf.readInt();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                var res = unpacker.unpackDecimal();

                assertEquals(val, res);
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
            //noinspection unused
            var len = buf.readInt();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                var res = unpacker.unpackBitSet();

                assertEquals(val, res);
            }
        }
    }

    @Test
    public void testTemporalTypes() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            LocalDate date = LocalDate.now();
            LocalTime time = LocalTime.now();
            Instant timestamp = Instant.now();

            packer.packDate(date);
            packer.packTime(time);
            packer.packTimestamp(timestamp);
            packer.packDateTime(LocalDateTime.of(date, time));

            var buf = packer.getBuffer();
            //noinspection unused
            var len = buf.readInt();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                assertEquals(date, unpacker.unpackDate());
                assertEquals(time, unpacker.unpackTime());
                assertEquals(timestamp, unpacker.unpackTimestamp());
                assertEquals(LocalDateTime.of(date, time), unpacker.unpackDateTime());
            }
        }
    }

    @Test
    public void testVariousTypesSupport() {
        Object[] values = new Object[]{
                (byte) 1, (short) 2, 3, 4L, 5.5f, 6.6d,
                BigDecimal.valueOf(rnd.nextLong()),
                UUID.randomUUID(),
                IgniteTestUtils.randomString(rnd, 11),
                IgniteTestUtils.randomBytes(rnd, 22),
                IgniteTestUtils.randomBitSet(rnd, 33),
                LocalDate.now(),
                LocalTime.now(),
                LocalDateTime.now(),
                Instant.now()
        };

        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            for (Object val : values) {
                packer.packObject(val);
            }

            var buf = packer.getBuffer();
            //noinspection unused
            var len = buf.readInt();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                for (int i = 0; i < values.length; i++) {
                    if (values[i] instanceof byte[]) {
                        assertArrayEquals((byte[]) values[i], (byte[]) unpacker.unpackObject(i + 1));
                    } else {
                        assertEquals(values[i], unpacker.unpackObject(i + 1));
                    }
                }
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
    public void testObjectArray() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            Object[] args = new Object[]{(byte) 4, (short) 8, 15, 16L, 23.0f, 42.0d, "TEST_STRING", null, UUID.randomUUID()};
            packer.packObjectArray(args);

            var buf = packer.getBuffer();

            byte[] data = new byte[buf.readableBytes()];

            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                unpacker.skipValues(4);
                Object[] res = unpacker.unpackObjectArray();
                assertArrayEquals(args, res);
            }
        }
    }

    @Test
    public void testObjectArrayLongValue() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            Object[] args = new Object[]{16L};
            packer.packObjectArray(args);

            var buf = packer.getBuffer();

            byte[] data = new byte[buf.readableBytes()];

            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                unpacker.skipValues(4);

                Object[] res = unpacker.unpackObjectArray();

                assertEquals(16L, (Long) res[0]);
            }
        }
    }

    @Test
    public void testNoValue() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packInt(1);
            packer.packNoValue();
            packer.packString("s");

            var buf = packer.getBuffer();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                unpacker.skipValues(4);

                assertFalse(unpacker.tryUnpackNoValue());
                assertEquals(1, unpacker.unpackInt());
                assertTrue(unpacker.tryUnpackNoValue());
                assertEquals("s", unpacker.unpackString());
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
            packer.packNoValue();
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

                assertEquals(-1, unpacker.tryUnpackInt(-1));
                assertTrue(unpacker.tryUnpackNoValue());

                assertEquals(-2, unpacker.tryUnpackInt(-2));
                assertEquals("s", unpacker.unpackString());
            }
        }
    }
}
