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

package org.apache.ignite.client.proto;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
    public void testPackerIncludesFourByteMessageLength() throws IOException {
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
    public void testUUID() throws IOException {
        testUUID(UUID.randomUUID());
        testUUID(new UUID(0, 0));
    }

    @Test
    public void testNumber() throws IOException {
        testNumber(BigInteger.ZERO);
        testNumber(BigInteger.valueOf(Long.MIN_VALUE));
        testNumber(BigInteger.valueOf(Long.MAX_VALUE));

        testNumber(new BigInteger(randomBytes(rnd, 100)));
        testNumber(new BigInteger(randomBytes(rnd, 250)));
        testNumber(new BigInteger(randomBytes(rnd, 1000)));
    }

    @Test
    public void testDecimal() throws IOException {
        testDecimal(BigDecimal.ZERO);
        testDecimal(BigDecimal.valueOf(Long.MIN_VALUE));
        testDecimal(BigDecimal.valueOf(Long.MAX_VALUE));

        testDecimal(new BigDecimal(new BigInteger(randomBytes(rnd, 100)), 50));
        testDecimal(new BigDecimal(new BigInteger(randomBytes(rnd, 250)), 200));
        testDecimal(new BigDecimal(new BigInteger(randomBytes(rnd, 1000)), 500));
    }

    @Test
    public void testBitSet() throws IOException {
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

    private void testUUID(UUID u) throws IOException {
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
}
