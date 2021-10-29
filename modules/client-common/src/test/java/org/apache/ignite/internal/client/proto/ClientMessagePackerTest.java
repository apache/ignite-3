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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests Ignite ByteBuf-based packer against third-party library implementation to ensure identical results.
 */
public class ClientMessagePackerTest {
    @Test
    public void testPackNil() {
        testPacker(ClientMessagePacker::packNil, MessagePacker::packNil);
    }

    @ParameterizedTest
    @ValueSource(bytes = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE})
    public void testPackByte(byte b) {
        testPacker(p -> p.packByte(b), p -> p.packByte(b));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE})
    public void testPackShort(short s) {
        testPacker(p -> p.packShort(s), p -> p.packShort(s));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE})
    public void testPackInt(int i) {
        testPacker(p -> p.packInt(i), p -> p.packInt(i));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE})
    public void testPackLong(long l) {
        testPacker(p -> p.packLong(l), p -> p.packLong(l));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE})
    public void testPackBigInteger(long l) {
        var bi = BigInteger.valueOf(l);
        testPacker(p -> p.packBigInteger(bi), p -> p.packBigInteger(bi));
    }

    @Test
    public void testPackBigIntegerThrowsOnTooLargeValues() {
        var bi = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN);

        assertThrows(IllegalArgumentException.class, () -> packIgnite(p -> p.packBigInteger(bi)));
    }

    private static void testPacker(Consumer<ClientMessagePacker> pack1, MessagePackerConsumer pack2) {
        var bytesIgnite = packIgnite(pack1);
        var bytesLibrary = packLibrary(pack2);

        assertArrayEquals(bytesLibrary, bytesIgnite);
    }

    private static byte[] packIgnite(Consumer<ClientMessagePacker> pack) {
        try (var packer = new ClientMessagePacker(Unpooled.buffer())) {
            pack.accept(packer);

            ByteBuf buf = packer.getBuffer();

            byte[] arr = buf.array();
            var offset = buf.arrayOffset() + ClientMessageCommon.HEADER_SIZE;
            var size = buf.writerIndex() - ClientMessageCommon.HEADER_SIZE;

            return Arrays.copyOfRange(arr, offset, offset + size);
        }
    }

    private static byte[] packLibrary(MessagePackerConsumer pack) {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            pack.accept(packer);

            return packer.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private interface MessagePackerConsumer {
        void accept(MessagePacker p) throws IOException;
    }
}
