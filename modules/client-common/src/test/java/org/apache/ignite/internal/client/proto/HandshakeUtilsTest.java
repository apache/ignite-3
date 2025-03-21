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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.BitSet;
import org.junit.jupiter.api.Test;

/**
 * Tests for handshake utils.
 */
public class HandshakeUtilsTest {
    @Test
    public void testUnpackExtensionsSkipsUnknown() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packInt(5);

            packer.packString("unknown-key-1");
            packer.packInt(1);

            packer.packString(HandshakeExtension.AUTHENTICATION_SECRET.key());
            packer.packString("secret");

            packer.packString("unknown-key-2");
            packer.packBinary(new byte[] {1, 2, 3});

            packer.packString(HandshakeExtension.AUTHENTICATION_IDENTITY.key());
            packer.packString("identity");

            packer.packString("unknown-key-3");
            packer.packString("value");

            ByteBuf buf = packer.getBuffer().skipBytes(ClientMessageCommon.HEADER_SIZE);
            try (var unpacker = new ClientMessageUnpacker(buf)) {
                var extensions = HandshakeUtils.unpackExtensions(unpacker);

                assertEquals(2, extensions.size());
                assertEquals("secret", extensions.get(HandshakeExtension.AUTHENTICATION_SECRET));
                assertEquals("identity", extensions.get(HandshakeExtension.AUTHENTICATION_IDENTITY));
            }
        }
    }

    @Test
    public void supportedFeaturesIntersect() {
        BitSet supported = new BitSet();
        supported.set(1);
        supported.set(199);
        supported.set(58);
        supported.set(42);

        BitSet asked = new BitSet();
        asked.set(42);
        asked.set(887);
        asked.set(58);

        BitSet result = HandshakeUtils.supportedFeatures(supported, asked);
        BitSet expected = new BitSet();
        expected.set(42);
        expected.set(58);
        assertEquals(expected, result);

        // Sanity check, checks that first argument is not modified
        assertNotSame(supported, expected);
    }

    @Test
    public void supportedFeaturesAreMutuallyExclusive() {
        BitSet supported = new BitSet();
        supported.set(1);
        supported.set(199);
        supported.set(87);

        BitSet asked = new BitSet();
        asked.set(887);
        asked.set(58);

        BitSet result = HandshakeUtils.supportedFeatures(supported, asked);
        BitSet expected = new BitSet();
        assertEquals(expected, result);

        // Sanity check, checks that first argument is not modified
        assertNotSame(supported, expected);
    }
}
