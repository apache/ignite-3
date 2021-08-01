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
import java.util.UUID;

import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests Ignite-specific MsgPack extensions.
 */
public class ClientMessagePackerUnpackerTest {
    @Test
    public void testPackerCloseReleasesPooledBuffer() {
        var packer = new ClientMessagePacker();
        var buf = packer.getBuffer();

        packer.close();

        try (var packer2 = new ClientMessagePacker()) {
            var buf2 = packer2.getBuffer();

            assertSame(buf, buf2);
        }
    }

    @Test
    public void testUUID() throws IOException {
        testUUID(UUID.randomUUID());
        testUUID(new UUID(0, 0));
    }

    private void testUUID(UUID u) throws IOException {
        try (var packer = new ClientMessagePacker()) {
            packer.packUuid(u);

            var buf = packer.getBuffer();
            buf.readInt(); // Skip message length.

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                var res = unpacker.unpackUuid();

                assertEquals(u, res);
            }
        }
    }
}
