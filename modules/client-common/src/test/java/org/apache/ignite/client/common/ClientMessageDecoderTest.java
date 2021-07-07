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

package org.apache.ignite.client.common;

import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Message decoding tests.
 */
public class ClientMessageDecoderTest {
    /** Magic bytes. */
    private static final byte[] MAGIC = new byte[]{0x49, 0x47, 0x4E, 0x49};

    @Test
    void testEmptyBufferReturnsNoResults() throws Exception {

        var buf = new byte[0];
        var res = new ArrayList<>();

        new ClientMessageDecoder().decode(null, Unpooled.wrappedBuffer(buf), res);

        assertEquals(0, res.size());
    }

    @Test
    void testValidMagicAndMessageReturnsPayload() throws Exception {
        var decoder = new ClientMessageDecoder();

        var buf = new byte[7];

        // Magic.
        System.arraycopy(MAGIC, 0, buf, 0, 4);

        // Message size.
        buf[4] = 2;

        // Payload.
        buf[5] = 33;
        buf[6] = 44;

        var res = new ArrayList<>();
        decoder.decode(null, Unpooled.wrappedBuffer(buf), res);

        assertEquals(1, res.size());

        var resBuf = (byte[])res.get(0);
        assertArrayEquals(new byte[]{33, 44}, resBuf);
    }
}
