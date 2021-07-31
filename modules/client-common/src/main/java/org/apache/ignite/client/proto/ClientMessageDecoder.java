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

import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.CharsetUtil;
import org.apache.ignite.lang.IgniteException;

/**
 * Decodes full client messages:
 * 1. MAGIC for first message.
 * 2. Payload length (varint).
 * 3. Payload (bytes).
 */
public class ClientMessageDecoder extends LengthFieldBasedFrameDecoder {
    /** Magic bytes before handshake. */
    public static final byte[] MAGIC_BYTES = new byte[]{0x49, 0x47, 0x4E, 0x49}; // IGNI

    /** Message header size. */
    public static final int HEADER_SIZE = 4;

    /** Data buffer. */
    private final byte[] data = new byte[4]; // TODO: Pooled buffers IGNITE-15162.

    /** Magic decoded flag. */
    private boolean magicDecoded;

    /** Magic decoding failed flag. */
    private boolean magicFailed;

    public ClientMessageDecoder() {
        super(Integer.MAX_VALUE - HEADER_SIZE, 0, HEADER_SIZE, 0, HEADER_SIZE, true);
    }

    /** {@inheritDoc} */
    @Override protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (!readMagic(in))
            return null;

        return super.decode(ctx, in);
    }

    /**
     * Checks the magic header for the first message.
     *
     * @param byteBuf Buffer.
     * @return {@code true} when magic header has been received and is valid, {@code false} otherwise.
     * @throws IgniteException When magic is invalid.
     */
    private boolean readMagic(ByteBuf byteBuf) {
        if (magicFailed)
            return false;

        if (magicDecoded)
            return true;

        if (byteBuf.readableBytes() < MAGIC_BYTES.length)
            return false;

        assert data.length == MAGIC_BYTES.length;

        byteBuf.readBytes(data, 0, MAGIC_BYTES.length);

        magicDecoded = true;

        if (Arrays.equals(data, MAGIC_BYTES))
            return true;

        magicFailed = true;

        throw new IgniteException("Invalid magic header in thin client connection. " +
                "Expected 'IGNI', but was '" + new String(data, CharsetUtil.US_ASCII) + "'.");
    }
}
