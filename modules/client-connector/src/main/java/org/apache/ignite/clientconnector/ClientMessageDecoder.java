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

package org.apache.ignite.clientconnector;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;
import org.apache.ignite.lang.IgniteException;

import java.util.List;

/**
 * Decodes full client messages.
 */
class ClientMessageDecoder extends ByteToMessageDecoder {
    /** Magic bytes before handshake. */
    private static final String MAGIC = "IGNI";

    /** */
    private byte[] data = new byte[4]; // TODO: Pooled buffers.

    /** */
    private int cnt = -4;

    /** */
    private int msgSize;

    /** */
    private boolean magicDecoded;

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) {
        if (!readMagic(byteBuf))
            return;

        while (read(byteBuf))
            list.add(data);
    }

    private boolean readMagic(ByteBuf byteBuf) {
        if (magicDecoded) {
            return true;
        }

        for (; cnt < 0 && byteBuf.readableBytes() > 0; cnt++)
            msgSize |= (byteBuf.readByte() & 0xFF) << (8 * (4 + cnt));

        if (cnt < 0)
            return false;

        magicDecoded = true;
        cnt = -1;
        msgSize = 0;

        var magic = new String(data, CharsetUtil.US_ASCII);

        if (MAGIC.equals(magic))
            return true;

        throw new IgniteException("Invalid magic header in thin client connection. " +
                "Expected 'IGNI', but was '" + magic + "'");
    }

    /**
     * Reads the buffer.
     *
     * @param buf Buffer.
     * @return True when a complete message has been received; false otherwise.
     */
    private boolean read(ByteBuf buf) {
        if (cnt < 0) {
            for (; cnt < 0 && buf.readableBytes() > 0; cnt++)
                msgSize |= (buf.readByte() & 0xFF) << (8 * (4 + cnt));

            if (cnt < 0)
                return false;

            data = new byte[msgSize];
        }

        assert data != null;
        assert cnt >= 0;
        assert msgSize > 0;

        int remaining = buf.readableBytes();

        if (remaining > 0) {
            int missing = msgSize - cnt;

            if (missing > 0) {
                int len = Math.min(missing, remaining);

                buf.readBytes(data, cnt, len);

                cnt += len;
            }
        }

        if (cnt == msgSize) {
            // TODO: MsgPack size can be variable.
            cnt = -1;
            msgSize = 0;

            return true;
        }

        return false;
    }
}
