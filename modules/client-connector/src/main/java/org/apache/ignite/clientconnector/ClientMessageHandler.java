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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.msgpack.core.MessagePack;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.BitSet;

/**
 * https://netty.io/wiki/user-guide-for-4.x.html
 */
public class ClientMessageHandler extends ChannelInboundHandlerAdapter {
    private static final int MESSAGE_TYPE_RESPONSE = 0;

    private static final int MESSAGE_TYPE_NOTIFICATION = 1;

    private static final int ERROR_CODE_SUCCESS = 0;

    private static final int ERROR_CODE_GENERIC = 1;

    private final Logger log;

    private ClientContext clientContext;

    public ClientMessageHandler(Logger log) {
        this.log = log;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
        var buf = (byte[]) msg;

        // TODO: Pooled or cached packer/unpacker.
        var unpacker = MessagePack.newDefaultUnpacker(buf);
        var packer = MessagePack.newDefaultBufferPacker();

        if (clientContext == null) {
            var major = unpacker.unpackInt();
            var minor = unpacker.unpackInt();
            var patch = unpacker.unpackInt();
            var clientCode = unpacker.unpackInt();
            var featuresLen = unpacker.unpackBinaryHeader();
            var features = BitSet.valueOf(unpacker.readPayload(featuresLen));

            clientContext = new ClientContext(major, minor, patch, clientCode, features);

            log.debug("Handshake: " + clientContext);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValue(extensionsLen);

            // Response.
            packer.writePayload(new byte[]{0x49, 0x47, 0x4E, 0x49}); // Magic. // TODO: Encoder should be responsible for this?
            packer.packInt(7); // Length.

            // TODO: Protocol version check.
            packer.packInt(3); // Major.
            packer.packInt(0); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(ERROR_CODE_SUCCESS); // Success.

            packer.packBinaryHeader(0); // Features.
            packer.packMapHeader(0); // Extensions.
        } else {
            var opCode = unpacker.unpackInt();
            var requestId = unpacker.unpackInt();

            packer.packInt(MESSAGE_TYPE_RESPONSE);
            packer.packInt(requestId);

            // TODO: Handle operations asynchronously.
            // TODO: Catch errors.
            switch (opCode) {
                case 3: // TABLES_GET
                    packer.packInt(ERROR_CODE_SUCCESS); // Success.
                    packer.packInt(0); // 0 tables.
                    // TODO: Wrapper around MsgPack with our custom types (UUID, dates and times).
                    break;

                default:
                    packer.packInt(1); // Error.
                    packer.packString("Unexpected operation code: " + opCode);
            }
        }

        // TODO: Pooled buffers.
        ByteBuf response = Unpooled.copiedBuffer(packer.toByteArray());

        ctx.write(response);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // TODO: Logging
        cause.printStackTrace();
        ctx.close();
    }
}
