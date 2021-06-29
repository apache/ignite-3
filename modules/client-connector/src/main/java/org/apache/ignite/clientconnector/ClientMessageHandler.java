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
import org.apache.ignite.app.Ignite;
import org.apache.ignite.table.Table;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

/**
 * https://netty.io/wiki/user-guide-for-4.x.html
 */
public class ClientMessageHandler extends ChannelInboundHandlerAdapter {
    private final Logger log;

    private final Ignite ignite;

    private ClientContext clientContext;

    public ClientMessageHandler(Ignite ignite, Logger log) {
        assert ignite != null;
        assert log != null;

        this.ignite = ignite;
        this.log = log;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
        var buf = (byte[]) msg;

        // TODO: Pooled or cached packer/unpacker.
        var unpacker = getUnpacker(buf);
        var packer = getPacker();

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

            packer.packInt(ClientErrorCode.SUCCESS);

            packer.packBinaryHeader(0); // Features.
            packer.packMapHeader(0); // Extensions.
        } else {
            var opCode = unpacker.unpackInt();
            var requestId = unpacker.unpackInt();

            packer.packInt(ClientMessageType.RESPONSE);
            packer.packInt(requestId);

            processOperation(packer, opCode);
        }

        // TODO: Pooled buffers.
        ByteBuf response = Unpooled.copiedBuffer(packer.toByteArray());

        ctx.write(response);
    }

    private ClientMessagePacker getPacker() {
        return new ClientMessagePacker();
    }

    private ClientMessageUnpacker getUnpacker(byte[] buf) {
        return new ClientMessageUnpacker(new ArrayBufferInput(buf), MessagePack.DEFAULT_UNPACKER_CONFIG);
    }

    private void processOperation(org.msgpack.core.MessageBufferPacker packer, int opCode) throws IOException {
        // TODO: Handle operations asynchronously.
        try {
            switch (opCode) {
                case ClientOp.TABLE_CREATE: {
                    break;
                }

                case ClientOp.TABLE_DROP: {
                    break;
                }

                case ClientOp.TABLES_GET: {
                    List<Table> tables = ignite.tables().tables();

                    packer.packInt(ClientErrorCode.SUCCESS);
                    packer.packInt(tables.size()); // 0 tables.
                    // TODO: Wrapper around MsgPack with our custom types (UUID, dates and times).
                    break;
                }

                default:
                    packer.packInt(ClientErrorCode.GENERIC);
                    packer.packString("Unexpected operation code: " + opCode);
            }
        } catch (Throwable t) {
            // TODO: Seek back to the start of the message.
            packer.packInt(ClientErrorCode.GENERIC);
            packer.packString("Internal server error: " + t.getMessage());
        }
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
