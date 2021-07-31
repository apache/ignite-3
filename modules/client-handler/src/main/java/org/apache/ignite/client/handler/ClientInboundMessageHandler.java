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

package org.apache.ignite.client.handler;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.handler.requests.table.ClientSchemasGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableDropRequest;
import org.apache.ignite.client.handler.requests.table.ClientTablesGetRequest;
import org.apache.ignite.client.proto.ClientDataType;
import org.apache.ignite.client.proto.ClientErrorCode;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.client.proto.ClientOp;
import org.apache.ignite.client.proto.ProtocolVersion;
import org.apache.ignite.client.proto.ServerMessageType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.msgpack.core.MessageFormat;
import org.slf4j.Logger;

/**
 * Handles messages from thin clients.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientInboundMessageHandler extends ChannelInboundHandlerAdapter {
    /** Logger. */
    private final Logger log;

    /** API entry point. */
    private final Ignite ignite;

    /** Context. */
    private ClientContext clientContext;

    /**
     * Constructor.
     *
     * @param ignite Ignite API entry point.
     * @param log Logger.
     */
    public ClientInboundMessageHandler(Ignite ignite, Logger log) {
        assert ignite != null;
        assert log != null;

        this.ignite = ignite;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
        var unpacker = getUnpacker((ByteBuf) msg);
        var packer = getPacker();

        if (clientContext == null)
            handshake(ctx, unpacker, packer);
        else
            processOperation(ctx, unpacker, packer);
    }

    private void handshake(ChannelHandlerContext ctx, ClientMessageUnpacker unpacker, ClientMessagePacker packer)
            throws IOException {
        try {
            var clientVer = ProtocolVersion.unpack(unpacker);

            if (!clientVer.equals(ProtocolVersion.LATEST_VER))
                throw new IgniteException("Unsupported version: " +
                        clientVer.major() + "." + clientVer.minor() + "." + clientVer.patch());

            var clientCode = unpacker.unpackInt();
            var featuresLen = unpacker.unpackBinaryHeader();
            var features = BitSet.valueOf(unpacker.readPayload(featuresLen));

            clientContext = new ClientContext(clientVer, clientCode, features);

            log.debug("Handshake: " + clientContext);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValue(extensionsLen);

            // Response.
            ProtocolVersion.LATEST_VER.pack(packer);

            packer.packInt(ClientErrorCode.SUCCESS)
                    .packBinaryHeader(0) // Features.
                    .packMapHeader(0); // Extensions.

            write(packer, ctx);
        }
        catch (Throwable t) {
            packer = getPacker();

            ProtocolVersion.LATEST_VER.pack(packer);
            packer.packInt(ClientErrorCode.FAILED).packString(t.getMessage());

            write(packer, ctx);
        }
    }

    private void write(ClientMessagePacker packer, ChannelHandlerContext ctx) {
        var buf = packer.toMessageBuffer().sliceAsByteBuffer();

        ctx.writeAndFlush(buf);
    }

    private void writeError(int requestId, Throwable err, ChannelHandlerContext ctx) {
        try {
            assert err != null;

            ClientMessagePacker packer = getPacker();
            packer.packInt(ServerMessageType.RESPONSE);
            packer.packInt(requestId);
            packer.packInt(ClientErrorCode.FAILED);

            String msg = err.getMessage();

            if (msg == null)
                msg = err.getClass().getName();

            packer.packString(msg);

            write(packer, ctx);
        }
        catch (Throwable t) {
            exceptionCaught(ctx, t);
        }
    }

    private ClientMessagePacker getPacker() {
        return new ClientMessagePacker();
    }

    private ClientMessageUnpacker getUnpacker(ByteBuf buf) {
        // TODO: Close objects - check if needed.
        // TODO: Is the buf pooled, and when it returns to the pool?
        return new ClientMessageUnpacker(buf);
    }

    private void processOperation(ChannelHandlerContext ctx, ClientMessageUnpacker unpacker, ClientMessagePacker packer) throws IOException {
        var opCode = unpacker.unpackInt();
        var requestId = unpacker.unpackInt();

        packer.packInt(ServerMessageType.RESPONSE)
                .packInt(requestId)
                .packInt(ClientErrorCode.SUCCESS);

        try {
            var fut = processOperation(unpacker, packer, opCode);

            if (fut == null) {
                // Operation completed synchronously.
                write(packer, ctx);
            }
            else {
                fut.whenComplete((Object res, Object err) -> {
                    if (err != null)
                        writeError(requestId, (Throwable) err, ctx);
                    else
                        write(packer, ctx);
                });
            }

        }
        catch (Throwable t) {
            writeError(requestId, t, ctx);
        }
    }

    private CompletableFuture processOperation(ClientMessageUnpacker in, ClientMessagePacker out, int opCode)
            throws IOException {
        // TODO: Handle all operations asynchronously (add async table API).
        switch (opCode) {
            case ClientOp.TABLE_DROP:
                return ClientTableDropRequest.process(in, out, ignite.tables());

            case ClientOp.TABLES_GET:
                return ClientTablesGetRequest.process(out, ignite.tables());

            case ClientOp.SCHEMAS_GET:
                return ClientSchemasGetRequest.process(in, out, ignite.tables());

            case ClientOp.TABLE_GET: {
                String tableName = in.unpackString();
                Table table = ignite.tables().table(tableName);

                if (table == null)
                    out.packNil();
                else
                    out.packUuid(((TableImpl) table).tableId());

                break;
            }

            case ClientOp.TUPLE_UPSERT: {
                var table = readTable(in);
                var tuple = readTuple(in, table, false);

                return table.upsertAsync(tuple);
            }

            case ClientOp.TUPLE_UPSERT_SCHEMALESS: {
                var table = readTable(in);
                var tuple = readTupleSchemaless(in, table);

                return table.upsertAsync(tuple);
            }

            case ClientOp.TUPLE_GET: {
                var table = readTable(in);
                var keyTuple = readTuple(in, table, true);

                return table.getAsync(keyTuple).thenAccept(t -> writeTuple(out, t));
            }

            default:
                throw new IgniteException("Unexpected operation code: " + opCode);
        }

        return null;
    }


    /** {@inheritDoc} */
    @Override public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    /** {@inheritDoc} */
    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);

        ctx.close();
    }
}
