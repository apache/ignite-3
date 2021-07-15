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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.ClientErrorCode;
import org.apache.ignite.client.ClientMessageDecoder;
import org.apache.ignite.client.ClientMessagePacker;
import org.apache.ignite.client.ClientMessageType;
import org.apache.ignite.client.ClientMessageUnpacker;
import org.apache.ignite.client.ClientOp;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TupleBuilderImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.buffer.ArrayBufferInput;
import org.msgpack.core.buffer.ByteBufferInput;
import org.msgpack.value.ImmutableMapValue;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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

    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
        var buf = (ByteBuffer) msg;

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

            processOperation(unpacker, packer, opCode);
        }

        ByteBuffer response = packer.toMessageBuffer().sliceAsByteBuffer();

        ctx.write(response);
    }

    private ClientMessagePacker getPacker() {
        // TODO: Pooling
        return new ClientMessagePacker();
    }

    private ClientMessageUnpacker getUnpacker(ByteBuffer buf) {
        // TODO: Pooling
        return new ClientMessageUnpacker(new ByteBufferInput(buf));
    }

    private void processOperation(ClientMessageUnpacker unpacker, ClientMessagePacker packer, int opCode) throws IOException {
        // TODO: Handle operations asynchronously.
        var op = ClientOp.fromOrdinal(opCode);

        if (op == null) {
            packer.packInt(ClientErrorCode.FAILED);
            packer.packString("Unexpected operation code: " + opCode);
            return;
        }

        try {
            switch (op) {
                case TABLE_CREATE: {
                    TableImpl table = createTable(unpacker);

                    packer.packUuid(table.tableId());

                    break;
                }

                case TABLE_DROP: {
                    var tableName = unpacker.unpackString();

                    ignite.tables().dropTable(tableName);

                    break;
                }

                case TABLES_GET: {
                    List<Table> tables = ignite.tables().tables();

                    packer.packInt(tables.size());

                    for (var table : tables) {
                        var tableImpl = (TableImpl) table;

                        packer.packUuid(tableImpl.tableId());
                        packer.packString(table.tableName());
                    }

                    break;
                }

                case TABLE_GET: {
                    String tableName = unpacker.unpackString();
                    Table table = ignite.tables().table(tableName);

                    if (table == null)
                        packer.packNil();
                    else
                        packer.packUuid(((TableImpl)table).tableId());

                    break;
                }

                case TUPLE_UPSERT: {
                    // TODO: Benchmark schema approach vs map approach (devlist) - both get and put operations.
                    // TUPLE_UPSERT vs TUPLE_UPSERT_SCHEMALESS
                    var table = readTable(unpacker);
                    var tuple = readTuple(unpacker, table);

                    table.upsert(tuple);

                    break;
                }

                case TUPLE_UPSERT_SCHEMALESS: {
                    var table = readTable(unpacker);
                    var tuple = readTupleSchemaless(unpacker, table);

                    table.upsert(tuple);

                    break;
                }

                case TUPLE_GET: {
                    var table = readTable(unpacker);
                    var keyTuple = readTuple(unpacker, table);

                    // TODO: getAsync
                    Tuple tuple = table.get(keyTuple);
                    writeTuple(packer, tuple);

                    break;
                }

                default:
                    packer.packInt(ClientErrorCode.FAILED);
                    packer.packString("Unexpected operation code: " + opCode);
            }
        } catch (Throwable t) {
            // TODO: Seek back to the start of the message.
            packer.packInt(ClientErrorCode.FAILED);
            packer.packString("Internal server error: " + t.getMessage());
        }
    }

    private void writeTuple(ClientMessagePacker packer, Tuple tuple) throws IOException {
        var schema = ((SchemaAware) tuple).schema();

        packer.packInt(schema.version());
        packer.packArrayHeader(schema.length());

        for (var col : schema.keyColumns().columns()) {
            var val = tuple.value(col.name());

            if (val == null) {
                packer.packNil();
                continue;
            }

            // TODO: Switch on type or on column type?
            packer.packInt((int)val);
        }
    }

    private Tuple readTuple(ClientMessageUnpacker unpacker, TableImpl table) throws IOException {
        var schemaId = unpacker.unpackInt();
        var cnt = unpacker.unpackArrayHeader();

        var schema = table.schemaView().schema(schemaId);

        if (cnt > schema.length())
            throw new IgniteException(
                "Incorrect number of tuple values. Expected: " + schema.length() + ", but got: " + cnt);

        var builder = table.tupleBuilderInternal();

        for (int i = 0; i < cnt; i++) {
            if (unpacker.getNextFormat() == MessageFormat.NIL) {
                unpacker.skipValue();
                continue;
            }

            readAndSetColumnValue(unpacker, builder, schema.column(i));
        }

        return builder.build();
    }

    private Tuple readTupleSchemaless(ClientMessageUnpacker unpacker, TableImpl table) throws IOException {
        var cnt = unpacker.unpackMapHeader();
        var builder = table.tupleBuilderInternal();

        for (int i = 0; i < cnt; i++) {
            var colName = unpacker.unpackString();

            builder.set(colName, unpacker.unpackValue());
        }

        return builder.build();
    }

    private TableImpl readTable(ClientMessageUnpacker unpacker) throws IOException {
        var tableId = unpacker.unpackUuid();
        return ((TableManager)ignite.tables()).table(tableId, true);
    }

    private void readAndSetColumnValue(ClientMessageUnpacker unpacker, TupleBuilderImpl builder, Column col)
            throws IOException {
        switch (col.type().spec()) {
            case INT8:
                builder.set(col, unpacker.unpackByte());
                break;

            case INT16:
                builder.set(col, unpacker.unpackShort());
                break;

            case INT32:
                builder.set(col, unpacker.unpackInt());
                break;

            case INT64:
                builder.set(col, unpacker.unpackLong());
                break;

            case FLOAT:
                builder.set(col, unpacker.unpackFloat());
                break;

            case DOUBLE:
                builder.set(col, unpacker.unpackDouble());
                break;

            case DECIMAL:
                throw new UnsupportedOperationException("TODO");

            case UUID:
                builder.set(col, unpacker.unpackUuid());
                break;

            case STRING:
                builder.set(col, unpacker.unpackString());
                break;

            case BYTES:
                builder.set(col, unpacker.readPayload(unpacker.unpackBinaryHeader()));
                break;

            case BITMASK:
                throw new UnsupportedOperationException("TODO");

            default:
                throw new UnsupportedOperationException("Unsupported data type: " + col.type().spec());
        }
    }

    private TableImpl createTable(ClientMessageUnpacker unpacker) throws IOException {
        var size = unpacker.unpackMapHeader();
        String name = null;
        var settings = new Object() {
            Integer replicas = null;
            Integer partitions = null;
            Map<String, ImmutableMapValue> columns = null;
        };

        for (int i = 0; i < size; i++) {
            var key = unpacker.unpackString();

            switch (key) {
                case "name":
                    name = unpacker.unpackString();
                    break;

                case "replicas":
                    settings.replicas = unpacker.unpackInt();
                    break;

                case "partitions":
                    settings.partitions = unpacker.unpackInt();
                    break;

                case "columns":
                    var colCnt = unpacker.unpackMapHeader();
                    settings.columns = new HashMap<>(colCnt);
                    for (int j = 0; j < colCnt; j++) {
                        var colName = unpacker.unpackString();
                        var colSettings = unpacker.unpackValue().asMapValue();

                        settings.columns.put(colName, colSettings);
                    }
            }
        }

        Consumer<TableChange> tableChangeConsumer = tbl -> {
            if (settings.replicas != null)
                tbl.changeReplicas(settings.replicas);

            if (settings.partitions != null)
                tbl.changePartitions(settings.partitions);

            if (settings.columns != null) {
                for (var col : settings.columns.entrySet()) {
                    tbl.changeColumns(c -> c.create(col.getKey(), cc ->
                            cc
                                    .changeName(col.getKey())
                                    .changeType(t -> t.changeType(col.getValue().map().get("type").toString()))));
                }
            }
        };

        return (TableImpl)ignite.tables().createTable(name, tableChangeConsumer);
    }

    @Override public void channelReadComplete(ChannelHandlerContext ctx) {
        // TODO: ???
        ctx.flush();
        // ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // TODO: Logging
        cause.printStackTrace();
        ctx.close();
    }
}
