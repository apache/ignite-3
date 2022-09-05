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

package org.apache.ignite.internal.client.table;

import static org.apache.ignite.internal.client.proto.ClientDataType.BIGINTEGER;
import static org.apache.ignite.internal.client.proto.ClientDataType.BITMASK;
import static org.apache.ignite.internal.client.proto.ClientDataType.BOOLEAN;
import static org.apache.ignite.internal.client.proto.ClientDataType.BYTES;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATE;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATETIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.DECIMAL;
import static org.apache.ignite.internal.client.proto.ClientDataType.DOUBLE;
import static org.apache.ignite.internal.client.proto.ClientDataType.FLOAT;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT16;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT32;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT64;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT8;
import static org.apache.ignite.internal.client.proto.ClientDataType.NUMBER;
import static org.apache.ignite.internal.client.proto.ClientDataType.STRING;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIMESTAMP;
import static org.apache.ignite.internal.client.proto.ClientMessageCommon.NO_VALUE;
import static org.apache.ignite.internal.client.table.ClientTable.writeTx;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.proto.ClientDataType;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tuple serializer.
 */
public class ClientTupleSerializer {
    /** Table ID. */
    private final UUID tableId;

    /**
     * Constructor.
     *
     * @param tableId Table id.
     */
    ClientTupleSerializer(UUID tableId) {
        this.tableId = tableId;
    }

    /**
     * Writes {@link Tuple}.
     *
     * @param tuple Tuple.
     * @param schema Schema.
     * @param out Out.
     */
    void writeTuple(
            @Nullable Transaction tx,
            @NotNull Tuple tuple,
            ClientSchema schema,
            PayloadOutputChannel out
    ) {
        writeTuple(tx, tuple, schema, out, false, false);
    }

    /**
     * Writes {@link Tuple}.
     *
     * @param tuple Tuple.
     * @param schema Schema.
     * @param out Out.
     * @param keyOnly Key only.
     */
    void writeTuple(
            @Nullable Transaction tx,
            @NotNull Tuple tuple,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean keyOnly
    ) {
        writeTuple(tx, tuple, schema, out, keyOnly, false);
    }

    /**
     * Writes {@link Tuple}.
     *
     * @param tuple Tuple.
     * @param schema Schema.
     * @param out Out.
     * @param keyOnly Key only.
     * @param skipHeader Skip header.
     */
    void writeTuple(
            @Nullable Transaction tx,
            @NotNull Tuple tuple,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean keyOnly,
            boolean skipHeader
    ) {
        if (!skipHeader) {
            out.out().packUuid(tableId);
            writeTx(tx, out);
            out.out().packInt(schema.version());
        }

        writeTupleRaw(tuple, schema, out, keyOnly);
    }

    /**
     * Writes {@link Tuple} without header.
     *
     * @param tuple Tuple.
     * @param schema Schema.
     * @param out Out.
     * @param keyOnly Key only.
     */
    public static void writeTupleRaw(@NotNull Tuple tuple, ClientSchema schema, PayloadOutputChannel out, boolean keyOnly) {
        var columns = schema.columns();
        var count = keyOnly ? schema.keyColumnCount() : columns.length;

        var builder = BinaryTupleBuilder.create(count, true);
        var noValueMask = new BitSet(count);

        for (var i = 0; i < count; i++) {
            var col = columns[i];
            Object v = tuple.valueOrDefault(col.name(), NO_VALUE);

            if (v == NO_VALUE) {
                builder.appendNull();
                noValueMask.set(i);
                continue;
            }

            appendValue(builder, col, v);
        }

        var buf = builder.build();

        out.out().packBitSet(noValueMask);
        out.out().packBinaryHeader(buf.limit() - buf.position()); // TODO IGNITE-17297: ???
        out.out().writePayload(buf);
    }

    /**
     * Writes key and value {@link Tuple}.
     *
     * @param key Key tuple.
     * @param val Value tuple.
     * @param schema Schema.
     * @param out Out.
     * @param skipHeader Skip header.
     */
    void writeKvTuple(
            @Nullable Transaction tx,
            @NotNull Tuple key,
            @Nullable Tuple val,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean skipHeader
    ) {
        if (!skipHeader) {
            out.out().packUuid(tableId);
            writeTx(tx, out);
            out.out().packInt(schema.version());
        }

        var columns = schema.columns();
        var noValueSet = new BitSet(columns.length);
        out.out().packBitSet(noValueSet);

        for (var i = 0; i < columns.length; i++) {
            var col = columns[i];

            Object v = col.key()
                    ? key.valueOrDefault(col.name(), NO_VALUE)
                    : val != null
                            ? val.valueOrDefault(col.name(), NO_VALUE)
                            : NO_VALUE;

            out.out().packObject(v);
        }
    }

    /**
     * Writes pairs {@link Tuple}.
     *
     * @param pairs Key tuple.
     * @param schema Schema.
     * @param out Out.
     */
    void writeKvTuples(@Nullable Transaction tx, Map<Tuple, Tuple> pairs, ClientSchema schema, PayloadOutputChannel out) {
        out.out().packUuid(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());
        out.out().packInt(pairs.size());

        for (Map.Entry<Tuple, Tuple> pair : pairs.entrySet()) {
            writeKvTuple(tx, pair.getKey(), pair.getValue(), schema, out, true);
        }
    }

    /**
     * Writes {@link Tuple}'s.
     *
     * @param tuples Tuples.
     * @param schema Schema.
     * @param out Out.
     * @param keyOnly Key only.
     */
    void writeTuples(
            @Nullable Transaction tx,
            @NotNull Collection<Tuple> tuples,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean keyOnly
    ) {
        out.out().packUuid(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());
        out.out().packInt(tuples.size());

        for (var tuple : tuples) {
            writeTuple(tx, tuple, schema, out, keyOnly, true);
        }
    }

    static Tuple readTuple(ClientSchema schema, ClientMessageUnpacker in, boolean keyOnly) {
        var tuple = new ClientTuple(schema);

        var colCnt = keyOnly ? schema.keyColumnCount() : schema.columns().length;

        // TODO IGNITE-17927 Do not deserialize fields, wrap BinaryTuple as Tuple?
        var bufSize = in.unpackBinaryHeader();
        var buf = in.readPayload(bufSize);
        var binTuple = new BinaryTupleReader(colCnt, buf);

        for (var i = 0; i < colCnt; i++) {
            readAndSetValue(binTuple, tuple, i, i, schema.columns()[i].type());
        }

        return tuple;
    }

    static Tuple readValueTuple(ClientSchema schema, ClientMessageUnpacker in, Tuple keyTuple) {
        var tuple = new ClientTuple(schema);

        var bufSize = in.unpackBinaryHeader();
        var buf = in.readPayload(bufSize);
        var binTuple = new BinaryTupleReader(schema.columns().length - schema.keyColumnCount(), buf);

        for (var i = 0; i < schema.columns().length; i++) {
            ClientColumn col = schema.columns()[i];

            if (i < schema.keyColumnCount()) {
                tuple.setInternal(i, keyTuple.value(col.name()));
            } else {
                readAndSetValue(binTuple, tuple, i - schema.keyColumnCount(), i, col.type());
            }
        }

        return tuple;
    }

    static Tuple readValueTuple(ClientSchema schema, ClientMessageUnpacker in) {
        var keyColCnt = schema.keyColumnCount();
        var colCnt = schema.columns().length;

        var valTuple = new ClientTuple(schema, keyColCnt, schema.columns().length - 1);

        // TODO IGNITE-17297: Read from Netty buf directly (easier) OR wrap netty buf in a Tuple impl (may be hard with multiple tuples)
        var binTupleBuf = in.readPayload(in.unpackBinaryHeader());
        var binTuple = new BinaryTupleReader(colCnt - keyColCnt, binTupleBuf);

        for (var i = keyColCnt; i < colCnt; i++) {
            ClientColumn col = schema.columns()[i];
            Object val = in.unpackObject(col.type());

            valTuple.setInternal(i - keyColCnt, val);
        }

        return valTuple;
    }

    static IgniteBiTuple<Tuple, Tuple> readKvTuple(ClientSchema schema, ClientMessageUnpacker in) {
        var keyColCnt = schema.keyColumnCount();
        var colCnt = schema.columns().length;

        var keyTuple = new ClientTuple(schema, 0, keyColCnt - 1);
        var valTuple = new ClientTuple(schema, keyColCnt, schema.columns().length - 1);

        for (var i = 0; i < colCnt; i++) {
            ClientColumn col = schema.columns()[i];
            Object val = in.unpackObject(col.type());

            if (i < keyColCnt) {
                keyTuple.setInternal(i, val);
            } else {
                valTuple.setInternal(i - keyColCnt, val);
            }
        }

        return new IgniteBiTuple<>(keyTuple, valTuple);
    }

    /**
     * Reads {@link Tuple} pairs.
     *
     * @param schema Schema.
     * @param in In.
     * @return Tuple pairs.
     */
    static Map<Tuple, Tuple> readKvTuplesNullable(ClientSchema schema, ClientMessageUnpacker in) {
        var cnt = in.unpackInt();
        Map<Tuple, Tuple> res = new HashMap<>(cnt);

        for (int i = 0; i < cnt; i++) {
            var hasValue = in.unpackBoolean();

            if (hasValue) {
                var pair = readKvTuple(schema, in);

                res.put(pair.get1(), pair.get2());
            }
        }

        return res;
    }

    static Collection<Tuple> readTuples(ClientSchema schema, ClientMessageUnpacker in) {
        return readTuples(schema, in, false);
    }

    static Collection<Tuple> readTuples(ClientSchema schema, ClientMessageUnpacker in, boolean keyOnly) {
        var cnt = in.unpackInt();
        var res = new ArrayList<Tuple>(cnt);

        for (int i = 0; i < cnt; i++) {
            res.add(readTuple(schema, in, keyOnly));
        }

        return res;
    }

    static Collection<Tuple> readTuplesNullable(ClientSchema schema, ClientMessageUnpacker in) {
        var cnt = in.unpackInt();
        var res = new ArrayList<Tuple>(cnt);

        for (int i = 0; i < cnt; i++) {
            var tuple = in.unpackBoolean()
                    ? readTuple(schema, in, false)
                    : null;

            res.add(tuple);
        }

        return res;
    }

    private static void appendValue(BinaryTupleBuilder builder, ClientColumn col, Object v) {
        if (v == null) {
            builder.appendNull();
            return;
        }

        switch (col.type()) {
            case ClientDataType.INT8:
                builder.appendByte((byte) v);
                return;

            case ClientDataType.INT16:
                builder.appendShort((short) v);
                return;

            case ClientDataType.INT32:
                builder.appendInt((int) v);
                return;

            case ClientDataType.INT64:
                builder.appendLong((long) v);
                return;

            case ClientDataType.STRING:
                builder.appendString((String) v);
                return;

            default:
                // TODO IGNITE-17297 support all types.
                throw new UnsupportedOperationException("TODO");
        }
    }

    private static void readAndSetValue(
            BinaryTupleReader src,
            ClientTuple dst,
            int srcIdx,
            int dstIdx,
            int type) {
        if (src.hasNullValue(srcIdx)) {
            dst.setInternal(dstIdx, null);
        }

        switch (type) {
            case INT8:
                dst.setInternal(dstIdx, src.byteValue(srcIdx));
                break;

            case INT16:
                dst.setInternal(dstIdx, src.shortValue(srcIdx));
                break;

            case INT32:
                dst.setInternal(dstIdx, src.intValue(srcIdx));
                break;

            case INT64:
                dst.setInternal(dstIdx, src.longValue(srcIdx));
                break;

            case FLOAT:
                dst.setInternal(dstIdx, src.floatValue(srcIdx));
                break;

            case DOUBLE:
                dst.setInternal(dstIdx, src.doubleValue(srcIdx));
                break;

            case ClientDataType.UUID:
                dst.setInternal(dstIdx, src.uuidValue(srcIdx));
                break;

            case STRING:
                dst.setInternal(dstIdx, src.stringValue(srcIdx));
                break;

            case BYTES:
            case DECIMAL:
            case BIGINTEGER:
            case BITMASK:
            case NUMBER:
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case BOOLEAN:
            default:
                // TODO IGNITE-17297 all types.
                throw new IgniteException("Unknown client data type: " + type);

        }
    }
}
