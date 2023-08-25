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

package org.apache.ignite.internal.client.table;

import static org.apache.ignite.internal.client.proto.ClientMessageCommon.NO_VALUE;
import static org.apache.ignite.internal.client.table.ClientTable.writeTx;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.marshaller.UnmappedColumnsException;
import org.apache.ignite.internal.util.HashCalculator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Tuple serializer.
 */
public class ClientTupleSerializer {
    /** Table ID. */
    private final int tableId;

    /**
     * Constructor.
     *
     * @param tableId Table id.
     */
    ClientTupleSerializer(int tableId) {
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
            Tuple tuple,
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
            Tuple tuple,
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
            Tuple tuple,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean keyOnly,
            boolean skipHeader
    ) {
        if (!skipHeader) {
            out.out().packInt(tableId);
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
    public static void writeTupleRaw(Tuple tuple, ClientSchema schema, PayloadOutputChannel out, boolean keyOnly) {
        var columns = schema.columns();
        var count = keyOnly ? schema.keyColumnCount() : columns.length;

        var builder = new BinaryTupleBuilder(count);
        var noValueSet = new BitSet(count);

        int usedCols = 0;

        for (var i = 0; i < count; i++) {
            var col = columns[i];
            Object v = tuple.valueOrDefault(col.name(), NO_VALUE);

            if (v != NO_VALUE) {
                usedCols++;
            }

            appendValue(builder, noValueSet, col, v);
        }

        if (!keyOnly && tuple.columnCount() > usedCols) {
            throwSchemaMismatchException(tuple, schema, TuplePart.KEY_AND_VAL);
        }

        out.out().packBinaryTuple(builder, noValueSet);
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
            Tuple key,
            @Nullable Tuple val,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean skipHeader
    ) {
        if (!skipHeader) {
            out.out().packInt(tableId);
            writeTx(tx, out);
            out.out().packInt(schema.version());
        }

        var columns = schema.columns();
        var noValueSet = new BitSet(columns.length);
        var builder = new BinaryTupleBuilder(columns.length);

        int usedKeyCols = 0;
        int usedValCols = 0;

        for (ClientColumn col : columns) {
            Object v;

            if (col.key()) {
                v = key.valueOrDefault(col.name(), NO_VALUE);

                if (v != NO_VALUE) {
                    usedKeyCols++;
                }
            } else {
                v = val != null
                        ? val.valueOrDefault(col.name(), NO_VALUE)
                        : NO_VALUE;

                if (v != NO_VALUE) {
                    usedValCols++;
                }
            }

            appendValue(builder, noValueSet, col, v);
        }

        if (key.columnCount() > usedKeyCols) {
            throwSchemaMismatchException(key, schema, TuplePart.KEY);
        }

        if (val != null && val.columnCount() > usedValCols) {
            throwSchemaMismatchException(val, schema, TuplePart.VAL);
        }

        out.out().packBinaryTuple(builder, noValueSet);
    }

    /**
     * Writes pairs {@link Tuple}.
     *
     * @param pairs Key tuple.
     * @param schema Schema.
     * @param out Out.
     */
    void writeKvTuples(@Nullable Transaction tx, Collection<Entry<Tuple, Tuple>> pairs, ClientSchema schema, PayloadOutputChannel out) {
        out.out().packInt(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());
        out.out().packInt(pairs.size());

        for (Map.Entry<Tuple, Tuple> pair : pairs) {
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
            Collection<Tuple> tuples,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean keyOnly
    ) {
        out.out().packInt(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());
        out.out().packInt(tuples.size());

        for (var tuple : tuples) {
            writeTuple(tx, tuple, schema, out, keyOnly, true);
        }
    }

    static Tuple readTuple(ClientSchema schema, ClientMessageUnpacker in, boolean keyOnly) {
        var colCnt = keyOnly ? schema.keyColumnCount() : schema.columns().length;
        var binTuple = new BinaryTupleReader(colCnt, in.readBinary());

        return new ClientTuple(schema, binTuple, 0, colCnt);
    }

    static Tuple readValueTuple(ClientSchema schema, ClientMessageUnpacker in) {
        var keyColCnt = schema.keyColumnCount();
        var colCnt = schema.columns().length;

        var binTuple = new BinaryTupleReader(colCnt, in.readBinary());

        return new ClientTuple(schema, binTuple, keyColCnt, colCnt);
    }

    private static IgniteBiTuple<Tuple, Tuple> readKvTuple(ClientSchema schema, ClientMessageUnpacker in) {
        var keyColCnt = schema.keyColumnCount();
        var colCnt = schema.columns().length;

        var binTuple = new BinaryTupleReader(colCnt, in.readBinary());
        var keyTuple2 = new ClientTuple(schema, binTuple, 0, keyColCnt);
        var valTuple2 = new ClientTuple(schema, binTuple, keyColCnt, colCnt);

        return new IgniteBiTuple<>(keyTuple2, valTuple2);
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

    static List<Tuple> readTuplesNullable(ClientSchema schema, ClientMessageUnpacker in) {
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

    private static void appendValue(BinaryTupleBuilder builder, BitSet noValueSet, ClientColumn col, @Nullable Object v) {
        if (v == NO_VALUE) {
            noValueSet.set(col.schemaIndex());
            builder.appendNull();
            return;
        }

        ClientBinaryTupleUtils.appendValue(builder, col.type(), col.name(), col.scale(), v);
    }

    /**
     * Gets partition awareness provider for the specified tuple.
     *
     * @param tx Transaction.
     * @param rec Tuple.
     * @return Partition awareness provider.
     */
    public static PartitionAwarenessProvider getPartitionAwarenessProvider(@Nullable Transaction tx, Tuple rec) {
        if (tx != null) {
            //noinspection resource
            return PartitionAwarenessProvider.of(ClientTransaction.get(tx).channel().protocolContext().clusterNode().name());
        }

        return PartitionAwarenessProvider.of(schema -> getColocationHash(schema, rec));
    }

    /**
     * Gets partition awareness provider for the specified object.
     *
     * @param tx Transaction.
     * @param rec Object.
     * @return Partition awareness provider.
     */
    public static PartitionAwarenessProvider getPartitionAwarenessProvider(
            @Nullable Transaction tx, Mapper<?> mapper, Object rec) {
        if (tx != null) {
            //noinspection resource
            return PartitionAwarenessProvider.of(ClientTransaction.get(tx).channel().protocolContext().clusterNode().name());
        }

        return PartitionAwarenessProvider.of(schema -> getColocationHash(schema, mapper, rec));
    }

    /**
     * Gets colocation hash for the specified tuple.
     *
     * @param schema Schema.
     * @param rec Tuple.
     * @return Colocation hash.
     */
    public static int getColocationHash(ClientSchema schema, Tuple rec) {
        var hashCalc = new HashCalculator();

        for (ClientColumn col : schema.colocationColumns()) {
            // Colocation columns are always part of the key and can't be missing; serializer will check this.
            Object value = rec.valueOrDefault(col.name(), null);
            hashCalc.append(value, col.scale(), col.precision());
        }

        return hashCalc.hash();
    }

    static Integer getColocationHash(ClientSchema schema, Mapper<?> mapper, Object rec) {
        // Colocation columns are always part of the key - https://cwiki.apache.org/confluence/display/IGNITE/IEP-86%3A+Colocation+Key.
        var hashCalc = new HashCalculator();
        var marsh = schema.getMarshaller(mapper, TuplePart.KEY, true);

        for (ClientColumn col : schema.colocationColumns()) {
            Object value = marsh.value(rec, col.schemaIndex());
            hashCalc.append(value, col.scale(), col.precision());
        }

        return hashCalc.hash();
    }

    private static void throwSchemaMismatchException(Tuple tuple, ClientSchema schema, TuplePart part) {
        ClientColumn[] columns = schema.columns();
        Set<String> extraColumns = new HashSet<>();
        int start = part == TuplePart.VAL ? schema.keyColumnCount() : 0;
        int end = part == TuplePart.KEY ? schema.keyColumnCount() : columns.length;

        for (int i = 0; i < tuple.columnCount(); i++) {
            extraColumns.add(tuple.columnName(i));
        }


        for (int i = start; i < end; i++) {
            extraColumns.remove(columns[i].name());
        }

        String prefix = "Tuple";

        if (part == TuplePart.KEY) {
            prefix = "Key tuple";
        } else if (part == TuplePart.VAL) {
            prefix = "Value tuple";
        }

        throw new IllegalArgumentException(String.format("%s doesn't match schema: schemaVersion=%s, extraColumns=%s",
                prefix, schema.version(), extraColumns), new UnmappedColumnsException());
    }
}
