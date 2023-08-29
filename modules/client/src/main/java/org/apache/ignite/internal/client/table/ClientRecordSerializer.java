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

import static org.apache.ignite.internal.client.table.ClientTable.writeTx;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallerUtil;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Record serializer.
 */
public class ClientRecordSerializer<R> {
    /** Table ID. */
    private final int tableId;

    /** Mapper. */
    private final Mapper<R> mapper;

    /** Simple mapping mode: single column maps to a basic type. For example, {@code RecordView<String>}.  */
    private final boolean oneColumnMode;

    /**
     * Constructor.
     *
     * @param tableId       Table ID.
     * @param mapper        Mapper.
     */
    ClientRecordSerializer(int tableId, Mapper<R> mapper) {
        assert mapper != null;

        this.tableId = tableId;
        this.mapper = mapper;

        oneColumnMode = MarshallerUtil.mode(mapper.targetType()) != null;
    }

    /**
     * Gets the mapper.
     *
     * @return Mapper.
     */
    Mapper<R> mapper() {
        return mapper;
    }

    /**
     * Writes a record without header.
     *
     * @param rec Record.
     * @param mapper Mapper.
     * @param schema Schema.
     * @param out Packer.
     * @param part Tuple part.
     * @param <R> Record type.
     */
    public static <R> void writeRecRaw(@Nullable R rec, Mapper<R> mapper, ClientSchema schema, ClientMessagePacker out, TuplePart part) {
        writeRecRaw(rec, out, schema.getMarshaller(mapper, part), columnCount(schema, part));
    }

    /**
     * Writes a record without header.
     *
     * @param rec Record.
     * @param out Writer.
     * @param marshaller Marshaller.
     * @param columnCount Column count.
     * @param <R> Record type.
     */
    static <R> void writeRecRaw(@Nullable R rec, ClientMessagePacker out, Marshaller marshaller, int columnCount) {
        try {
            var builder = new BinaryTupleBuilder(columnCount);
            var noValueSet = new BitSet();

            var writer = new ClientMarshallerWriter(builder, noValueSet);
            marshaller.writeObject(rec, writer);

            out.packBinaryTuple(builder, noValueSet);
        } catch (MarshallerException e) {
            throw new IgniteException(INTERNAL_ERR, e.getMessage(), e);
        }
    }

    void writeRecRaw(@Nullable R rec, ClientSchema schema, ClientMessagePacker out, TuplePart part) {
        writeRecRaw(rec, mapper, schema, out, part);
    }

    void writeRec(@Nullable Transaction tx, @Nullable R rec, ClientSchema schema, PayloadOutputChannel out, TuplePart part) {
        out.out().packInt(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());

        writeRecRaw(rec, schema, out.out(), part);
    }

    void writeRecs(
            @Nullable Transaction tx,
            @Nullable R rec,
            @Nullable R rec2,
            ClientSchema schema,
            PayloadOutputChannel out,
            TuplePart part
    ) {
        out.out().packInt(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());

        Marshaller marshaller = schema.getMarshaller(mapper, part);
        int columnCount = columnCount(schema, part);

        writeRecRaw(rec, out.out(), marshaller, columnCount);
        writeRecRaw(rec2, out.out(), marshaller, columnCount);
    }

    void writeRecs(
            @Nullable Transaction tx,
            Collection<R> recs,
            ClientSchema schema,
            PayloadOutputChannel out,
            TuplePart part
    ) {
        out.out().packInt(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());
        out.out().packInt(recs.size());

        Marshaller marshaller = schema.getMarshaller(mapper, part);
        int columnCount = columnCount(schema, part);

        for (R rec : recs) {
            writeRecRaw(rec, out.out(), marshaller, columnCount);
        }
    }

    List<R> readRecs(ClientSchema schema, ClientMessageUnpacker in, boolean nullable, TuplePart part) {
        var cnt = in.unpackInt();

        if (cnt == 0) {
            return Collections.emptyList();
        }

        var res = new ArrayList<R>(cnt);

        Marshaller marshaller = schema.getMarshaller(mapper, part);

        try {
            for (int i = 0; i < cnt; i++) {
                if (nullable && !in.unpackBoolean()) {
                    res.add(null);
                } else {
                    var tupleReader = new BinaryTupleReader(columnCount(schema, part), in.readBinaryUnsafe());
                    var reader = new ClientMarshallerReader(tupleReader);
                    res.add((R) marshaller.readObject(reader, null));
                }
            }
        } catch (MarshallerException e) {
            throw new IgniteException(INTERNAL_ERR, e.getMessage(), e);
        }

        return res;
    }

    R readRec(ClientSchema schema, ClientMessageUnpacker in, TuplePart part) {
        Marshaller marshaller = schema.getMarshaller(mapper, part);

        int columnCount = part == TuplePart.KEY ? schema.keyColumnCount() : schema.columns().length;
        var tupleReader = new BinaryTupleReader(columnCount, in.readBinaryUnsafe());

        int startIndex = part == TuplePart.VAL ? schema.keyColumnCount() : 0;
        ClientMarshallerReader reader = new ClientMarshallerReader(tupleReader, startIndex);

        try {
            return (R) marshaller.readObject(reader, null);
        } catch (MarshallerException e) {
            throw new IgniteException(INTERNAL_ERR, e.getMessage(), e);
        }
    }

    R readValRec(R keyRec, ClientSchema schema, ClientMessageUnpacker in) {
        if (oneColumnMode) {
            return keyRec;
        }

        Marshaller valMarshaller = schema.getMarshaller(mapper, TuplePart.KEY_AND_VAL);

        var tupleReader = new BinaryTupleReader(schema.columns().length, in.readBinaryUnsafe());
        ClientMarshallerReader reader = new ClientMarshallerReader(tupleReader);

        try {
            return (R) valMarshaller.readObject(reader, null);
        } catch (MarshallerException e) {
            throw new IgniteException(INTERNAL_ERR, e.getMessage(), e);
        }
    }

    private static int columnCount(ClientSchema schema, TuplePart part) {
        switch (part) {
            case KEY:
                return schema.keyColumnCount();

            case VAL:
                return schema.columns().length - schema.keyColumnCount();

            case KEY_AND_VAL:
                return schema.columns().length;

            default:
                throw new IllegalArgumentException();
        }
    }
}
