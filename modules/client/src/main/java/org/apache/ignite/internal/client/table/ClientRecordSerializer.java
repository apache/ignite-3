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

import static org.apache.ignite.internal.client.tx.DirectTxUtils.writeTx;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.WriteContext;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.BinaryMode;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.Marshaller;
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

        oneColumnMode = BinaryMode.forClass(mapper.targetType()) != BinaryMode.POJO;
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
     * @param allowUnmappedFields Allow unmapped fields.
     */
    public static <R> void writeRecRaw(@Nullable R rec, Mapper<R> mapper, ClientSchema schema, ClientMessagePacker out, TuplePart part,
            boolean allowUnmappedFields) {
        writeRecRaw(rec, out, schema.getMarshaller(mapper, part, allowUnmappedFields), columnCount(schema, part));
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
    private static <R> void writeRecRaw(@Nullable R rec, ClientMessagePacker out, Marshaller marshaller, int columnCount) {
        var builder = new BinaryTupleBuilder(columnCount);
        var noValueSet = new BitSet();

        var writer = new ClientMarshallerWriter(builder, noValueSet);
        marshaller.writeObject(rec, writer);

        out.packBinaryTuple(builder, noValueSet);
    }

    private void writeRecRaw(@Nullable R rec, ClientSchema schema, ClientMessagePacker out, TuplePart part, boolean allowUnmappedFields) {
        writeRecRaw(rec, mapper, schema, out, part, allowUnmappedFields);
    }

    void writeRec(
            @Nullable Transaction tx,
            @Nullable R rec,
            ClientSchema schema,
            PayloadOutputChannel out,
            WriteContext ctx,
            TuplePart part
    ) {
        writeRec(tx, rec, schema, out, ctx, part, false);
    }

    void writeRec(
            @Nullable Transaction tx,
            @Nullable R rec,
            ClientSchema schema,
            PayloadOutputChannel out,
            WriteContext ctx,
            TuplePart part,
            boolean allowUnmappedFields
    ) {
        out.out().packInt(tableId);
        writeTx(tx, out, ctx);
        out.out().packInt(schema.version());

        writeRecRaw(rec, schema, out.out(), part, allowUnmappedFields);
    }

    void writeRecs(
            @Nullable Transaction tx,
            @Nullable R rec,
            @Nullable R rec2,
            ClientSchema schema,
            PayloadOutputChannel out,
            WriteContext ctx,
            TuplePart part
    ) {
        out.out().packInt(tableId);
        writeTx(tx, out, ctx);
        out.out().packInt(schema.version());

        Marshaller marshaller = schema.getMarshaller(mapper, part, false);
        int columnCount = columnCount(schema, part);

        writeRecRaw(rec, out.out(), marshaller, columnCount);
        writeRecRaw(rec2, out.out(), marshaller, columnCount);
    }

    void writeRecs(
            @Nullable Transaction tx,
            Collection<R> recs,
            ClientSchema schema,
            PayloadOutputChannel out,
            WriteContext ctx,
            TuplePart part
    ) {
        writeRecs(tx, recs, schema, out, ctx, part, false);
    }

    void writeRecs(
            @Nullable Transaction tx,
            Collection<R> recs,
            ClientSchema schema,
            PayloadOutputChannel out,
            WriteContext ctx,
            TuplePart part,
            boolean allowUnmappedFields
    ) {
        out.out().packInt(tableId);
        writeTx(tx, out, ctx);
        out.out().packInt(schema.version());
        out.out().packInt(recs.size());

        Marshaller marshaller = schema.getMarshaller(mapper, part, allowUnmappedFields);
        int columnCount = columnCount(schema, part);

        for (R rec : recs) {
            writeRecRaw(rec, out.out(), marshaller, columnCount);
        }
    }

    void writeStreamerRecs(
            int partitionId,
            Collection<R> recs,
            @Nullable BitSet deleted,
            ClientSchema schema,
            PayloadOutputChannel out
    ) {
        ClientMessagePacker w = out.out();

        w.packInt(tableId);
        w.packInt(partitionId);
        w.packBitSetNullable(deleted);
        w.packInt(schema.version());
        w.packInt(recs.size());

        Marshaller marshaller = schema.getMarshaller(mapper, TuplePart.KEY_AND_VAL);
        Marshaller keyMarshaller = deleted == null || deleted.cardinality() == 0
                ? null
                : schema.getMarshaller(mapper, TuplePart.KEY);

        int columnCount = schema.columns().length;
        int keyColumnCount = schema.keyColumns().length;

        int i = 0;

        for (R rec : recs) {
            boolean del = deleted != null && deleted.get(i++);
            int colCount = del ? keyColumnCount : columnCount;
            Marshaller marsh = del ? keyMarshaller : marshaller;

            //noinspection DataFlowIssue (reviewed).
            writeRecRaw(rec, w, marsh, colCount);
        }
    }

    List<R> readRecs(ClientSchema schema, ClientMessageUnpacker in, boolean nullable, TuplePart part) {
        var cnt = in.unpackInt();

        if (cnt == 0) {
            return Collections.emptyList();
        }

        var res = new ArrayList<R>(cnt);

        Marshaller marshaller = schema.getMarshaller(mapper, part);

        for (int i = 0; i < cnt; i++) {
            if (nullable && !in.unpackBoolean()) {
                res.add(null);
            } else {
                ClientColumn[] columns = schema.columns(part);
                var tupleReader = new BinaryTupleReader(columns.length, in.readBinaryUnsafe());
                var reader = new ClientMarshallerReader(tupleReader, columns, part);
                res.add((R) marshaller.readObject(reader, null));
            }
        }

        return res;
    }

    R readRec(ClientSchema schema, ClientMessageUnpacker in, TuplePart partToRead, TuplePart dataPart) {
        Marshaller marshaller = schema.getMarshaller(mapper, partToRead);

        var tupleReader = new BinaryTupleReader(schema.columns().length, in.readBinaryUnsafe());
        ClientMarshallerReader reader = new ClientMarshallerReader(tupleReader, schema.columns(partToRead), dataPart);

        return (R) marshaller.readObject(reader, null);
    }

    R readValRec(R keyRec, ClientSchema schema, ClientMessageUnpacker in) {
        if (oneColumnMode) {
            return keyRec;
        }

        Marshaller valMarshaller = schema.getMarshaller(mapper, TuplePart.KEY_AND_VAL);

        var tupleReader = new BinaryTupleReader(schema.columns().length, in.readBinaryUnsafe());
        ClientMarshallerReader reader = new ClientMarshallerReader(tupleReader, schema.columns(), TuplePart.KEY_AND_VAL);

        return (R) valMarshaller.readObject(reader, null);
    }

    private static int columnCount(ClientSchema schema, TuplePart part) {
        return schema.columns(part).length;
    }
}
