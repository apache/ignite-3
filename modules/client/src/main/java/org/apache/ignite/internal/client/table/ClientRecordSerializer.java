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

import static org.apache.ignite.internal.client.table.ClientTable.writeTx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallerUtil;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Record serializer.
 */
public class ClientRecordSerializer<R> {
    /** Table ID. */
    private final UUID tableId;

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
    ClientRecordSerializer(UUID tableId, Mapper<R> mapper) {
        assert tableId != null;
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
        Marshaller marshaller = schema.getMarshaller(mapper, part);
        ClientMarshallerWriter writer = new ClientMarshallerWriter(out);

        try {
            marshaller.writeObject(rec, writer);
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    void writeRecRaw(@Nullable R rec, ClientSchema schema, ClientMessagePacker out, TuplePart part) {
        writeRecRaw(rec, mapper, schema, out, part);
    }

    void writeRec(@Nullable Transaction tx, @Nullable R rec, ClientSchema schema, PayloadOutputChannel out, TuplePart part) {
        out.out().packUuid(tableId);
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
        out.out().packUuid(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());

        Marshaller marshaller = schema.getMarshaller(mapper, part);
        ClientMarshallerWriter writer = new ClientMarshallerWriter(out.out());

        try {
            marshaller.writeObject(rec, writer);
            marshaller.writeObject(rec2, writer);
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    void writeRecs(
            @Nullable Transaction tx,
            @NotNull Collection<R> recs,
            ClientSchema schema,
            PayloadOutputChannel out,
            TuplePart part
    ) {
        out.out().packUuid(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());
        out.out().packInt(recs.size());

        Marshaller marshaller = schema.getMarshaller(mapper, part);
        ClientMarshallerWriter writer = new ClientMarshallerWriter(out.out());

        try {
            for (R rec : recs) {
                marshaller.writeObject(rec, writer);
            }
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    Collection<R> readRecs(ClientSchema schema, ClientMessageUnpacker in, boolean nullable, TuplePart part) {
        var cnt = in.unpackInt();

        if (cnt == 0) {
            return Collections.emptyList();
        }

        var res = new ArrayList<R>(cnt);

        Marshaller marshaller = schema.getMarshaller(mapper, part);
        var reader = new ClientMarshallerReader(in);

        try {
            for (int i = 0; i < cnt; i++) {
                if (nullable && !in.unpackBoolean()) {
                    res.add(null);
                } else {
                    res.add((R) marshaller.readObject(reader, null));
                }
            }
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }

        return res;
    }

    R readRec(ClientSchema schema, ClientMessageUnpacker in, TuplePart part) {
        Marshaller marshaller = schema.getMarshaller(mapper, part);
        ClientMarshallerReader reader = new ClientMarshallerReader(in);

        try {
            return (R) marshaller.readObject(reader, null);
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    R readValRec(@NotNull R keyRec, ClientSchema schema, ClientMessageUnpacker in) {
        if (oneColumnMode) {
            return keyRec;
        }

        Marshaller keyMarshaller = schema.getMarshaller(mapper, TuplePart.KEY);
        Marshaller valMarshaller = schema.getMarshaller(mapper, TuplePart.VAL);

        ClientMarshallerReader reader = new ClientMarshallerReader(in);

        try {
            var res = (R) valMarshaller.readObject(reader, null);

            keyMarshaller.copyObject(keyRec, res);

            return res;
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }
}
