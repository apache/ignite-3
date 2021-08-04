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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.client.proto.ClientOp;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.msgpack.core.MessageFormat;

/**
 * Client table API implementation.
 */
public class ClientTable implements Table {
    /** */
    private final UUID id;

    /** */
    private final String name;

    /** */
    private final ReliableChannel ch;

    /** */
    private final ConcurrentHashMap<Integer, ClientSchema> schemas = new ConcurrentHashMap<>();

    /** */
    private volatile int latestSchemaVer = -1;

    /** */
    private final Object latestSchemaLock = new Object();

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param id Table id.
     * @param name Table name.
     */
    public ClientTable(ReliableChannel ch, UUID id, String name) {
        assert ch != null;
        assert id != null;
        assert name != null && !name.isEmpty();

        this.ch = ch;
        this.id = id;
        this.name = name;
    }

    /**
     * Gets the table id.
     *
     * @return Table id.
     */
    public UUID tableId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public KeyValueBinaryView kvView() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Table withTransaction(Transaction tx) {
        // TODO: Transactions IGNITE-15240
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder tupleBuilder() {
        return new ClientTupleBuilder(getLatestSchema().join());
    }

    /** {@inheritDoc} */
    @Override public Tuple get(@NotNull Tuple keyRec) {
        return getAsync(keyRec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        return getLatestSchema()
                .thenCompose(schema ->
                        ch.serviceAsync(ClientOp.TUPLE_GET,
                                w -> writeTuple(keyRec, schema, w.out(), true),
                                r -> {
                                    if (r.in().getNextFormat() == MessageFormat.NIL)
                                        return null;

                                    var schemaVer = r.in().unpackInt();

                                    var resSchema = schemaVer == schema.version() ? schema : schemas.get(schemaVer);

                                    if (resSchema != null) {
                                        return readTuple(schema, r.in());
                                    }

                                    // Schema is not yet known - request
                                    return new IgniteBiTuple<>(r.in().copy(), schemaVer);
                                }))
                .thenCompose(this::getSchemaAndReadTuple);
    }

    @SuppressWarnings("unchecked")
    public CompletionStage<Tuple> getSchemaAndReadTuple(Object data) {
        if (!(data instanceof IgniteBiTuple))
            return CompletableFuture.completedFuture((Tuple) data);

        var biTuple = (IgniteBiTuple<ClientMessageUnpacker, Integer>) data;

        var in = biTuple.getKey();
        var schemaId = biTuple.getValue();

        assert in != null;
        assert schemaId != null;

        var tupleFut = getSchema(schemaId).thenApply(schema -> readTuple(schema, in));

        // Close copied unpacker.
        tupleFut.handle((tuple, err) -> {
            in.close();
            return null;
        });

        return tupleFut;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> getAll(@NotNull Collection<Tuple> keyRecs) {
        return getAllAsync(keyRecs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@NotNull Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void upsert(@NotNull Tuple rec) {
        upsertAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return getLatestSchema().thenCompose(schema -> ch.serviceAsync(ClientOp.TUPLE_UPSERT,
                w -> writeTuple(rec, schema, w.out(), false), r -> null));
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(@NotNull Collection<Tuple> recs) {
        upsertAllAsync(recs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndUpsert(@NotNull Tuple rec) {
        return getAndUpsertAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean insert(@NotNull Tuple rec) {
        return insertAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return getLatestSchema().thenCompose(schema -> ch.serviceAsync(ClientOp.TUPLE_INSERT,
                w -> writeTuple(rec, schema, w.out(), false),
                r -> r.in().unpackBoolean()));
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> insertAll(@NotNull Collection<Tuple> recs) {
        return insertAllAsync(recs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return getLatestSchema().thenCompose(schema -> ch.serviceAsync(
                ClientOp.TUPLE_INSERT_ALL,
                w -> writeTuples(recs, schema, w.out(), false),

                // TODO: get schema, reuse code
                r -> readTuples(null, null)));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple rec) {
        return replaceAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(@NotNull Tuple rec) {
        return getAndUpsertAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean delete(@NotNull Tuple keyRec) {
        return deleteAsync(keyRec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(@NotNull Tuple rec) {
        return deleteExactAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndDelete(@NotNull Tuple rec) {
        return getAndDeleteAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAll(@NotNull Collection<Tuple> recs) {
        return deleteAllAsync(recs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAllExact(@NotNull Collection<Tuple> recs) {
        return deleteAllExactAsync(recs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(@NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(@NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<Tuple, T> invokeAll(@NotNull Collection<Tuple> keyRecs, InvokeProcessor<Tuple, Tuple, T> proc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(@NotNull Collection<Tuple> keyRecs, InvokeProcessor<Tuple, Tuple, T> proc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public @Nullable Transaction transaction() {
        // TODO: Transactions IGNITE-15240
        throw new UnsupportedOperationException();
    }

    private CompletableFuture<ClientSchema> getLatestSchema() {
        if (latestSchemaVer >= 0)
            return CompletableFuture.completedFuture(schemas.get(latestSchemaVer));

        return loadSchema(null);
    }

    private CompletableFuture<ClientSchema> getSchema(int ver) {
        var schema = schemas.get(ver);

        if (schema != null)
            return CompletableFuture.completedFuture(schema);

        return loadSchema(ver);
    }

    private CompletableFuture<ClientSchema> loadSchema(Integer ver) {
        return ch.serviceAsync(ClientOp.SCHEMAS_GET, w -> {
            w.out().packUuid(id);

            if (ver == null)
                w.out().packNil();
            else {
                w.out().packArrayHeader(1);
                w.out().packInt(ver);
            }
        }, r -> {
            int schemaCnt = r.in().unpackMapHeader();

            if (schemaCnt == 0)
                throw new IgniteClientException("Schema not found: " + ver);

            ClientSchema last = null;

            for (var i = 0; i < schemaCnt; i++)
                last = readSchema(r.in());

            return last;
        });
    }

    private ClientSchema readSchema(ClientMessageUnpacker in) {
        var schemaVer = in.unpackInt();
        var colCnt = in.unpackArrayHeader();

        var columns = new ClientColumn[colCnt];

        for (int i = 0; i < colCnt; i++) {
            var propCnt = in.unpackArrayHeader();

            assert propCnt >= 4;

            var name = in.unpackString();
            var type = in.unpackInt();
            var isKey = in.unpackBoolean();
            var isNullable = in.unpackBoolean();

            // Skip unknown extra properties, if any.
            in.skipValue(propCnt - 4);

            var column = new ClientColumn(name, type, isNullable, isKey, i);
            columns[i] = column;
        }

        var schema = new ClientSchema(schemaVer, columns);

        schemas.put(schemaVer, schema);

        synchronized (latestSchemaLock) {
            if (schemaVer > latestSchemaVer) {
                latestSchemaVer = schemaVer;
            }
        }

        return schema;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return IgniteToStringBuilder.toString(ClientTable.class, this);
    }

    private void writeTuple(@NotNull Tuple tuple, ClientSchema schema, ClientMessagePacker out, boolean keyOnly) {
        // TODO: Special case for ClientTupleBuilder - it has columns in order
        var vals = new Object[keyOnly ? schema.keyColumnCount() : schema.columns().length];
        var tupleSize = tuple.columnCount();

        for (var i = 0; i < tupleSize; i++) {
            var colName = tuple.columnName(i);
            var col = schema.column(colName);

            if (keyOnly && !col.key())
                continue;

            vals[col.schemaIndex()] = tuple.value(i);
        }

        out.packUuid(id);
        out.packInt(schema.version());

        for (var val : vals)
            out.packObject(val);
    }

    private void writeTuples(
            @NotNull Collection<Tuple> tuples,
            ClientSchema schema,
            ClientMessagePacker out,
            boolean keyOnly
    ) {
        // TODO
    }

    private Tuple readTuple(ClientSchema schema, ClientMessageUnpacker in) {
        var builder = new ClientTupleBuilder(schema);

        for (var col : schema.columns())
            builder.setInternal(col.schemaIndex(), in.unpackObject(col.type()));

        return builder;
    }

    private Collection<Tuple> readTuples(ClientSchema schema, ClientMessageUnpacker in) {
        // TODO
        return null;
    }
}
