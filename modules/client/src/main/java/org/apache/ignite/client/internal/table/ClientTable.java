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

package org.apache.ignite.client.internal.table;

import org.apache.ignite.client.ClientMessageUnpacker;
import org.apache.ignite.client.ClientOp;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.client.internal.ReliableChannel;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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

    public ClientTable(ReliableChannel ch, UUID id, String name) {
        assert ch != null;
        assert id != null;
        assert name != null && name.length() > 0;

        this.ch = ch;
        this.id = id;
        this.name = name;
    }

    public UUID tableId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public KeyValueBinaryView kvView() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Table withTransaction(Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder tupleBuilder() {
        return new ClientTupleBuilder();
    }

    /** {@inheritDoc} */
    @Override public Tuple get(@NotNull Tuple keyRec) {
        return getAsync(keyRec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple keyRec) {
        // TODO: Implement shared logic for tuple serialization and schema handling.
        return getLatestSchema().thenCompose(schema -> ch.serviceAsync(ClientOp.TUPLE_GET, w -> {
            // TODO: We should accept any Tuple implementation, but this requires extending the Tuple interface
            // with methods to retrieve column list.
            var tuple = (ClientTupleBuilder) keyRec;

            // TODO: Match columns to schema and write in schema order.
            var vals = new Object[schema.keyColumns().size()];

            for (var entry : tuple.map().entrySet()) {
                var col = schema.keyColumns().get(entry.getKey());

                if (col == null)
                    continue; // Not a key column.

                vals[col.schemaIndex()] = entry.getValue();
            }

            w.out().packUuid(id);
            w.out().packInt(schema.version());
            w.out().packArrayHeader(vals.length);

            for (var val : vals)
                w.out().packObject(val);
        }, r -> {
            return null;
        }));
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> getAll(@NotNull Collection<Tuple> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@NotNull Collection<Tuple> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsert(@NotNull Tuple rec) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAsync(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(@NotNull Collection<Tuple> recs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndUpsert(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean insert(@NotNull Tuple rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> insertAll(@NotNull Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@NotNull Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean delete(@NotNull Tuple keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull Tuple keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(@NotNull Tuple rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndDelete(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@NotNull Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAll(@NotNull Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@NotNull Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAllExact(@NotNull Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(@NotNull Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(@NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(@NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<Tuple, T> invokeAll(@NotNull Collection<Tuple> keyRecs, InvokeProcessor<Tuple, Tuple, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(@NotNull Collection<Tuple> keyRecs, InvokeProcessor<Tuple, Tuple, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Transaction transaction() {
        return null;
    }

    private CompletableFuture<ClientSchema> getLatestSchema() {
        if (latestSchemaVer >= 0)
            return CompletableFuture.completedFuture(schemas.get(latestSchemaVer));

        return loadSchemas();
    }

    private CompletableFuture<ClientSchema> getSchema(int ver) {
        var schema = schemas.get(ver);

        if (schema != null)
            return CompletableFuture.completedFuture(schema);

        return loadSchemas(ver);
    }

    private CompletableFuture<ClientSchema> loadSchemas(int... vers) {
        return ch.serviceAsync(ClientOp.SCHEMAS_GET, w -> {
            w.out().packUuid(id);

            if (vers == null || vers.length == 0)
                w.out().packNil();
            else {
                w.out().packArrayHeader(vers.length);

                for (var ver : vers)
                    w.out().packInt(ver);
            }
        }, r -> {
            int schemaCnt = r.in().unpackMapHeader();

            if (schemaCnt == 0)
                return null;

            ClientSchema last = null;

            for (var i = 0; i < schemaCnt; i++)
                last = readSchema(r.in());

            return last;
        });
    }

    private ClientSchema readSchema(ClientMessageUnpacker in) throws IOException {
        var schemaVer = in.unpackInt();
        var colCnt = in.unpackArrayHeader();

        var columns = new ClientColumn[colCnt];

        for (int i = 0; i < colCnt; i++) {
            var propCnt = in.unpackArrayHeader();

            assert propCnt >= 4;

            var name = in.unpackString();
            var type = in.unpackString();
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
}
