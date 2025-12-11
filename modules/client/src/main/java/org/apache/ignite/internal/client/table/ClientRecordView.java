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

import static org.apache.ignite.internal.client.table.ClientTupleSerializer.getColocationHash;
import static org.apache.ignite.internal.client.table.ClientTupleSerializer.getPartitionAwarenessProvider;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ViewUtils.checkCollectionForNulls;
import static org.apache.ignite.internal.util.ViewUtils.checkKeysForNulls;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.TupleReader;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.table.criteria.SqlRowProjection;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client record view implementation.
 */
public class ClientRecordView<R> extends AbstractClientView<R> implements RecordView<R> {
    /** Serializer. */
    private final ClientRecordSerializer<R> ser;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     * @param sql Sql.
     * @param recMapper Mapper.
     */
    ClientRecordView(ClientTable tbl, ClientSql sql, Mapper<R> recMapper) {
        super(tbl, sql);

        ser = new ClientRecordSerializer<>(tbl.tableId(), recMapper);
    }

    /** {@inheritDoc} */
    @Override
    public R get(@Nullable Transaction tx, R keyRec) {
        return sync(getAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAsync(@Nullable Transaction tx, R keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w, n) -> ser.writeRec(tx, keyRec, s, w, n, TuplePart.KEY, true),
                (s, r) -> ser.readValRec(keyRec, s, r.in()),
                null,
                getPartitionAwarenessProvider(ser.mapper(), keyRec),
                tx);
    }

    @Override
    public List<R> getAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return sync(getAllAsync(tx, keyRecs));
    }

    @Override
    public CompletableFuture<List<R>> getAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        checkCollectionForNulls(keyRecs, "keyRecs", "key");

        if (keyRecs.isEmpty()) {
            return emptyListCompletedFuture();
        }

        List<Transaction> txns = new ArrayList<>();

        MapFunction<R, List<R>> clo = (batch, provider, txRequired) -> {
            Transaction tx0 = tbl.startTxIfNeeded(tx, txns, txRequired);

            return tbl.doSchemaOutInOpAsync(
                    ClientOp.TUPLE_GET_ALL,
                    (s, w, n) -> ser.writeRecs(tx0, batch, s, w, n, TuplePart.KEY, true),
                    (s, r) -> ser.readRecs(s, r.in(), true, TuplePart.KEY_AND_VAL),
                    Collections.emptyList(),
                    provider,
                    tx0);
        };

        return tbl.splitAndRun(keyRecs, clo, (schema, entry) -> getColocationHash(schema, ser.mapper(), entry), txns);
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, R key) {
        return sync(containsAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, R key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_CONTAINS_KEY,
                (s, w, n) -> ser.writeRec(tx, key, s, w, n, TuplePart.KEY, true),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(ser.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsAll(@Nullable Transaction tx, Collection<R> keys) {
        return sync(containsAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<R> keys) {
        checkKeysForNulls(keys);

        if (keys.isEmpty()) {
            return trueCompletedFuture();
        }

        List<Transaction> txns = new ArrayList<>();

        MapFunction<R, Boolean> clo = (batch, provider, txRequired) -> {
            Transaction tx0 = tbl.startTxIfNeeded(tx, txns, txRequired);

            return tbl.doSchemaOutOpAsync(
                    ClientOp.TUPLE_CONTAINS_ALL_KEYS,
                    (s, w, n) -> ser.writeRecs(tx0, batch, s, w, n, TuplePart.KEY, true),
                    r -> r.in().unpackBoolean(),
                    provider,
                    tx0);
        };

        return tbl.splitAndRun(keys, clo, Boolean.TRUE, (agg, cur) -> agg && cur,
                (schema, entry) -> getColocationHash(schema, ser.mapper(), entry), txns);
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@Nullable Transaction tx, R rec) {
        sync(upsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w, n) -> ser.writeRec(tx, rec, s, w, n, TuplePart.KEY_AND_VAL),
                r -> null,
                getPartitionAwarenessProvider(ser.mapper(), rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@Nullable Transaction tx, Collection<R> recs) {
        sync(upsertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        checkCollectionForNulls(recs, "recs", "rec");

        if (recs.isEmpty()) {
            return nullCompletedFuture();
        }

        MapFunction<R, Void> clo = (batch, provider, txRequired) -> {
            return tbl.doSchemaOutOpAsync(
                    ClientOp.TUPLE_UPSERT_ALL,
                    (s, w, n) -> ser.writeRecs(tx, batch, s, w, n, TuplePart.KEY_AND_VAL),
                    r -> null,
                    provider,
                    tx);
        };

        if (tx == null) {
            return clo.apply(recs, getPartitionAwarenessProvider(ser.mapper(), recs.iterator().next()), false);
        }

        return tbl.splitAndRun(recs, clo, null, (agg, cur) -> null,
                (schema, entry) -> getColocationHash(schema, ser.mapper(), entry));
    }

    /** {@inheritDoc} */
    @Override
    public R getAndUpsert(@Nullable Transaction tx, R rec) {
        return sync(getAndUpsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAndUpsertAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w, n) -> ser.writeRec(tx, rec, s, w, n, TuplePart.KEY_AND_VAL),
                (s, r) -> ser.readValRec(rec, s, r.in()),
                null,
                getPartitionAwarenessProvider(ser.mapper(), rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@Nullable Transaction tx, R rec) {
        return sync(insertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w, n) -> ser.writeRec(tx, rec, s, w, n, TuplePart.KEY_AND_VAL),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(ser.mapper(), rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public List<R> insertAll(@Nullable Transaction tx, Collection<R> recs) {
        return sync(insertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<R>> insertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        checkCollectionForNulls(recs, "recs", "rec");

        if (recs.isEmpty()) {
            return emptyListCompletedFuture();
        }

        MapFunction<R, List<R>> clo = (batch, provider, txRequired) -> {
            return tbl.doSchemaOutInOpAsync(
                    ClientOp.TUPLE_INSERT_ALL,
                    (s, w, n) -> ser.writeRecs(tx, batch, s, w, n, TuplePart.KEY_AND_VAL),
                    (s, r) -> ser.readRecs(s, r.in(), false, TuplePart.KEY_AND_VAL),
                    Collections.emptyList(),
                    provider,
                    tx);
        };

        if (tx == null) {
            return clo.apply(recs, getPartitionAwarenessProvider(ser.mapper(), recs.iterator().next()), false);
        }

        return tbl.splitAndRun(recs, clo, new ArrayList<>(recs.size()),
                (agg, cur) -> {
                    agg.addAll(cur);
                    return agg;
                },
                (schema, entry) -> getColocationHash(schema, ser.mapper(), entry));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, R rec) {
        return sync(replaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, R oldRec, R newRec) {
        return sync(replaceAsync(tx, oldRec, newRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w, n) -> ser.writeRec(tx, rec, s, w, n, TuplePart.KEY_AND_VAL),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(ser.mapper(), rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R oldRec, R newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w, n) -> ser.writeRecs(tx, oldRec, newRec, s, w, n, TuplePart.KEY_AND_VAL),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(ser.mapper(), oldRec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public R getAndReplace(@Nullable Transaction tx, R rec) {
        return sync(getAndReplaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAndReplaceAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w, n) -> ser.writeRec(tx, rec, s, w, n, TuplePart.KEY_AND_VAL),
                (s, r) -> ser.readValRec(rec, s, r.in()),
                null,
                getPartitionAwarenessProvider(ser.mapper(), rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@Nullable Transaction tx, R keyRec) {
        return sync(deleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, R keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w, n) -> ser.writeRec(tx, keyRec, s, w, n, TuplePart.KEY, true),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(ser.mapper(), keyRec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@Nullable Transaction tx, R rec) {
        return sync(deleteExactAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w, n) -> ser.writeRec(tx, rec, s, w, n, TuplePart.KEY_AND_VAL),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(ser.mapper(), rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public R getAndDelete(@Nullable Transaction tx, R keyRec) {
        return sync(getAndDeleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAndDeleteAsync(@Nullable Transaction tx, R keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w, n) -> ser.writeRec(tx, keyRec, s, w, n, TuplePart.KEY, true),
                (s, r) -> ser.readValRec(keyRec, s, r.in()),
                null,
                getPartitionAwarenessProvider(ser.mapper(), keyRec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public List<R> deleteAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return sync(deleteAllAsync(tx, keyRecs));
    }

    @Override
    public void deleteAll(@Nullable Transaction tx) {
        sync(deleteAllAsync(tx));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<R>> deleteAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        Objects.requireNonNull(keyRecs);

        if (keyRecs.isEmpty()) {
            return emptyListCompletedFuture();
        }

        MapFunction<R, List<R>> clo = (batch, provider, txRequired) -> {
            return tbl.doSchemaOutInOpAsync(
                    ClientOp.TUPLE_DELETE_ALL,
                    (s, w, n) -> ser.writeRecs(tx, batch, s, w, n, TuplePart.KEY, true),
                    (s, r) -> ser.readRecs(s, r.in(), false, TuplePart.KEY),
                    Collections.emptyList(),
                    provider,
                    tx);
        };

        if (tx == null) {
            return clo.apply(keyRecs, getPartitionAwarenessProvider(ser.mapper(), keyRecs.iterator().next()), false);
        }

        return tbl.splitAndRun(keyRecs, clo, new ArrayList<>(keyRecs.size()),
                (agg, cur) -> {
                    agg.addAll(cur);
                    return agg;
                },
                (schema, entry) -> getColocationHash(schema, ser.mapper(), entry));
    }

    @Override
    public CompletableFuture<Void> deleteAllAsync(@Nullable Transaction tx) {
        return sql.executeAsync(tx, "DELETE FROM " + tbl.name()).thenApply(r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public List<R> deleteAllExact(@Nullable Transaction tx, Collection<R> recs) {
        return sync(deleteAllExactAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<R>> deleteAllExactAsync(@Nullable Transaction tx, Collection<R> recs) {
        Objects.requireNonNull(recs);

        if (recs.isEmpty()) {
            return emptyListCompletedFuture();
        }

        MapFunction<R, List<R>> clo = (batch, provider, txRequired) -> {
            return tbl.doSchemaOutInOpAsync(
                    ClientOp.TUPLE_DELETE_ALL_EXACT,
                    (s, w, n) -> ser.writeRecs(tx, batch, s, w, n, TuplePart.KEY_AND_VAL),
                    (s, r) -> ser.readRecs(s, r.in(), false, TuplePart.KEY_AND_VAL),
                    Collections.emptyList(),
                    provider,
                    tx);
        };

        if (tx == null) {
            return clo.apply(recs, getPartitionAwarenessProvider(ser.mapper(), recs.iterator().next()), false);
        }

        return tbl.splitAndRun(recs, clo, new ArrayList<>(recs.size()),
                (agg, cur) -> {
                    agg.addAll(cur);
                    return agg;
                },
                (schema, entry) -> getColocationHash(schema, ser.mapper(), entry));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<R>> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        var provider = new PojoStreamerPartitionAwarenessProvider<>(tbl, ser.mapper());
        var opts = options == null ? DataStreamerOptions.DEFAULT : options;

        // Partition-aware (best effort) sender with retries.
        // The batch may go to a different node when a direct connection is not available.
        StreamerBatchSender<R, Integer, Void> batchSender = (partition, items, deleted) -> tbl.doSchemaOutOpAsync(
                ClientOp.STREAMER_BATCH_SEND,
                (s, w, n) -> ser.writeStreamerRecs(partition, items, deleted, s, w),
                r -> null,
                PartitionAwarenessProvider.of(partition),
                new RetryLimitPolicy().retryLimit(opts.retryLimit()),
                null);

        return ClientDataStreamer.streamData(publisher, opts, batchSender, provider, tbl);
    }

    @Override
    public <E, V, A, R1> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            DataStreamerReceiverDescriptor<V, A, R1> receiver,
            Function<E, R> keyFunc,
            Function<E, V> payloadFunc,
            @Nullable A receiverArg,
            @Nullable Flow.Subscriber<R1> resultSubscriber,
            @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);
        Objects.requireNonNull(keyFunc);
        Objects.requireNonNull(payloadFunc);
        Objects.requireNonNull(receiver);

        return ClientDataStreamer.streamData(
                publisher,
                keyFunc,
                payloadFunc,
                x -> false,
                options == null ? DataStreamerOptions.DEFAULT : options,
                new PojoStreamerPartitionAwarenessProvider<>(tbl, ser.mapper()),
                tbl,
                resultSubscriber,
                receiver,
                receiverArg
        );
    }

    /** {@inheritDoc} */
    @Override
    protected Function<SqlRow, R> queryMapper(ResultSetMetadata meta, ClientSchema schema) {
        String[] cols = columnNames(schema.columns());
        Marshaller marsh = schema.getMarshaller(ser.mapper(), TuplePart.KEY_AND_VAL, true);

        return (row) -> (R) marsh.readObject(new TupleReader(new SqlRowProjection(row, meta, cols)), null);
    }
}
