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

import static org.apache.ignite.internal.client.ClientUtils.sync;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

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
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.marshaling.ByteArrayMarshaler;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.ReceiverDescriptor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client record view implementation for binary user-object representation.
 */
public class ClientRecordBinaryView extends AbstractClientView<Tuple> implements RecordView<Tuple> {
    /** Tuple serializer. */
    private final ClientTupleSerializer ser;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param sql Sql.
     */
    ClientRecordBinaryView(ClientTable tbl, ClientSql sql) {
        super(tbl, sql);

        ser = new ClientTupleSerializer(tbl.tableId());
    }

    /** {@inheritDoc} */
    @Override
    public Tuple get(@Nullable Transaction tx, Tuple keyRec) {
        return sync(getAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAsync(@Nullable Transaction tx, Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w) -> ser.writeTuple(tx, keyRec, s, w, true),
                (s, r) -> ClientTupleSerializer.readTuple(s, r.in(), false),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keyRec),
                tx);
    }

    @Override
    public List<Tuple> getAll(@Nullable Transaction tx, Collection<Tuple> keyRecs) {
        return sync(getAllAsync(tx, keyRecs));
    }

    @Override
    public CompletableFuture<List<Tuple>> getAllAsync(@Nullable Transaction tx, Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        if (keyRecs.isEmpty()) {
            return emptyListCompletedFuture();
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (s, w) -> ser.writeTuples(tx, keyRecs, s, w, true),
                (s, r) -> ClientTupleSerializer.readTuplesNullable(s, r.in()),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keyRecs.iterator().next()),
                tx
        );
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, Tuple key) {
        return sync(containsAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_CONTAINS_KEY,
                (s, w) -> ser.writeTuple(tx, key, s, w, true),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@Nullable Transaction tx, Tuple rec) {
        sync(upsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> ser.writeTuple(tx, rec, s, w),
                r -> null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@Nullable Transaction tx, Collection<Tuple> recs) {
        sync(upsertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        if (recs.isEmpty()) {
            return nullCompletedFuture();
        }

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> ser.writeTuples(tx, recs, s, w, false),
                r -> null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, recs.iterator().next()),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndUpsert(@Nullable Transaction tx, Tuple rec) {
        return sync(getAndUpsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndUpsertAsync(@Nullable Transaction tx, Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> ser.writeTuple(tx, rec, s, w, false),
                (s, r) -> ClientTupleSerializer.readTuple(s, r.in(), false),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@Nullable Transaction tx, Tuple rec) {
        return sync(insertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> ser.writeTuple(tx, rec, s, w, false),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public List<Tuple> insertAll(@Nullable Transaction tx, Collection<Tuple> recs) {
        return sync(insertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Tuple>> insertAllAsync(@Nullable Transaction tx, Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        if (recs.isEmpty()) {
            return emptyListCompletedFuture();
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_INSERT_ALL,
                (s, w) -> ser.writeTuples(tx, recs, s, w, false),
                (s, r) -> ClientTupleSerializer.readTuples(s, r.in()),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, recs.iterator().next()),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, Tuple rec) {
        return sync(replaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, Tuple oldRec, Tuple newRec) {
        return sync(replaceAsync(tx, oldRec, newRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> ser.writeTuple(tx, rec, s, w, false),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, Tuple oldRec, Tuple newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> {
                    ser.writeTuple(tx, oldRec, s, w, false, false);
                    ser.writeTuple(tx, newRec, s, w, false, true);
                },
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, oldRec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndReplace(@Nullable Transaction tx, Tuple rec) {
        return sync(getAndReplaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndReplaceAsync(@Nullable Transaction tx, Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> ser.writeTuple(tx, rec, s, w, false),
                (s, r) -> ClientTupleSerializer.readTuple(s, r.in(), false),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@Nullable Transaction tx, Tuple keyRec) {
        return sync(deleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w) -> ser.writeTuple(tx, keyRec, s, w, true),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keyRec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@Nullable Transaction tx, Tuple rec) {
        return sync(deleteExactAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> ser.writeTuple(tx, rec, s, w, false),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, rec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndDelete(@Nullable Transaction tx, Tuple keyRec) {
        return sync(getAndDeleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndDeleteAsync(@Nullable Transaction tx, Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> ser.writeTuple(tx, keyRec, s, w, true),
                (s, r) -> ClientTupleSerializer.readTuple(s, r.in(), false),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keyRec),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public List<Tuple> deleteAll(@Nullable Transaction tx, Collection<Tuple> keyRecs) {
        return sync(deleteAllAsync(tx, keyRecs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Tuple>> deleteAllAsync(@Nullable Transaction tx, Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        if (keyRecs.isEmpty()) {
            return emptyListCompletedFuture();
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                (s, w) -> ser.writeTuples(tx, keyRecs, s, w, true),
                (s, r) -> ClientTupleSerializer.readTuples(s, r.in(), true),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keyRecs.iterator().next()),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public List<Tuple> deleteAllExact(@Nullable Transaction tx, Collection<Tuple> recs) {
        return sync(deleteAllExactAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Tuple>> deleteAllExactAsync(@Nullable Transaction tx, Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        if (recs.isEmpty()) {
            return emptyListCompletedFuture();
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL_EXACT,
                (s, w) -> ser.writeTuples(tx, recs, s, w, false),
                (s, r) -> ClientTupleSerializer.readTuples(s, r.in()),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, recs.iterator().next()),
                tx);
    }


    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<Tuple>> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        var provider = new TupleStreamerPartitionAwarenessProvider(tbl);
        var opts = options == null ? DataStreamerOptions.DEFAULT : options;

        // Partition-aware (best effort) sender with retries.
        // The batch may go to a different node when a direct connection is not available.
        StreamerBatchSender<Tuple, Integer, Void> batchSender = (partitionId, items, deleted) -> tbl.doSchemaOutOpAsync(
                ClientOp.STREAMER_BATCH_SEND,
                (s, w) -> ser.writeStreamerTuples(partitionId, items, deleted, s, w),
                r -> null,
                PartitionAwarenessProvider.of(partitionId),
                new RetryLimitPolicy().retryLimit(opts.retryLimit()),
                null);

        return ClientDataStreamer.streamData(publisher, opts, batchSender, provider, tbl);
    }

    /** {@inheritDoc} */
    @Override
    public <E, V, R> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            Function<E, Tuple> keyFunc,
            Function<E, V> payloadFunc,
            ReceiverDescriptor receiver,
            @Nullable Flow.Subscriber<R> resultSubscriber,
            @Nullable DataStreamerOptions options,
            Object receiverArgs) {
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
                new TupleStreamerPartitionAwarenessProvider(tbl),
                tbl,
                resultSubscriber,
                receiver.units(),
                receiver.receiverClassName(),
                receiverArgs,
                ByteArrayMarshaler.create()
        );
    }
}
