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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.client.ClientUtils.sync;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client record view implementation.
 */
public class ClientRecordView<R> implements RecordView<R> {
    /** Underlying table. */
    private final ClientTable tbl;

    /** Serializer. */
    private final ClientRecordSerializer<R> ser;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     * @param recMapper Mapper.
     */
    ClientRecordView(ClientTable tbl, Mapper<R> recMapper) {
        this.tbl = tbl;
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
                (s, w) -> ser.writeRec(tx, keyRec, s, w, TuplePart.KEY),
                (s, r) -> ser.readValRec(keyRec, s, r),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), keyRec));
    }

    @Override
    public List<R> getAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return sync(getAllAsync(tx, keyRecs));
    }

    @Override
    public CompletableFuture<List<R>> getAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        Objects.requireNonNull(keyRecs);

        if (keyRecs.isEmpty()) {
            return completedFuture(Collections.emptyList());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (s, w) -> ser.writeRecs(tx, keyRecs, s, w, TuplePart.KEY),
                (s, r) -> ser.readRecs(s, r, true, TuplePart.KEY_AND_VAL),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), keyRecs.iterator().next())
        );
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
                (s, w) -> ser.writeRec(tx, rec, s, w, TuplePart.KEY_AND_VAL),
                r -> null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), rec));
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@Nullable Transaction tx, Collection<R> recs) {
        sync(upsertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        Objects.requireNonNull(recs);

        if (recs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> ser.writeRecs(tx, recs, s, w, TuplePart.KEY_AND_VAL),
                r -> null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), recs.iterator().next()));
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
                (s, w) -> ser.writeRec(tx, rec, s, w, TuplePart.KEY_AND_VAL),
                (s, r) -> ser.readValRec(rec, s, r),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), rec));
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
                (s, w) -> ser.writeRec(tx, rec, s, w, TuplePart.KEY_AND_VAL),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), rec));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> insertAll(@Nullable Transaction tx, Collection<R> recs) {
        return sync(insertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<R>> insertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        Objects.requireNonNull(recs);

        if (recs.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_INSERT_ALL,
                (s, w) -> ser.writeRecs(tx, recs, s, w, TuplePart.KEY_AND_VAL),
                (s, r) -> ser.readRecs(s, r, false, TuplePart.KEY_AND_VAL),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), recs.iterator().next()));
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
                (s, w) -> ser.writeRec(tx, rec, s, w, TuplePart.KEY_AND_VAL),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R oldRec, R newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> ser.writeRecs(tx, oldRec, newRec, s, w, TuplePart.KEY_AND_VAL),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), oldRec));
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
                (s, w) -> ser.writeRec(tx, rec, s, w, TuplePart.KEY_AND_VAL),
                (s, r) -> ser.readValRec(rec, s, r),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), rec));
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
                (s, w) -> ser.writeRec(tx, keyRec, s, w, TuplePart.KEY),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), keyRec));
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
                (s, w) -> ser.writeRec(tx, rec, s, w, TuplePart.KEY_AND_VAL),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), rec));
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
                (s, w) -> ser.writeRec(tx, keyRec, s, w, TuplePart.KEY),
                (s, r) -> ser.readValRec(keyRec, s, r),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return sync(deleteAllAsync(tx, keyRecs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<R>> deleteAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        Objects.requireNonNull(keyRecs);

        if (keyRecs.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                (s, w) -> ser.writeRecs(tx, keyRecs, s, w, TuplePart.KEY),
                (s, r) -> ser.readRecs(s, r, false, TuplePart.KEY),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), keyRecs.iterator().next()));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAllExact(@Nullable Transaction tx, Collection<R> recs) {
        return sync(deleteAllExactAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<R>> deleteAllExactAsync(@Nullable Transaction tx, Collection<R> recs) {
        Objects.requireNonNull(recs);

        if (recs.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL_EXACT,
                (s, w) -> ser.writeRecs(tx, recs, s, w, TuplePart.KEY_AND_VAL),
                (s, r) -> ser.readRecs(s, r, false, TuplePart.KEY_AND_VAL),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, ser.mapper(), recs.iterator().next()));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<R> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        var provider = new PojoStreamerPartitionAwarenessProvider<>(tbl, ser.mapper());
        var opts = options == null ? DataStreamerOptions.DEFAULT : options;

        // Partition-aware (best effort) sender with retries.
        // The batch may go to a different node when a direct connection is not available.
        StreamerBatchSender<R, String> batchSender = (nodeId, items) -> tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> ser.writeRecs(null, items, s, w, TuplePart.KEY_AND_VAL),
                r -> null,
                PartitionAwarenessProvider.of(nodeId),
                new RetryLimitPolicy().retryLimit(opts.retryLimit()));

        return ClientDataStreamer.streamData(publisher, opts, batchSender, provider, tbl);
    }
}
