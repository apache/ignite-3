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

package org.apache.ignite.internal.restart;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Reference to {@link RecordView} under a swappable {@link Ignite} instance. When a restart happens, this switches to the new Ignite
 * instance.
 *
 * <p>API operations on this are linearized with respect to node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
class RestartProofRecordView<R> extends RestartProofApiObject<RecordView<R>> implements RecordView<R>, Wrapper {
    RestartProofRecordView(IgniteAttachmentLock attachmentLock, Ignite initialIgnite, Function<Ignite, RecordView<R>> viewFactory) {
        super(attachmentLock, initialIgnite, viewFactory);
    }

    @Override
    public R get(@Nullable Transaction tx, R keyRec) {
        return attached(view -> view.get(tx, keyRec));
    }

    @Override
    public CompletableFuture<R> getAsync(@Nullable Transaction tx, R keyRec) {
        return attachedAsync(view -> view.getAsync(tx, keyRec));
    }

    @Override
    public List<R> getAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return attached(view -> view.getAll(tx, keyRecs));
    }

    @Override
    public CompletableFuture<List<R>> getAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        return attachedAsync(view -> view.getAllAsync(tx, keyRecs));
    }

    @Override
    public boolean contains(@Nullable Transaction tx, R keyRec) {
        return attached(view -> view.contains(tx, keyRec));
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, R keyRec) {
        return attachedAsync(view -> view.containsAsync(tx, keyRec));
    }

    @Override
    public boolean containsAll(@Nullable Transaction tx, Collection<R> keys) {
        return attached(view -> view.containsAll(tx, keys));
    }

    @Override
    public CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<R> keys) {
        return attachedAsync(view -> view.containsAllAsync(tx, keys));
    }

    @Override
    public void upsert(@Nullable Transaction tx, R rec) {
        consumeAttached(view -> view.upsert(tx, rec));
    }

    @Override
    public CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, R rec) {
        return attachedAsync(view -> view.upsertAsync(tx, rec));
    }

    @Override
    public void upsertAll(@Nullable Transaction tx, Collection<R> recs) {
        consumeAttached(view -> view.upsertAll(tx, recs));
    }

    @Override
    public CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        return attachedAsync(view -> view.upsertAllAsync(tx, recs));
    }

    @Override
    public R getAndUpsert(@Nullable Transaction tx, R rec) {
        return attached(view -> view.getAndUpsert(tx, rec));
    }

    @Override
    public CompletableFuture<R> getAndUpsertAsync(@Nullable Transaction tx, R rec) {
        return attachedAsync(view -> view.getAndUpsertAsync(tx, rec));
    }

    @Override
    public boolean insert(@Nullable Transaction tx, R rec) {
        return attached(view -> view.insert(tx, rec));
    }

    @Override
    public CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, R rec) {
        return attachedAsync(view -> view.insertAsync(tx, rec));
    }

    @Override
    public List<R> insertAll(@Nullable Transaction tx, Collection<R> recs) {
        return attached(view -> view.insertAll(tx, recs));
    }

    @Override
    public CompletableFuture<List<R>> insertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        return attachedAsync(view -> view.insertAllAsync(tx, recs));
    }

    @Override
    public boolean replace(@Nullable Transaction tx, R rec) {
        return attached(view -> view.replace(tx, rec));
    }

    @Override
    public boolean replaceExact(@Nullable Transaction tx, R oldRec, R newRec) {
        return attached(view -> view.replaceExact(tx, oldRec, newRec));
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R rec) {
        return attachedAsync(view -> view.replaceAsync(tx, rec));
    }

    @Override
    public CompletableFuture<Boolean> replaceExactAsync(@Nullable Transaction tx, R oldRec, R newRec) {
        return attachedAsync(view -> view.replaceExactAsync(tx, oldRec, newRec));
    }

    @Override
    public R getAndReplace(@Nullable Transaction tx, R rec) {
        return attached(view -> view.getAndReplace(tx, rec));
    }

    @Override
    public CompletableFuture<R> getAndReplaceAsync(@Nullable Transaction tx, R rec) {
        return attachedAsync(view -> view.getAndReplaceAsync(tx, rec));
    }

    @Override
    public boolean delete(@Nullable Transaction tx, R keyRec) {
        return attached(view -> view.delete(tx, keyRec));
    }

    @Override
    public CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, R keyRec) {
        return attachedAsync(view -> view.deleteAsync(tx, keyRec));
    }

    @Override
    public boolean deleteExact(@Nullable Transaction tx, R rec) {
        return attached(view -> view.deleteExact(tx, rec));
    }

    @Override
    public CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, R rec) {
        return attachedAsync(view -> view.deleteExactAsync(tx, rec));
    }

    @Override
    public R getAndDelete(@Nullable Transaction tx, R keyRec) {
        return attached(view -> view.getAndDelete(tx, keyRec));
    }

    @Override
    public CompletableFuture<R> getAndDeleteAsync(@Nullable Transaction tx, R keyRec) {
        return attachedAsync(view -> view.getAndDeleteAsync(tx, keyRec));
    }

    @Override
    public List<R> deleteAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return attached(view -> view.deleteAll(tx, keyRecs));
    }

    @Override
    public void deleteAll(@Nullable Transaction tx) {
        consumeAttached(view -> view.deleteAll(tx));
    }

    @Override
    public CompletableFuture<List<R>> deleteAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        return attachedAsync(view -> view.deleteAllAsync(tx, keyRecs));
    }

    @Override
    public CompletableFuture<Void> deleteAllAsync(@Nullable Transaction tx) {
        return attachedAsync(view -> view.deleteAllAsync(tx));
    }

    @Override
    public List<R> deleteAllExact(@Nullable Transaction tx, Collection<R> recs) {
        return attached(view -> view.deleteAllExact(tx, recs));
    }

    @Override
    public CompletableFuture<List<R>> deleteAllExactAsync(@Nullable Transaction tx, Collection<R> recs) {
        return attachedAsync(view -> view.deleteAllExactAsync(tx, recs));
    }

    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<R>> publisher, @Nullable DataStreamerOptions options) {
        return attachedAsync(view -> view.streamData(publisher, options));
    }

    @Override
    public <E, V, A, R1> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            DataStreamerReceiverDescriptor<V, A, R1> receiver,
            Function<E, R> keyFunc,
            Function<E, V> payloadFunc,
            @Nullable A receiverArg,
            @Nullable Subscriber<R1> resultSubscriber,
            @Nullable DataStreamerOptions options) {
        return attachedAsync(view -> view.streamData(publisher, receiver, keyFunc, payloadFunc, receiverArg, resultSubscriber, options));
    }

    // TODO: IGNITE-23011 - support cursor transparency?

    @Override
    public Cursor<R> query(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts
    ) {
        return attached(view -> view.query(tx, criteria, indexName, opts));
    }

    @Override
    public CompletableFuture<AsyncCursor<R>> queryAsync(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts
    ) {
        return attachedAsync(view -> view.queryAsync(tx, criteria, indexName, opts));
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attached(view -> Wrappers.unwrap(view, classToUnwrap));
    }
}
