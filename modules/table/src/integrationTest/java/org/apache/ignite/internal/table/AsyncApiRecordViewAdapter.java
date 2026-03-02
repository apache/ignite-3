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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Function;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.DataStreamerTarget;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Adapter for {@link RecordView} to run async methods using sync methods.
 *
 *<p>NOTE: Class does not support {@link CriteriaQuerySource} and {@link DataStreamerTarget} methods.
 */
public class AsyncApiRecordViewAdapter<V> implements RecordView<V> {
    private final RecordView<V> delegate;

    AsyncApiRecordViewAdapter(RecordView<V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public V get(@Nullable Transaction tx, V keyRec) {
        return await(delegate.getAsync(tx, keyRec));
    }

    @Override
    public List<V> getAll(@Nullable Transaction tx, Collection<V> keyRecs) {
        return await(delegate.getAllAsync(tx, keyRecs));
    }

    @Override
    public boolean contains(@Nullable Transaction tx, V keyRec) {
        return await(delegate.containsAsync(tx, keyRec));
    }

    @Override
    public boolean containsAll(@Nullable Transaction tx, Collection<V> keys) {
        return await(delegate.containsAllAsync(tx, keys));
    }

    @Override
    public void upsert(@Nullable Transaction tx, V rec) {
        await(delegate.upsertAsync(tx, rec));
    }

    @Override
    public void upsertAll(@Nullable Transaction tx, Collection<V> recs) {
        await(delegate.upsertAllAsync(tx, recs));
    }

    @Override
    public V getAndUpsert(@Nullable Transaction tx, V rec) {
        return await(delegate.getAndUpsertAsync(tx, rec));
    }

    @Override
    public boolean insert(@Nullable Transaction tx, V rec) {
        return await(delegate.insertAsync(tx, rec));
    }

    @Override
    public List<V> insertAll(@Nullable Transaction tx, Collection<V> recs) {
        return await(delegate.insertAllAsync(tx, recs));
    }

    @Override
    public boolean replace(@Nullable Transaction tx, V rec) {
        return await(delegate.replaceAsync(tx, rec));
    }

    @Override
    public boolean replaceExact(@Nullable Transaction tx, V oldRec, V newRec) {
        return await(delegate.replaceExactAsync(tx, oldRec, newRec));
    }

    @Override
    public V getAndReplace(@Nullable Transaction tx, V rec) {
        return await(delegate.getAndReplaceAsync(tx, rec));
    }

    @Override
    public boolean delete(@Nullable Transaction tx, V keyRec) {
        return await(delegate.deleteAsync(tx, keyRec));
    }

    @Override
    public boolean deleteExact(@Nullable Transaction tx, V rec) {
        return await(delegate.deleteExactAsync(tx, rec));
    }

    @Override
    public V getAndDelete(@Nullable Transaction tx, V keyRec) {
        return await(delegate.getAndDeleteAsync(tx, keyRec));
    }

    @Override
    public List<V> deleteAll(@Nullable Transaction tx, Collection<V> keyRecs) {
        return await(delegate.deleteAllAsync(tx, keyRecs));
    }

    @Override
    public void deleteAll(@Nullable Transaction tx) {
        await(delegate.deleteAllAsync(tx));
    }

    @Override
    public List<V> deleteAllExact(@Nullable Transaction tx, Collection<V> recs) {
        return await(delegate.deleteAllExactAsync(tx, recs));
    }

    @Override
    public CompletableFuture<V> getAsync(@Nullable Transaction tx, V keyRec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<List<V>> getAllAsync(@Nullable Transaction tx, Collection<V> keyRecs) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, V keyRec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<V> keys) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, V rec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, Collection<V> recs) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<V> getAndUpsertAsync(@Nullable Transaction tx, V rec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, V rec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<List<V>> insertAllAsync(@Nullable Transaction tx, Collection<V> recs) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, V rec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> replaceExactAsync(@Nullable Transaction tx, V oldRec, V newRec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, V rec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, V keyRec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, V rec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<V> getAndDeleteAsync(@Nullable Transaction tx, V keyRec) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<List<V>> deleteAllAsync(@Nullable Transaction tx, Collection<V> keyRecs) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Void> deleteAllAsync(@Nullable Transaction tx) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<List<V>> deleteAllExactAsync(@Nullable Transaction tx, Collection<V> recs) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public Cursor<V> query(@Nullable Transaction tx, @Nullable Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<V> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<V> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<AsyncCursor<V>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<AsyncCursor<V>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<AsyncCursor<V>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<V>> publisher, @Nullable DataStreamerOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E, V1, A, R> CompletableFuture<Void> streamData(Publisher<E> publisher, DataStreamerReceiverDescriptor<V1, A, R> receiver,
            Function<E, V> keyFunc, Function<E, V1> payloadFunc, @Nullable A receiverArg, @Nullable Subscriber<R> resultSubscriber,
            @Nullable DataStreamerOptions options) {
        throw new UnsupportedOperationException();
    }
}
