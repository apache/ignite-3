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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerTarget;
import org.apache.ignite.table.ReceiverDescriptor;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

abstract class PublicApiThreadingViewBase<T> implements DataStreamerTarget<T>, CriteriaQuerySource<T> {
    private final DataStreamerTarget<T> streamerTarget;
    private final CriteriaQuerySource<T> querySource;

    private final Executor asyncContinuationExecutor;

    PublicApiThreadingViewBase(
            DataStreamerTarget<T> streamerTarget,
            CriteriaQuerySource<T> querySource,
            Executor asyncContinuationExecutor
    ) {
        this.streamerTarget = streamerTarget;
        this.querySource = querySource;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<T>> publisher, @Nullable DataStreamerOptions options) {
        return executeAsyncOp(() -> streamerTarget.streamData(publisher, options));
    }

    @Override
    public <E, V, R> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            Function<E, T> keyFunc,
            Function<E, V> payloadFunc,
            ReceiverDescriptor receiver,
            @Nullable Flow.Subscriber<R> resultSubscriber,
            @Nullable DataStreamerOptions options,
            Object... receiverArgs) {
        return executeAsyncOp(() -> streamerTarget.streamData(
                publisher,
                keyFunc,
                payloadFunc,
                receiver,
                resultSubscriber,
                options,
                receiverArgs));
    }

    @Override
    public Cursor<T> query(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts
    ) {
        return executeSyncOp(() -> querySource.query(tx, criteria, indexName, opts));
    }

    @Override
    public CompletableFuture<AsyncCursor<T>> queryAsync(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts
    ) {
        return executeAsyncOp(() -> querySource.queryAsync(tx, criteria, indexName, opts))
                .thenApply(cursor -> new AntiHijackAsyncCursor<>(cursor, asyncContinuationExecutor));
    }

    final <U> CompletableFuture<U> executeAsyncOp(Supplier<CompletableFuture<U>> operation) {
        CompletableFuture<U> future = PublicApiThreading.execUserAsyncOperation(operation);

        return PublicApiThreading.preventThreadHijack(future, asyncContinuationExecutor);
    }

    static <T> T executeSyncOp(Supplier<T> operation) {
        return PublicApiThreading.execUserSyncOperation(operation);
    }

    static void executeSyncOp(Runnable operation) {
        PublicApiThreading.execUserSyncOperation(operation);
    }
}
