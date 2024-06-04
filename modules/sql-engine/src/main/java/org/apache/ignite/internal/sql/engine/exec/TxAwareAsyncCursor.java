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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.lang.SqlExceptionMapperUtil;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;

/**
 * Cursor that takes care of transaction used during query execution.
 *
 * @param <T> Type of the returned items.
 */
class TxAwareAsyncCursor<T> implements AsyncDataCursor<T> {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CompletableFuture<Void> closeResult = new CompletableFuture<>();

    private final QueryTransactionWrapper txWrapper;
    private final AsyncCursor<T> dataCursor;
    private final CompletableFuture<Void> firstPageReady;

    TxAwareAsyncCursor(
            QueryTransactionWrapper txWrapper,
            AsyncCursor<T> dataCursor,
            CompletableFuture<Void> firstPageReady
    ) {
        this.txWrapper = txWrapper;
        this.dataCursor = dataCursor;
        this.firstPageReady = firstPageReady;
    }

    @Override
    public CompletableFuture<BatchedResult<T>> requestNextAsync(int rows) {
        return dataCursor.requestNextAsync(rows)
                .handle((batch, error) -> {
                    if (error != null) {
                        return handleError(error).thenApply(none -> batch);
                    }

                    CompletableFuture<Void> fut = batch.hasMore()
                            ? nullCompletedFuture()
                            : closeAsync();

                    return fut.thenApply(none -> batch);
                })
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!closed.compareAndSet(false, true)) {
            return closeResult;
        }

        dataCursor.closeAsync()
                .thenCompose(ignored -> txWrapper.commitImplicit())
                .whenComplete((r, e) -> {
                    if (e != null) {
                        closeResult.completeExceptionally(e);
                    } else {
                        closeResult.complete(null);
                    }
                });

        return closeResult;
    }

    @Override
    public CompletableFuture<Void> onClose() {
        return closeResult;
    }

    @Override
    public CompletableFuture<Void> onFirstPageReady() {
        return firstPageReady.handle((none, executionError) -> {
            if (executionError != null) {
                return handleError(executionError);
            }

            return CompletableFutures.<Void>nullCompletedFuture();
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<Void> handleError(Throwable throwable) {
        Throwable wrapped = wrapIfNecessary(throwable);

        return txWrapper.rollback(throwable)
                .handle((none, rollbackError) -> {
                    if (rollbackError != null) {
                        wrapped.addSuppressed(rollbackError);
                    }

                    return closeAsync();
                })
                .thenCompose(Function.identity())
                .handle((none, closeError) -> {
                    if (closeError != null) {
                        wrapped.addSuppressed(closeError);
                    }

                    throw new CompletionException(wrapped);
                });
    }

    private static Throwable wrapIfNecessary(Throwable t) {
        Throwable err = ExceptionUtils.unwrapCause(t);

        return SqlExceptionMapperUtil.mapToPublicSqlException(err);
    }
}
