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
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.lang.SqlExceptionMapperUtil;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
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
    private final Function<CancellationReason, CompletableFuture<Void>> closeHandler;
    private final Consumer<Throwable> errorListener;

    TxAwareAsyncCursor(
            QueryTransactionWrapper txWrapper,
            AsyncCursor<T> dataCursor,
            CompletableFuture<Void> firstPageReady,
            Function<CancellationReason, CompletableFuture<Void>> closeHandler,
            Consumer<Throwable> errorListener
    ) {
        this.txWrapper = txWrapper;
        this.dataCursor = dataCursor;
        this.firstPageReady = firstPageReady;
        this.closeHandler = closeHandler;
        this.errorListener = errorListener;
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
    public CompletableFuture<Void> cancelAsync(CancellationReason reason) {
        if (!closed.compareAndSet(false, true)) {
            return closeResult;
        }

        closeHandler.apply(reason)
                .thenCompose(ignored -> {
                    if (reason != CancellationReason.CLOSE) {
                        String message = reason == CancellationReason.TIMEOUT
                                ? QueryCancelledException.TIMEOUT_MSG
                                : QueryCancelledException.CANCEL_MSG;

                        QueryCancelledException cancelEx = new QueryCancelledException(message);

                        errorListener.accept(cancelEx);

                        return txWrapper.finalise(cancelEx);
                    }

                    return txWrapper.finalise();
                })
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
    public CompletableFuture<Void> closeAsync() {
        return cancelAsync(CancellationReason.CLOSE);
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

        errorListener.accept(throwable);

        return txWrapper.finalise(throwable)
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
