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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.lang.SqlExceptionMapperUtil;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Sql query cursor.
 *
 * @param <T> Type of elements.
 */
public class AsyncSqlCursorImpl<T> implements AsyncSqlCursor<T> {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CompletableFuture<Void> closeResult = new CompletableFuture<>();

    private final SqlQueryType queryType;
    private final ResultSetMetadata meta;
    private final QueryTransactionWrapper txWrapper;
    private final AsyncCursor<T> dataCursor;
    private final Runnable onClose;
    private final CompletableFuture<AsyncSqlCursor<T>> nextStatement;

    /**
     * Constructor.
     *
     * @param queryType Type of the query.
     * @param meta The meta of the result set.
     * @param txWrapper Transaction wrapper.
     * @param dataCursor The result set.
     * @param onClose Callback to invoke when cursor is closed.
     */
    public AsyncSqlCursorImpl(
            SqlQueryType queryType,
            ResultSetMetadata meta,
            QueryTransactionWrapper txWrapper,
            AsyncCursor<T> dataCursor,
            Runnable onClose
    ) {
        this(queryType, meta, txWrapper, dataCursor, onClose, null);
    }

    /**
     * Constructor.
     *
     * @param queryType Type of the query.
     * @param meta The meta of the result set.
     * @param txWrapper Transaction wrapper.
     * @param dataCursor The result set.
     * @param onClose Callback to invoke when cursor is closed.
     * @param nextStatement Next statement future, non-null in the case of a
     *         multi-statement query and if current statement is not the last.
     */
    AsyncSqlCursorImpl(
            SqlQueryType queryType,
            ResultSetMetadata meta,
            QueryTransactionWrapper txWrapper,
            AsyncCursor<T> dataCursor,
            Runnable onClose,
            @Nullable CompletableFuture<AsyncSqlCursor<T>> nextStatement
    ) {
        this.queryType = queryType;
        this.meta = meta;
        this.txWrapper = txWrapper;
        this.dataCursor = dataCursor;
        this.onClose = onClose;
        this.nextStatement = nextStatement;
    }

    /** {@inheritDoc} */
    @Override
    public SqlQueryType queryType() {
        return queryType;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BatchedResult<T>> requestNextAsync(int rows) {
        return dataCursor.requestNextAsync(rows)
                .thenApply(batch -> {
                    CompletableFuture<Void> fut = batch.hasMore()
                            ? nullCompletedFuture()
                            : closeAsync();

                    return fut.thenApply(none -> batch);
                })
                .exceptionally(rootEx -> {
                    // Always rollback a transaction in case of an error.
                    return txWrapper.rollback()
                            .handle((none, rollbackEx) -> {
                                Throwable wrapped = wrapIfNecessary(rootEx);

                                if (rollbackEx != null) {
                                    wrapped.addSuppressed(rollbackEx);
                                }

                                throw new CompletionException(wrapped);
                            });
                })
                .thenCompose(Function.identity());
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNextResult() {
        return nextStatement != null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncSqlCursor<T>> nextResult() {
        if (nextStatement == null) {
            throw new NoSuchElementException("Query has no more results");
        }

        return nextStatement;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!closed.compareAndSet(false, true)) {
            return closeResult;
        }

        dataCursor.closeAsync()
                .thenCompose(ignored -> txWrapper.commitImplicit())
                .thenRun(onClose)
                .whenComplete((r, e) -> {
                    if (e != null) {
                        closeResult.completeExceptionally(e);
                    } else {
                        closeResult.complete(null);
                    }
                });

        return closeResult;
    }

    private static Throwable wrapIfNecessary(Throwable t) {
        Throwable err = ExceptionUtils.unwrapCause(t);

        return SqlExceptionMapperUtil.mapToPublicSqlException(err);
    }
}
