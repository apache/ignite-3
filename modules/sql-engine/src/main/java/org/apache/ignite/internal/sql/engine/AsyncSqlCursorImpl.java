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

import static org.apache.ignite.internal.tracing.TracingManager.wrap;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Sql query cursor.
 *
 * @param <T> Type of elements.
 */
public class AsyncSqlCursorImpl<T> implements AsyncSqlCursor<T> {
    private final SqlQueryType queryType;
    private final ResultSetMetadata meta;
    private final AsyncDataCursor<T> dataCursor;
    private final @Nullable CompletableFuture<AsyncSqlCursor<T>> nextStatement;

    /**
     * Constructor.
     *
     * @param queryType Type of the query.
     * @param meta The meta of the result set.
     * @param dataCursor The result set.
     * @param nextStatement Next statement future, non-null in the case of a
     *         multi-statement query and if current statement is not the last.
     */
    public AsyncSqlCursorImpl(
            SqlQueryType queryType,
            ResultSetMetadata meta,
            AsyncDataCursor<T> dataCursor,
            @Nullable CompletableFuture<AsyncSqlCursor<T>> nextStatement
    ) {
        this.queryType = queryType;
        this.meta = meta;
        this.dataCursor = dataCursor;
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
        return wrap(dataCursor.requestNextAsync(rows));
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
        return dataCursor.closeAsync();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> onClose() {
        return dataCursor.onClose();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> onFirstPageReady() {
        return dataCursor.onFirstPageReady();
    }
}
