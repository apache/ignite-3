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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.lang.SqlExceptionMapperUtil;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Sql query cursor.
 *
 * @param <T> Type of elements.
 */
public class AsyncSqlCursorImpl<T> implements AsyncSqlCursor<T> {
    private final SqlQueryType queryType;
    private final ResultSetMetadata meta;
    private final QueryTransactionWrapper txWrapper;
    private final AsyncCursor<T> dataCursor;
    private final Runnable onClose;

    /**
     * Constructor.
     *
     * @param queryType Type of the query.
     * @param meta The meta of the result set.
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
        this.queryType = queryType;
        this.meta = meta;
        this.txWrapper = txWrapper;
        this.dataCursor = dataCursor;
        this.onClose = onClose;
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
        return dataCursor.requestNextAsync(rows).handle((batch, t) -> {
            if (t != null) {
                // Always rollback a transaction in case of an error.
                txWrapper.rollback();

                throw new CompletionException(wrapIfNecessary(t));
            }

            if (!batch.hasMore()) {
                closeAsync();
            }

            return batch;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        // Commit implicit transaction, if any.
        txWrapper.commitImplicit();

        onClose.run();

        return dataCursor.closeAsync();
    }

    private static Throwable wrapIfNecessary(Throwable t) {
        Throwable err = ExceptionUtils.unwrapCause(t);

        return SqlExceptionMapperUtil.mapToPublicSqlException(err);
    }
}
