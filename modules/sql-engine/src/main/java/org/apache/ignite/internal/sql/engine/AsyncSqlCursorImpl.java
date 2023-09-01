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

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteExceptionMapperUtil;
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
    private final @Nullable InternalTransaction implicitTx;
    private final AsyncCursor<T> dataCursor;

    /**
     * Constructor.
     *
     * @param queryType Type of the query.
     * @param meta The meta of the result set.
     * @param dataCursor The result set.
     */
    public AsyncSqlCursorImpl(
            SqlQueryType queryType,
            ResultSetMetadata meta,
            @Nullable InternalTransaction implicitTx,
            AsyncCursor<T> dataCursor
    ) {
        this.queryType = queryType;
        this.meta = meta;
        this.implicitTx = implicitTx;
        this.dataCursor = dataCursor;
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
        Context context = Context.current();
        return dataCursor.requestNextAsync(rows).handle(context.wrapFunction((batch, t) -> {
            if (t != null) {
                if (implicitTx != null) {
                    implicitTx.rollback();
                }

                throw new CompletionException(wrapIfNecessary(t));
            }

            if (implicitTx != null && !batch.hasMore()) {
                // last batch, need to commit transaction
                implicitTx.commit();
            }

            return batch;
        }));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        // Commit implicit transaction, if any.
        if (implicitTx != null) {
            implicitTx.commit();
        }
        return dataCursor.closeAsync();
    }

    private static Throwable wrapIfNecessary(Throwable t) {
        Throwable err = ExceptionUtils.unwrapCause(t);

        return IgniteExceptionMapperUtil.mapToPublicException(err);
    }
}
