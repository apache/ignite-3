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

import java.lang.reflect.Constructor;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.metadata.RemoteException;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.NotNull;
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
        return dataCursor.requestNextAsync(rows).handle((batch, t) -> {
            if (t != null) {
                if (implicitTx != null) {
                    implicitTx.rollback();
                }

                throw wrapIfNecessary(t);
            }

            if (implicitTx != null && !batch.hasMore()) {
                // last batch, need to commit transaction
                implicitTx.commit();
            }

            return batch;
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        return dataCursor.closeAsync();
    }

    private static RuntimeException wrapIfNecessary(@NotNull Throwable t) {
        Throwable cause = unwrapRemoteCause(t);

        // If the cause is IgniteException then create
        // an exception of the same type with the same properties
        // and set its cause to the original exception.
        if (cause instanceof IgniteException) {
            return preserveExceptionType((IgniteException) cause, t);
        } else {
            // If the cause is not a subclass of IgniteException, wrap it in IgniteException.
            return IgniteException.wrap(t);
        }
    }

    private static Throwable unwrapRemoteCause(@NotNull Throwable t) {
        Throwable err = t;

        while (err != null) {
            err = ExceptionUtils.unwrapCause(err);
            // Unwrap RemoteExceptions because they are just wrappers.
            if (err instanceof RemoteException) {
                err = err.getCause();
                continue;
            }

            return err;
        }

        return t;
    }

    private static IgniteException preserveExceptionType(IgniteException e, Throwable t) {
        // Return IgniteException as is
        if (e.getClass() == IgniteException.class) {
            return e;
        }

        try {
            Constructor<?> ctor = e.getClass().getDeclaredConstructor(UUID.class, int.class, String.class, Throwable.class);

            return (IgniteException) ctor.newInstance(e.traceId(), e.code(), e.getMessage(), t);
        } catch (Exception ex) {
            throw new RuntimeException("IgniteException-derived class does not have required constructor: " + e.getClass().getName(), ex);
        }
    }
}
