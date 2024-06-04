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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.thread.PublicApiThreading.preventThreadHijack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.table.AntiHijackAsyncCursor;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link AsyncResultSet} which prevents Ignite internal threads from being hijacked by the user via asynchronous methods.
 *
 * @see PublicApiThreading
 */
public class AntiHijackAsyncResultSet<T> extends AntiHijackAsyncCursor<T> implements AsyncResultSet<T>, Wrapper {
    private final AsyncResultSet<T> resultSet;
    private final Executor asyncContinuationExecutor;

    /**
     * Constructor.
     */
    AntiHijackAsyncResultSet(AsyncResultSet<T> resultSet, Executor asyncContinuationExecutor) {
        super(resultSet, asyncContinuationExecutor);

        this.resultSet = resultSet;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public @Nullable ResultSetMetadata metadata() {
        return resultSet.metadata();
    }

    @Override
    public boolean hasRowSet() {
        return resultSet.hasRowSet();
    }

    @Override
    public long affectedRows() {
        return resultSet.affectedRows();
    }

    @Override
    public boolean wasApplied() {
        return resultSet.wasApplied();
    }

    @Override
    public CompletableFuture<? extends AsyncResultSet<T>> fetchNextPage() {
        return preventThreadHijack(resultSet.fetchNextPage(), asyncContinuationExecutor)
                .thenApply(resultSet -> new AntiHijackAsyncResultSet<>(resultSet, asyncContinuationExecutor));
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(resultSet);
    }
}
