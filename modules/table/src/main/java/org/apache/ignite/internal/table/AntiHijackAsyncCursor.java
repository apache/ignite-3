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

import static org.apache.ignite.internal.thread.PublicApiThreading.preventThreadHijack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.lang.AsyncCursor;

/**
 * Wrapper around {@link AsyncCursor} which prevents Ignite internal threads from being hijacked by the user via asynchronous methods.
 *
 * @see PublicApiThreading
 */
public class AntiHijackAsyncCursor<T> implements AsyncCursor<T> {
    private final AsyncCursor<T> cursor;

    private final Executor asyncContinuationExecutor;

    public AntiHijackAsyncCursor(AsyncCursor<T> cursor, Executor asyncContinuationExecutor) {
        this.cursor = cursor;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public Iterable<T> currentPage() {
        return cursor.currentPage();
    }

    @Override
    public int currentPageSize() {
        return cursor.currentPageSize();
    }

    @Override
    public CompletableFuture<? extends AsyncCursor<T>> fetchNextPage() {
        return preventThreadHijack(cursor.fetchNextPage(), asyncContinuationExecutor)
                .thenApply(nextCursor -> new AntiHijackAsyncCursor<>(nextCursor, asyncContinuationExecutor));
    }

    @Override
    public boolean hasMorePages() {
        return cursor.hasMorePages();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return preventThreadHijack(cursor.closeAsync(), asyncContinuationExecutor);
    }
}
