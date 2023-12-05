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

package org.apache.ignite.internal.table.criteria;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper over {@link AsyncResultSet} that close {@link Session} when {@link AsyncResultSet} is closed.
 */
public class ClosableSessionAsyncResultSet<T> implements AsyncResultSet<T> {
    /** Session instance. */
    private final Session session;

    /** Wrapped async result set. */
    private final AsyncResultSet<T> ars;

    /**
     * Constructor.
     *
     * @param session Session instance.
     * @param ars Asynchronous result set.
     */
    public ClosableSessionAsyncResultSet(Session session, AsyncResultSet<? extends T> ars) {
        this.session = session;
        this.ars = (AsyncResultSet<T>) ars;
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<T> currentPage() {
        return ars.currentPage();
    }

    /** {@inheritDoc} */
    @Override
    public int currentPageSize() {
        return ars.currentPageSize();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<? extends AsyncResultSet<T>> fetchNextPage() {
        return ars.fetchNextPage();
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return ars.hasMorePages();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        return ars.closeAsync()
                .thenCompose((ignored) -> session.closeAsync());
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ResultSetMetadata metadata() {
        return ars.metadata();
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasRowSet() {
        return ars.hasRowSet();
    }

    /** {@inheritDoc} */
    @Override
    public long affectedRows() {
        return ars.affectedRows();
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasApplied() {
        return ars.wasApplied();
    }
}
