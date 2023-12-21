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
import org.apache.ignite.sql.async.AsyncClosableCursor;
import org.apache.ignite.sql.async.AsyncResultSet;

/**
 * Wrapper over {@link AsyncResultSet} for criteria queries.
 */
public class QueryCriteriaAsyncCursor<T> implements AsyncClosableCursor<T> {
    private final AsyncResultSet<T> ars;

    private final Runnable closeRun;

    /**
     * Constructor.
     *
     * @param ars Asynchronous result set.
     * @param closeRun Callback to be invoked after result is closed.
     */
    public QueryCriteriaAsyncCursor(AsyncResultSet<? extends T> ars, Runnable closeRun) {
        this.ars = (AsyncResultSet<T>) ars;
        this.closeRun = closeRun;
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
        return ars.fetchNextPage()
            .whenComplete((v, t) -> {
                if (t == null && !hasMorePages()) {
                    closeRun.run();
                }
            });
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return ars.hasMorePages();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        return ars.closeAsync().thenRun(closeRun);
    }
}
