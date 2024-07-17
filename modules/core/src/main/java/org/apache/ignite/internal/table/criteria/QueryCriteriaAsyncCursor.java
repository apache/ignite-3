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

import static org.apache.ignite.internal.table.criteria.CriteriaExceptionMapperUtil.mapToPublicCriteriaException;
import static org.apache.ignite.internal.util.CollectionUtils.mapIterable;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper over {@link AsyncResultSet} for criteria queries.
 *
 * @param <R> A type of the objects contained by wrapped result set. This will be either {@link SqlRow} if no explicit mapper is provided
 *      or a particular type defined by supplied mapper.
 * @param <T> The type of elements returned by this cursor.
 */
public class QueryCriteriaAsyncCursor<T, R> implements AsyncCursor<T> {
    private final AsyncResultSet<R> ars;

    @Nullable
    private final Function<R, T> mapper;

    private final Runnable closeRun;

    /**
     * Constructor.
     *
     * @param ars Asynchronous result set.
     * @param mapper Conversion function for objects contained by result set (if {@code null}, conversion isn't required).
     * @param closeRun Callback to be invoked after result set is closed.
     */
    public QueryCriteriaAsyncCursor(AsyncResultSet<R> ars, @Nullable Function<R, T> mapper, Runnable closeRun) {
        this.ars = ars;
        this.mapper = mapper;
        this.closeRun = closeRun;
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<T> currentPage() {
        try {
            return mapIterable(ars.currentPage(), mapper, null);
        } catch (NoRowSetExpectedException e) {
            throw sneakyThrow(mapToPublicCriteriaException(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public int currentPageSize() {
        try {
            return ars.currentPageSize();
        } catch (NoRowSetExpectedException e) {
            throw sneakyThrow(mapToPublicCriteriaException(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<? extends AsyncCursor<T>> fetchNextPage() {
        return ars.fetchNextPage()
                .thenApply((rs) -> {
                    if (!hasMorePages()) {
                        closeAsync();
                    }

                    return this;
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
