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

import static org.apache.ignite.internal.util.CollectionUtils.mapIterable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper over {@link AsyncResultSet} for criteria queries.
 * <ul>
 * <li>Close {@link Session} when wrapped {@link AsyncResultSet} is closed</li>
 * <li>Map objects contained by wrapped result set to target value</li>
 * </ul>
 */
public class QueryCriteriaAsyncResultSet<T, R> implements AsyncResultSet<T> {
    private final Session session;

    @Nullable
    private final Function<R, T> mapper;

    private final AsyncResultSet<R> ars;

    /**
     * Constructor.
     *
     * @param session Session instance.
     * @param mapper Mapper.
     * @param ars Asynchronous result set.
     */
    public QueryCriteriaAsyncResultSet(Session session, @Nullable Function<R, T> mapper, AsyncResultSet<R> ars) {
        this.session = session;
        this.mapper = mapper;
        this.ars = ars;
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<T> currentPage() {
        return mapIterable(ars.currentPage(), mapper, null);
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
                .thenApply((rs) -> new QueryCriteriaAsyncResultSet<>(session, mapper, rs));
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
