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

package org.apache.ignite.sql.async;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.sql.CursorClosedException;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.criteria.CriteriaQuerySource;

/**
 * Provides methods for iterate over query results in an asynchronous way.
 *
 * @param <T> The type of elements returned by this iterator.
 *
 * @see CriteriaQuerySource
 */
public interface AsyncClosableCursor<T> {
    /**
     * Returns the current page content if the query returns rows.
     *
     * @return Iterable set of rows.
     * @throws NoRowSetExpectedException If no row set is returned.
     */
    Iterable<T> currentPage();

    /**
     * Returns the current page size if the query return rows.
     *
     * @return The size of {@link #currentPage()}.
     * @throws NoRowSetExpectedException If no row set is returned.
     */
    int currentPageSize();

    /**
     * Fetches the next page of results asynchronously.
     * The current page is changed after the future completion.
     * The methods {@link #currentPage()}, {@link #currentPageSize()}, {@link #hasMorePages()}
     * use the current page and return consistent results between complete last page future and call {@code fetchNextPage}.
     *
     * @return A future which will be completed when next page will be fetched and set as the current page.
     *     The future will return {@code this} for chaining.
     * @throws NoRowSetExpectedException If no row set is expected as a query result.
     * @throws CursorClosedException If cursor is closed.
     * @throws SqlException If there are no more pages.
     */
    CompletableFuture<? extends AsyncClosableCursor<T>> fetchNextPage();

    /**
     * Indicates whether there are more pages of results.
     *
     * @return {@code True} if there are more pages with results, {@code false} otherwise.
     */
    boolean hasMorePages();

    /**
     * Invalidates a query result, stops the query, and cleans up query resources.
     *
     * @return A future which will be completed when the resources will be actually released.
     */
    CompletableFuture<Void> closeAsync();
}
