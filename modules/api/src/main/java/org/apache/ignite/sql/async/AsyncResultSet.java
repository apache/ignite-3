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
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.CursorClosedException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Provides methods for processing query results in an asynchronous way.
 *
 * <p>Usage example:
 * <pre><code>
 *      private CompletionStage&lt;Void&gt; fetchAllRowsInto(
 *          AsyncResultSet resultSet,
 *          List&lt;SqlRow&gt; target
 *      ) {
 *          for (var row : resultSet.currentPage()) {
 *              target.add(row);
 *          }
 *
 *          if (!resultSet.hasMorePages()) {
 *              return CompletableFuture.completedFuture(null);
 *          }
 *
 *           return resultSet.fetchNextPage()
 *              .thenCompose(res -&gt; fetchAllRowsInto(res, target));
 *      }
 * </code></pre>
 *
 * @param <T> A type of the objects contained by this result set (when row set is present). This will be either {@link SqlRow}
 *     if no explicit mapper is provided or a particular type defined by supplied mapper.
 *
 * @see ResultSet
 * @see IgniteSql#executeAsync(Transaction, String, Object...)
 * @see IgniteSql#executeAsync(Transaction, Mapper, String, Object...)
 */
public interface AsyncResultSet<T> extends AsyncCursor<T> {
    /**
     * Returns metadata for query results. If the result set contains rows ({@link #hasRowSet()}, returns {@code true}).
     * If not applicable, returns {@code null}.
     *
     * @return ResultSet Metadata.
     * @see ResultSet#metadata()
     */
    @Nullable ResultSetMetadata metadata();

    /**
     * Defines whether the result of a query is a collection of rows or not.
     *
     * <p>Note: If the method returns {@code false}, calling {@link #currentPage()} will fail, and either {@link #affectedRows()} return
     * number of affected rows or {@link #wasApplied()} returns {@code true}.
     *
     * @return {@code True} if a query returned rows, {@code false} otherwise.
     */
    boolean hasRowSet();

    /**
     * Returns the number of rows affected by the DML statement execution (such as "INSERT", "UPDATE", etc.),
     * or {@code 0} if the statement returns nothing (such as "ALTER TABLE", etc), or {@code -1} if inapplicable.
     *
     * <p>Note: If the method returns {@code -1}, either {@link #hasRowSet()} or {@link #wasApplied()} returns {@code true}.
     *
     * @return Number of rows affected by the query, or {@code 0} if the statement returns nothing, or {@code -1} if not applicable.
     * @see ResultSet#affectedRows()
     */
    long affectedRows();

    /**
     * Indicates whether the query that had produced the result was a conditional query.
     * E.g., for query "Create table if not exists", the method returns {@code true} if
     * the operation was successful or {@code false} if the operation was ignored because the table already existed.
     *
     * <p>Note: If the method returns {@code false}, then either {@link #affectedRows()} returns the number of
     * affected rows, or {@link #hasRowSet()} returns {@code true}, or the conditional DDL query is not applied.
     *
     * @return {@code True} if a conditional query is applied, {@code false} otherwise.
     * @see ResultSet#wasApplied()
     */
    boolean wasApplied();

    /**
     * Returns the current page content if the query returns rows.
     *
     * @return Iterable set of rows.
     * @throws NoRowSetExpectedException if no row set is returned.
     */
    @Override
    Iterable<T> currentPage();

    /**
     * Returns the current page size if the query return rows.
     *
     * @return The size of {@link #currentPage()}.
     * @throws NoRowSetExpectedException if no row set is returned.
     */
    @Override
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
     */
    @Override
    CompletableFuture<? extends AsyncResultSet<T>> fetchNextPage();

    /**
     * Invalidates a query result, stops the query, and cleans up query resources.
     *
     * @return Operation future.
     */
    @Override
    CompletableFuture<Void> closeAsync();
}
