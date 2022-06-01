/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.concurrent.CompletionStage;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.jetbrains.annotations.Nullable;

/**
 * Asynchronous result set provides methods for query results processing in asynchronous way.
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
 * @see ResultSet
 */
public interface AsyncResultSet {
    /**
     * Returns metadata for the results if the result contains rows ({@link #hasRowSet()} returns {@code true}), or {@code null} if
     * inapplicable.
     *
     * @return ResultSet metadata.
     * @see ResultSet#metadata()
     */
    @Nullable ResultSetMetadata metadata();

    /**
     * Returns whether the result of the query execution is a collection of rows, or not.
     *
     * <p>Note: when returns {@code false}, then calling {@link #currentPage()} will failed, and either {@link #affectedRows()} return
     * number of affected rows or {@link #wasApplied()} returns {@code true}.
     *
     * @return {@code True} if the query returns rows, {@code false} otherwise.
     */
    boolean hasRowSet();

    /**
     * Returns number of rows affected by the DML statement execution (such as "INSERT", "UPDATE", etc.), or {@code 0} if statement return
     * nothing (such as "ALTER TABLE", etc) or {@code -1} if inapplicable.
     *
     * <p>Note: when returns {@code -1}, then either {@link #hasRowSet()} or {@link #wasApplied()} returns {@code true}.
     *
     * @return Number of rows affected by the query, or {@code 0} if statement return nothing, or {@code -1} if inapplicable.
     * @see ResultSet#affectedRows()
     */
    long affectedRows();

    /**
     * Returns whether the query that produce this result was a conditional query, or not. E.g. for the query "Create table if not exists"
     * the method returns {@code true} when an operation was applied successfully, and {@code false} when an operation was ignored due to
     * table was already existed.
     *
     * <p>Note: when returns {@code false}, then either {@link #affectedRows()} return number of affected rows or {@link #hasRowSet()}
     * returns {@code true} or conditional DDL query is not applied.
     *
     * @return {@code True} if conditional query applied, {@code false} otherwise.
     * @see ResultSet#wasApplied()
     */
    boolean wasApplied();

    /**
     * Returns the current page content if the query return rows.
     *
     * @return Iterable over rows.
     * @throws NoRowSetExpectedException if no row set is expected as a query result.
     */
    Iterable<SqlRow> currentPage();

    /**
     * Returns the current page size if the query return rows.
     *
     * @return The size of {@link #currentPage()}.
     * @throws NoRowSetExpectedException if no row set is expected as a query result.
     */
    int currentPageSize();

    /**
     * Fetch the next page of results asynchronously.
     * The future that is completed with the same {@code AsyncResultSet} object.
     * The current page is changed after the future complete.
     * The methods {@link #currentPage()}, {@link #currentPageSize()}, {@link #hasMorePages()}
     * use current page and return consistent results between complete last page future and call {@code fetchNextPage}.
     *
     * @return Operation future.
     * @throws NoRowSetExpectedException if no row set is expected as a query result.
     */
    CompletionStage<? extends AsyncResultSet> fetchNextPage();

    /**
     * Returns whether there are more pages of results.
     *
     * @return {@code True} if there are more pages with results, {@code false} otherwise.
     */
    boolean hasMorePages();

    /**
     * Invalidates query result, stops the query and cleanups query resources.
     *
     * @return Operation future.
     */
    CompletionStage<Void> closeAsync();
}
