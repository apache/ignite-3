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

import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
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
 * @see Session#executeAsync(Transaction, String, Object...)
 * @see Session#executeAsync(Transaction, Mapper, String, Object...)
 */
public interface AsyncResultSet<T> extends AsyncClosableCursor<T> {
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

}
