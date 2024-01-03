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

package org.apache.ignite.table.criteria;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a criteria query facade that can be used to executes a query to a record view using a predicate.
 *
 * @param <T> The type of elements returned by iterator.
 */
public interface CriteriaQuerySource<T> {
    /**
     * Executes a query to a record view using a predicate.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries in record view.
     * @return Iterator with query results.
     * @throws IgniteException If failed.
     */
    default Cursor<T> query(@Nullable Transaction tx, @Nullable Criteria criteria) {
        return query(tx, criteria, CriteriaQueryOptions.DEFAULT);
    }

    /**
     * Executes a query to a record view using a predicate.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries in record view.
     * @param opts Criteria query options.
     * @return Iterator with query results.
     * @throws IgniteException If failed.
     */
    Cursor<T> query(@Nullable Transaction tx, @Nullable Criteria criteria, CriteriaQueryOptions opts);

    /**
     * Executes a query to a record view using a predicate in an asynchronous way.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries in record view.
     * @return Future that represents the pending completion of the operation.
     * @throws IgniteException If failed.
     */
    default CompletableFuture<AsyncCursor<T>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria) {
        return queryAsync(tx, criteria, CriteriaQueryOptions.DEFAULT);
    }

    /**
     * Executes a query to a record view using a predicate in an asynchronous way.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries in record view.
     * @param opts Criteria query options.
     * @return Future that represents the pending completion of the operation.
     * @throws IgniteException If failed.
     */
    CompletableFuture<AsyncCursor<T>> queryAsync(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            CriteriaQueryOptions opts
    );
}
