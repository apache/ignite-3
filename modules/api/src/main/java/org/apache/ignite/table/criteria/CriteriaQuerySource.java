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
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Represents an entity that can be used to execute predicate-based criteria queries.
 *
 * @param <T> The type of elements returned by iterator.
 */
public interface CriteriaQuerySource<T> {
    /**
     * Executes predicate-based criteria query.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries from the underlying table.
     * @return Iterator with query results.
     * @throws CriteriaException If failed.
     */
    default Cursor<T> query(@Nullable Transaction tx, @Nullable Criteria criteria) {
        return query(tx, criteria, null, null);
    }

    /**
     * Executes predicate-based criteria query.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries from the underlying table.
     * @param indexName The name of the index to force usage in the query or {@code null} to use the default.
     * @return Iterator with query results.
     * @throws CriteriaException If failed.
     */
    default Cursor<T> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName) {
        return query(tx, criteria, indexName, null);
    }

    /**
     * Executes a predicate-based criteria query.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries from the underlying table.
     * @param indexName The name of the index to force usage in the query or {@code null} to use the default.
     * @param opts Criteria query options or {@code null} to use default.
     * @return Iterator with query results.
     * @throws CriteriaException If failed.
     */
    Cursor<T> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName, @Nullable CriteriaQueryOptions opts);

    /**
     * Executes a predicate-based criteria query in an asynchronous way.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries from the underlying table.
     * @return Future that represents the pending completion of the operation.
     * @throws CriteriaException If failed.
     */
    default CompletableFuture<AsyncCursor<T>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria) {
        return queryAsync(tx, criteria, null, null);
    }

    /**
     * Executes a predicate-based criteria query in an asynchronous way.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries from the underlying table.
     * @param indexName The name of the index to force usage in the query or {@code null} to use the default.
     * @return Future that represents the pending completion of the operation.
     * @throws CriteriaException If failed.
     */
    default CompletableFuture<AsyncCursor<T>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria,
            @Nullable String indexName) {
        return queryAsync(tx, criteria, indexName, null);
    }

    /**
     * Executes a predicate-based criteria query in an asynchronous way.
     *
     * @param tx Transaction to execute the query within or {@code null} to run within implicit transaction.
     * @param criteria The predicate to filter entries or {@code null} to return all entries from the underlying table.
     * @param indexName The name of the index to force usage in the query or {@code null} to use the default.
     * @param opts Criteria query options or {@code null} to use default.
     * @return Future that represents the pending completion of the operation.
     * @throws CriteriaException If failed.
     */
    CompletableFuture<AsyncCursor<T>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts);
}
