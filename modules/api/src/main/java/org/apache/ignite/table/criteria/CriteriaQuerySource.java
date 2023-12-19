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
import org.apache.ignite.sql.ClosableCursor;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.async.AsyncClosableCursor;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Represents criteria query facade which can be used to iterate through table entries filtered by some criterion.
 *
 * @param <T> The type of elements returned by iterator.
 */
public interface CriteriaQuerySource<T> {
    /**
     * Filter table entries by some criterion.
     *
     * @param tx Transaction to filter table entries within or {@code null} to run within implicit transaction.
     * @param criteria Will accept all the entries if {@code null}.
     * @return Iterator for table entries filtered by some criterion.
     * @throws SqlException If failed.
     */
    default ClosableCursor<T> queryCriteria(@Nullable Transaction tx, @Nullable Criteria criteria) {
        return queryCriteria(tx, criteria, CriteriaQueryOptions.DEFAULT);
    }

    /**
     * Filter table entries by some criterion.
     *
     * @param tx Transaction to filter table entries within or {@code null} to run within implicit transaction.
     * @param criteria Will accept all the entries if {@code null}.
     * @param opts Criteria query options.
     * @return Iterator for table entries filtered by some criterion.
     * @throws SqlException If failed.
     */
    ClosableCursor<T> queryCriteria(@Nullable Transaction tx, @Nullable Criteria criteria, CriteriaQueryOptions opts);

    /**
     * Filter table entries by some criterion in an asynchronous way.
     *
     * @param tx Transaction to filter table entries within or {@code null} to run within implicit transaction.
     * @param criteria Will accept all the entries if {@code null}.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    default CompletableFuture<? extends AsyncClosableCursor<T>> queryCriteriaAsync(@Nullable Transaction tx, @Nullable Criteria criteria) {
        return queryCriteriaAsync(tx, criteria, CriteriaQueryOptions.DEFAULT);
    }

    /**
     * Filter table entries by some criterion in an asynchronous way.
     *
     * @param tx Transaction to filter table entries within or {@code null} to run within implicit transaction.
     * @param criteria Will accept all the entries if {@code null}.
     * @param opts Criteria query options.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<? extends AsyncClosableCursor<T>> queryCriteriaAsync(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            CriteriaQueryOptions opts
    );
}
