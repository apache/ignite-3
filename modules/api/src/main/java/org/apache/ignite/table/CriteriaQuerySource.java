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

package org.apache.ignite.table;

import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Represents an object which can be queried with criteria.
 *
 * @param <T> Entry type.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface CriteriaQuerySource<T> {
    /**
     * Criteria query over cache entries.
     *
     * @param tx Transaction or {@code null} to auto-commit.
     * @param criteria If {@code null} then all entries will be returned.
     */
    default CriteriaQueryCursor<T> queryCriteria(@Nullable Transaction tx, @Nullable Criteria criteria) {
        return queryCriteria(tx, criteria, CriteriaQueryOptions.DEFAULT);
    }

    /**
     * Criteria query over cache entries.
     *
     * @param tx Transaction or {@code null} to auto-commit.
     * @param criteria If {@code null} then all entries will be returned.
     * @param opts Criteria query options.
     */
    CriteriaQueryCursor<T> queryCriteria(@Nullable Transaction tx, @Nullable Criteria criteria, CriteriaQueryOptions opts);
}
