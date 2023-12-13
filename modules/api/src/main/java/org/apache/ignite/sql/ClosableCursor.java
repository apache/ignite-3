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

package org.apache.ignite.sql;

import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.tx.Transaction;

/**
 * An iterator over a query results.
 *
 * @param <T> The type of elements returned by this iterator.
 *
 * @see KeyValueView#queryCriteria(Transaction, Criteria, CriteriaQueryOptions)
 * @see RecordView#queryCriteria(Transaction, Criteria, CriteriaQueryOptions)
 */
public interface ClosableCursor<T> extends Iterator<T>, AutoCloseable {
    /**
     * Returns a sequential Stream over the elements covered by this iterator.
     *
     * @return Sequential Stream over the elements covered by this iterator.
     */
    default Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), false);
    }

    /** {@inheritDoc} */
    @Override
    void close();
}
