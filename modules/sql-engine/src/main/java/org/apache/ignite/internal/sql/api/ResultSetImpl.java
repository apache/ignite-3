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

package org.apache.ignite.internal.sql.api;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Synchronous result set implementation.
 */
public class ResultSetImpl implements ResultSet {
    private final AsyncResultSet ars;

    /**
     * Constructor.
     *
     * @param ars Asynchronous result set.
     */
    public ResultSetImpl(AsyncResultSet ars) {
        this.ars = ars;
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

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        ars.closeAsync().toCompletableFuture().get();
    }

    /** {@inheritDoc} */
    @NotNull
    @Override
    public Iterator<SqlRow> iterator() {
        return ars.currentPage().iterator();
    }

    private class IteratorImpl implements Iterator<SqlRow> {
        private AsyncResultSet cur;
        private CompletableFuture<? extends AsyncResultSet> fut;

        IteratorImpl(AsyncResultSet ars) {
            cur = ars;

            if (ars.hasMorePages()) {
                fut = cur.fetchNextPage().toCompletableFuture();
            }
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public SqlRow next() {
            return null;
        }
    }
}
