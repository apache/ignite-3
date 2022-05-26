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
import java.util.concurrent.CompletionStage;
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
    public void close() {
        SessionImpl.await(ars.closeAsync().toCompletableFuture());
    }

    /** {@inheritDoc} */
    @NotNull
    @Override
    public Iterator<SqlRow> iterator() {
        return new IteratorImpl(ars);
    }

    private static class IteratorImpl implements Iterator<SqlRow> {
        private AsyncResultSet curRes;

        private CompletionStage<? extends AsyncResultSet> nextPageStage;

        private Iterator<SqlRow> curPage;

        IteratorImpl(AsyncResultSet ars) {
            curRes = ars;

            advance();
        }

        @Override
        public boolean hasNext() {
            if (curPage.hasNext()) {
                return true;
            } else if (nextPageStage != null) {
                curRes = SessionImpl.await(nextPageStage);

                advance();

                return curPage.hasNext();
            } else {
                return false;
            }
        }

        private void advance() {
            curPage = curRes.currentPage().iterator();

            if (curRes.hasMorePages()) {
                nextPageStage = curRes.fetchNextPage();
            } else {
                nextPageStage = null;
            }
        }

        @Override
        public SqlRow next() {
            return curPage.next();
        }
    }
}
