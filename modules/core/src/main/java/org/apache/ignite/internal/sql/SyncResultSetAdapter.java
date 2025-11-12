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

package org.apache.ignite.internal.sql;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Synchronous wrapper over {@link org.apache.ignite.sql.async.AsyncResultSet}.
 */
public class SyncResultSetAdapter<T> implements ResultSet<T> {
    /** Wrapped async result set. */
    private final AsyncResultSet<T> ars;

    /** Iterator. */
    private final IteratorImpl<T> it;

    /**
     * Constructor.
     *
     * @param ars Asynchronous result set.
     */
    public SyncResultSetAdapter(AsyncResultSet<T> ars) {
        assert ars != null;

        this.ars = ars;
        it = ars.hasRowSet() ? new IteratorImpl<>(ars) : null;
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
        sync(ars.closeAsync().toCompletableFuture());
    }

    private static <T> T sync(CompletableFuture<T> future) {
        return IgniteUtils.getInterruptibly(future);
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        if (it == null) {
            return false;
        }

        return it.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public T next() {
        if (it == null) {
            throw new NoRowSetExpectedException();
        }

        return it.next();
    }

    private static class IteratorImpl<T> implements Iterator<T> {
        private AsyncResultSet<T> curRes;

        private Iterator<T> curPage;

        IteratorImpl(AsyncResultSet<T> ars) {
            curRes = ars;
            curPage = ars.currentPage().iterator();
        }

        @Override
        public boolean hasNext() {
            if (curPage.hasNext()) {
                return true;
            }

            if (curRes == null || !curRes.hasMorePages()) {
                return false;
            }

            curRes = sync(curRes.fetchNextPage().toCompletableFuture());

            if (curRes == null) {
                return false;
            }

            curPage = curRes.currentPage().iterator();

            return curPage.hasNext();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return curPage.next();
        }
    }
}
