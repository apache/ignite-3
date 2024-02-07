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

package org.apache.ignite.internal.table.criteria;

import static org.apache.ignite.internal.util.ExceptionUtils.copyExceptionWithCause;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.Iterator;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;

/**
 * Synchronous wrapper over {@link AsyncCursor}.
 */
public class CursorAdapter<T> implements Cursor<T> {
    /** Wrapped asynchronous cursor. */
    private final AsyncCursor<T> ac;

    /** Iterator. */
    private final Iterator<T> it;

    /**
     * Constructor.
     *
     * @param ac Asynchronous cursor.
     */
    public CursorAdapter(AsyncCursor<T> ac) {
        assert ac != null;

        this.ac = ac;
        this.it = new IteratorImpl<>(ac);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        try {
            ac.closeAsync().toCompletableFuture().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag.

            throw sneakyThrow(unwrapCause(e));
        } catch (ExecutionException e) {
            throw sneakyThrow(copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public T next() {
        return it.next();
    }

    private static class IteratorImpl<T> implements Iterator<T> {
        private AsyncCursor<T> curRes;

        private CompletionStage<? extends AsyncCursor<T>> nextPageStage;

        private Iterator<T> curPage;

        IteratorImpl(AsyncCursor<T> ars) {
            curRes = ars;

            advance();
        }

        @Override
        public boolean hasNext() {
            if (curPage.hasNext()) {
                return true;
            } else if (nextPageStage != null) {
                try {
                    curRes = nextPageStage.toCompletableFuture().join();
                } catch (CompletionException e) {
                    throw sneakyThrow(copyExceptionWithCause(e));
                }

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
        public T next() {
            return curPage.next();
        }
    }
}
