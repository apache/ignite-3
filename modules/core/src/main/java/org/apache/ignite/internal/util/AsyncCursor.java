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

package org.apache.ignite.internal.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.CursorClosedException;

/**
 * Asynchronous cursor.
 *
 * @param <T> Type of elements.
 */
public interface AsyncCursor<T> {
    /**
     * Request next batch of rows.
     *
     * <p>Several calls to this method should be chained and resulting stages should be completed in the order of invocation. Any call
     * to this method after call to {@link #closeAsync()} should be completed immediately with
     * {@link CursorClosedException} even if the future returned by {@link #closeAsync()} is not completed yet.
     *
     * @param rows Desired amount of rows.
     * @return A completion stage that will be completed with batch of size {@code rows} or less if there is no more data.
     * @throws CursorClosedException If cursor is closed.
     */
    CompletableFuture<BatchedResult<T>> requestNextAsync(int rows);

    /**
     * Releases resources acquired by the cursor.
     *
     * @return A future which will be completed when the resources will be actually released.
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Batch of the items.
     *
     * @param <T> Type of the item.
     */
    class BatchedResult<T> {
        private final List<T> items;

        private final boolean hasMore;

        /**
         * Constructor.
         *
         * @param items Batch of items.
         * @param hasMore Whether there is at least one more row in the cursor.
         */
        public BatchedResult(List<T> items, boolean hasMore) {
            this.items = items;
            this.hasMore = hasMore;
        }

        /** Returns items of this batch. */
        public List<T> items() {
            return items;
        }

        /** Returns {@code true} in case this cursor has more data. */
        public boolean hasMore() {
            return hasMore;
        }
    }
}
