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

package org.apache.ignite.internal.table.distributed.replicator;

import java.util.concurrent.CompletableFuture;

/**
 * Helper class for counting the number of RW transactions operations.
 *
 * <p>{@link #operationsFuture()} needs to be completed from the outside.</p>
 */
class TxRwOperationCounter {
    private final long operationCount;

    private final CompletableFuture<Void> completeOperationFuture;

    private TxRwOperationCounter(long operationCount, CompletableFuture<Void> completeOperationFuture) {
        this.operationCount = operationCount;
        this.completeOperationFuture = completeOperationFuture;
    }

    /** Returns new RW transactions operations with {@code 1} operation. */
    static TxRwOperationCounter withCountOne() {
        return new TxRwOperationCounter(1, new CompletableFuture<>());
    }

    /** Returns a new counter with the count of RW transactions operations incremented by {@code 1}. */
    TxRwOperationCounter incrementOperationCount() {
        assert operationCount > 0 : operationCount;

        return new TxRwOperationCounter(operationCount + 1, completeOperationFuture);
    }

    /** Returns a new counter with the count of RW transactions operations decremented by {@code 1}. */
    TxRwOperationCounter decrementOperationCount() {
        assert operationCount > 0 : operationCount;

        return new TxRwOperationCounter(operationCount - 1, completeOperationFuture);
    }

    /** Returns true if the count of RW transactions operations is {@code 0}. */
    boolean isOperationsOver() {
        return operationCount == 0;
    }

    /**
     * Returns a future that will complete when the count of RW transaction operations is {@code 0}, should be completed from the outside.
     */
    CompletableFuture<Void> operationsFuture() {
        return completeOperationFuture;
    }
}
