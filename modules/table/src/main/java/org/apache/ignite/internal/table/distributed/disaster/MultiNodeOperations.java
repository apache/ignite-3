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

package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.LOCAL_NODE_ERR;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;

/** Used to poll statuses of multi node operations from other nodes. */
class MultiNodeOperations {
    private final Map<UUID, CompletableFuture<Void>> operationsById = new ConcurrentHashMap<>();

    /**
     * Used to track if new request should be sent. Incremented when new operation is added or last request didn't return all operations.
     * Decremented after new request is sent, by value last seen by the sender. If not zero, new request should be sent.
     */
    private final AtomicInteger accumulatedChanges = new AtomicInteger(0);

    Set<UUID> operationsIds() {
        return operationsById.keySet();
    }

    /** Adds new operation to be tracked. Increments accumulated changes. */
    void add(UUID operationId, CompletableFuture<Void> operationFuture) {
        operationsById.put(operationId, operationFuture);
        accumulatedChanges.incrementAndGet();
    }

    /** Removes operation tracking. Doesn't affect versioning. */
    void remove(UUID operationId) {
        operationsById.remove(operationId);
    }

    /** Returns number of accumulated changes since last request was sent. */
    int accumulatedChanges() {
        return accumulatedChanges.get();
    }

    /** Returns operation future by its id. */
    CompletableFuture<Void> get(UUID operationId) {
        return operationsById.get(operationId);
    }

    /** Decrements accumulated changes by the number of changes seen by the sender. */
    void markLatestVersionSent(int sentVersion) {
        accumulatedChanges.addAndGet(-sentVersion);
    }

    /** Last request didn't return all requested operations, so we need to send a new one. */
    void shouldSendNewRequest() {
        accumulatedChanges.incrementAndGet();
    }

    /** Completes all tracked operations with a given exception. */
    void exceptionally(Throwable ex) {
        for (CompletableFuture<Void> fut : operationsById.values()) {
            fut.completeExceptionally(new DisasterRecoveryException(
                    LOCAL_NODE_ERR,
                    "Couldn't get multi node operation status: " + ex.getMessage()
            ));
        }
    }
}
