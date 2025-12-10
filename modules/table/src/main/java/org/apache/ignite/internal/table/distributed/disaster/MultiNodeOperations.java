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

import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.REMOTE_NODE_ERR;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;

/** Used to poll statuses of multi node operations from other nodes. */
class MultiNodeOperations {
    private final Map<UUID, CompletableFuture<Void>> operationsById = new ConcurrentHashMap<>();

    private final AtomicBoolean shouldPoll = new AtomicBoolean(true);

    /** Returns ids of all tracked operations. */
    Set<UUID> operationsIds() {
        return Set.copyOf(operationsById.keySet());
    }

    /** Adds new operation to track. Requires new polling request to be sent. */
    void add(UUID operationId, CompletableFuture<Void> operationFuture) {
        operationsById.put(operationId, operationFuture);

        shouldPoll.set(true);
    }

    /**
     * Removes operation tracking. Doesn't require new polling request.
     *
     * @return Removed operation future.
     */
    CompletableFuture<Void> remove(UUID operationId) {
        return operationsById.remove(operationId);
    }

    /** Returns operation future by operation id. */
    CompletableFuture<Void> get(UUID operationId) {
        return operationsById.get(operationId);
    }

    /** Marks that new polling request should be sent. */
    void triggerNextRequest() {
        shouldPoll.set(true);
    }

    /** Returns {@code true} if new request should be sent, and unsets the {@link #shouldPoll} flag. */
    boolean startPollingIfNeeded() {
        return shouldPoll.getAndSet(false);
    }

    /** Completes all tracked operations with a given exception. */
    void exceptionally(Throwable ex) {
        Set<UUID> operationIds = Set.copyOf(operationsById.keySet());

        for (UUID operationId : operationIds) {
            operationsById.get(operationId).completeExceptionally(new DisasterRecoveryException(
                    REMOTE_NODE_ERR,
                    "Couldn't get multi node operation status: " + ex.getMessage()
            ));

            operationsById.remove(operationId);
        }
    }
}
