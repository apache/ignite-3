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

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.RemoteOperationException;
import org.jetbrains.annotations.Nullable;

/** Contains operations that should be processed by remote node. */
class MultiNodeOperations {
    private final Map<UUID, CompletableFuture<Void>> operationsById = new ConcurrentHashMap<>();

    /** Adds new operation to track. */
    void add(UUID operationId, CompletableFuture<Void> operationFuture) {
        operationsById.put(operationId, operationFuture);
    }

    /**
     * Removes operation tracking.
     *
     * @return Removed operation future.
     */
    CompletableFuture<Void> remove(UUID operationId) {
        return operationsById.remove(operationId);
    }

    /** Completes all tracked operations with a given exception. */
    void completeAllExceptionally(String nodeName, Throwable e) {
        Set<UUID> operationIds = Set.copyOf(operationsById.keySet());

        for (UUID operationId : operationIds) {
            operationsById.remove(operationId).completeExceptionally(new RemoteOperationException(e.getMessage(), nodeName));
        }
    }

    /** Completes operation successfully or with {@link RemoteOperationException} using provided nodeName and exception message. */
    public void complete(UUID operationId, String nodeName, @Nullable String exceptionMessage) {
        CompletableFuture<Void> operationFuture = operationsById.remove(operationId);

        if (operationFuture != null) {
            if (exceptionMessage != null) {
                operationFuture.completeExceptionally(new RemoteOperationException(exceptionMessage, nodeName));
            } else {
                operationFuture.complete(null);
            }
        }
    }

    /** If there are no ongoing operations. */
    public boolean isEmpty() {
        return operationsById.isEmpty();
    }
}
