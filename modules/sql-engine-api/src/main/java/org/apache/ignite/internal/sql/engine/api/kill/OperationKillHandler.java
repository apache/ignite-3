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

package org.apache.ignite.internal.sql.engine.api.kill;

import java.util.concurrent.CompletableFuture;

/**
 * Handler that can abort operations of a certain type.
 *
 * @see KillHandlerRegistry#register(OperationKillHandler)
 */
public interface OperationKillHandler {
    /**
     * Cancels an operation with the specified ID.
     *
     * @param operationId ID of the operation to cancel.
     * @return {@code true} if the operation was successfully canceled,
     *         {@code false} if a specific operation was not found.
     *
     * @throws IllegalArgumentException If the operation identifier is not in the correct format.
     */
    CompletableFuture<Boolean> cancelAsync(String operationId);

    /**
     * Returns whether the handler can abort operations only on local node or across the entire cluster.
     *
     * <p>If a handler can only abort operations on the local node, then distributed coordination will
     * be performed by the SQL engine.
     *
     * @return {@code True} if the handler can abort operations only on local node,
     *         {@code false} if the handler can abort operations across the entire cluster.
     */
    boolean local();

    /**
     * Returns the type of the operation that this handler can cancel.
     */
    CancellableOperationType type();
}
