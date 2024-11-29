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

package org.apache.ignite.internal.sql.common.cancel.api;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Handler that can cancel operations of a certain type.
 *
 * <p>When registering a handler in the {@link CancelHandlerRegistry registry}, you must specify
 * whether the handler can cancel the operation on the local node only or on the entire cluster.
 *
 * @see CancelHandlerRegistry#register(CancellableOperationType, OperationCancelHandler, boolean)
 */
@FunctionalInterface
public interface OperationCancelHandler {
    /**
     * Cancels an operation with the specified ID.
     *
     * @param objectId ID of the operation to cancel.
     * @return {@code true} if the operation was successfully canceled,
     *         {@code false} if a specific operation was not found or is inactive.
     */
    CompletableFuture<Boolean> cancelAsync(UUID objectId);
}
