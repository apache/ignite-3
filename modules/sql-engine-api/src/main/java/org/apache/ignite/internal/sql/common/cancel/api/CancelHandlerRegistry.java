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

/**
 * Registry of handlers that can cancel a specific operation.
 */
public interface CancelHandlerRegistry {
    /**
     * Registers a cancel handler.
     *
     * @param type Type of the cancellable operation.
     * @param handler Handler to register.
     * @param local {@code True} if the handler can cancel operations only on local node,
     *              {@code false} if the handler can cancel operations across the entire cluster.
     */
    void register(CancellableOperationType type, OperationCancelHandler handler, boolean local);

    /**
     * Returns a handler that can cancel an operation of the specified type across the entire cluster.
     *
     * @param type Type of the cancellable operation.
     * @return Handler that can cancel an operation of the specified type across the entire cluster.
     */
    OperationCancelHandler handler(CancellableOperationType type);
}
