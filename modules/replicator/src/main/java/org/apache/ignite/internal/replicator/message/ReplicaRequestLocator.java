/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.replicator.message;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * The class identifies a replication request as a part of a business transaction.
 */
public class ReplicaRequestLocator {
    /**
     * Operation context is used to combine operation into one group.
     * The operation context id can be the similar for several operations.
     */
    private final UUID operationContextId;

    /**
     * The id determines a specific operation.
     */
    private final UUID operationId;

    /**
     * The constructor creates a locator for the common group.
     *
     * @param operationId Operation id.
     */
    public ReplicaRequestLocator(UUID operationId) {
        this(null, operationId);
    }

    /**
     * The constructor.
     *
     * @param operationContextId Operation context id.
     * @param operationId Operation id.
     */
    public ReplicaRequestLocator(@Nullable UUID operationContextId, UUID operationId) {
        this.operationContextId = operationContextId;
        this.operationId = operationId;
    }

    /**
     * Gets a locator prefix. If the prefix is {@code null}, the locator identifies an operation in the common group.
     *
     * @return Transaction id.
     */
    public UUID operationContextId() {
        return operationContextId;
    }

    /**
     * An id that is assigned to an operation.
     *
     * @return Operation id.
     */
    public UUID operationId() {
        return operationId;
    }
}
