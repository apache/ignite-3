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

package org.apache.ignite.internal.table.distributed.replicator.action;

import java.util.UUID;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.message.ReplicaAction;

/**
 * Partition action.
 */
public abstract class PartitionAction implements ReplicaAction {
    /**
     * Action type.
     */
    private final RequestType actionType;

    /**
     * Transaction timestamp.
     */
    private final HybridTimestamp timestamp;

    /**
     * Transaction id.
     */
    private final UUID txId;

    /**
     * Index id that use to apply action.
     */
    private final UUID indexToUse;

    /**
     * The constructor.
     *
     * @param type       Action type.
     * @param txId       Transaction id.
     * @param timestamp  Transaction timestamp.
     * @param indexToUse Index id.
     */
    protected PartitionAction(RequestType type, UUID txId, HybridTimestamp timestamp, UUID indexToUse) {
        this.actionType = type;
        this.txId = txId;
        this.timestamp = timestamp;
        this.indexToUse = indexToUse;
    }

    /**
     * Gets an action type for the request.
     *
     * @return Transaction operation type.
     */
    public RequestType actionType() {
        return actionType;
    }

    /**
     * Gets a transaction timestamp.
     *
     * @return Timestamp.
     */
    public HybridTimestamp timestamp() {
        return timestamp;
    }

    /**
     * Gets a transaction id.
     *
     * @return Transaction id.
     */
    public UUID txId() {
        return txId;
    }

    /**
     * Gets an index id or {@code null} if the action does not use index.
     *
     * @return Index id.
     */
    public UUID indexToUse() {
        return indexToUse;
    }
}
