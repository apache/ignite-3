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

package org.apache.ignite.internal.partition.replicator;

import org.apache.ignite.internal.replicator.ZonePartitionId;

/**
 * Parameters for the {@link LocalPartitionReplicaEvent#BEFORE_REPLICA_STARTED} event.
 *
 * <p>Used as a container to pass information from event listeners to the partition replica lifecycle manager about whether any storage
 * is seen in the rebalance state.
 */
public class LocalBeforeReplicaStartEventParameters extends LocalPartitionReplicaEventParameters {
    private volatile boolean anyStorageIsInRebalanceState;

    /**
     * Constructor.
     *
     * @param zonePartitionId Zone partition id.
     * @param revision Event's revision.
     * @param onRecovery Flag indicating if this event was produced on node recovery.
     */
    public LocalBeforeReplicaStartEventParameters(
            ZonePartitionId zonePartitionId,
            long revision,
            boolean onRecovery,
            boolean anyStorageIsInRebalanceState
    ) {
        super(zonePartitionId, revision, onRecovery);

        this.anyStorageIsInRebalanceState = anyStorageIsInRebalanceState;
    }

    /** Returns whether at least one storage is in rebalance state. */
    public boolean anyStorageIsInRebalanceState() {
        return anyStorageIsInRebalanceState;
    }

    /** Registers that at least one storage is in rebalance state. */
    public void registerStorageInRebalanceState() {
        this.anyStorageIsInRebalanceState = true;
    }
}
