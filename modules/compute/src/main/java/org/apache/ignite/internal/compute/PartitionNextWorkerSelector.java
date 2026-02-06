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

package org.apache.ignite.internal.compute;

import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.table.partition.Partition;

/**
 * Next worker selector that returns node that holds a primary replica for the specified partition as a next worker. If there is no such
 * node (we lost the majority, for example) the {@code CompletableFuture.completedFuture(null)} will be returned.
 */
class PartitionNextWorkerSelector extends PrimaryReplicaNextWorkerSelector {
    private final ZonePartitionId partitionGroupId;

    PartitionNextWorkerSelector(
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            int zoneId,
            Partition partition
    ) {
        super(placementDriver, topologyService, clock);

        this.partitionGroupId = new ZonePartitionId(zoneId, Math.toIntExact(partition.id()));
    }

    @Override
    protected ZonePartitionId partitionGroupId() {
        return partitionGroupId;
    }
}
