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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;

/**
 * Next worker selector that returns a node that holds a primary replica for the partition specified in a subclass as a next worker. If
 * there is no such node (we lost the majority, for example) the {@code CompletableFuture.completedFuture(null)} will be returned.
 */
abstract class PrimaryReplicaNextWorkerSelector implements NextWorkerSelector {
    private static final int PRIMARY_REPLICA_ASK_CLOCK_ADDITION_MILLIS = 10_000;

    private static final int AWAIT_FOR_PRIMARY_REPLICA_SECONDS = 15;

    private final PlacementDriver placementDriver;

    private final TopologyService topologyService;

    private final HybridClock clock;

    PrimaryReplicaNextWorkerSelector(
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock
    ) {
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.clock = clock;
    }

    @Override
    public CompletableFuture<InternalClusterNode> next() {
        return placementDriver.awaitPrimaryReplica(
                        partitionGroupId(),
                        clock.now().addPhysicalTime(PRIMARY_REPLICA_ASK_CLOCK_ADDITION_MILLIS),
                        AWAIT_FOR_PRIMARY_REPLICA_SECONDS,
                        TimeUnit.SECONDS
                )
                .thenApply(ReplicaMeta::getLeaseholderId)
                .thenApply(topologyService::getById);
    }

    protected abstract ZonePartitionId partitionGroupId();
}
