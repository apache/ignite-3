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
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Next worker selector that returns primary replica node for next worker. If there is no such node (we lost the majority, for example) the
 * {@code CompletableFuture.completedFuture(null)} will be returned.
 *
 * @param <K> type of the key for the colocated table.
 */
public class NextColocatedWorkerSelector<K> implements NextWorkerSelector {
    private static final int PRIMARY_REPLICA_ASK_CLOCK_ADDITION_MILLIS = 10_000;

    private static final int AWAIT_FOR_PRIMARY_REPLICA_SECONDS = 15;

    private final PlacementDriver placementDriver;

    private final TopologyService topologyService;

    private final HybridClock clock;

    @Nullable
    private final K key;

    @Nullable
    private final Mapper<K> keyMapper;

    private final Tuple tuple;

    private final TableViewInternal table;

    NextColocatedWorkerSelector(
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            TableViewInternal table,
            K key,
            Mapper<K> keyMapper) {
        this(placementDriver, topologyService, clock, table, key, keyMapper, null);
    }

    NextColocatedWorkerSelector(
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            TableViewInternal table,
            Tuple tuple) {
        this(placementDriver, topologyService, clock, table, null, null, tuple);
    }

    private NextColocatedWorkerSelector(
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            TableViewInternal table,
            @Nullable K key,
            @Nullable Mapper<K> keyMapper,
            @Nullable Tuple tuple) {
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.table = table;
        this.clock = clock;
        this.key = key;
        this.keyMapper = keyMapper;
        this.tuple = tuple;
    }

    private CompletableFuture<ClusterNode> tryToFindPrimaryReplica(ZonePartitionId zonePartitionId) {
        return placementDriver.awaitPrimaryReplicaForTable(
                        zonePartitionId,
                        clock.now().addPhysicalTime(PRIMARY_REPLICA_ASK_CLOCK_ADDITION_MILLIS),
                        AWAIT_FOR_PRIMARY_REPLICA_SECONDS,
                        TimeUnit.SECONDS
                ).thenApply(ReplicaMeta::getLeaseholderId)
                .thenApply(topologyService::getById);
    }

    @Override
    public CompletableFuture<ClusterNode> next() {
        ZonePartitionId zonePartitionId = zonePartitionId();
        return tryToFindPrimaryReplica(zonePartitionId);
    }

    private ZonePartitionId zonePartitionId() {
        if (key != null && keyMapper != null) {
            return new ZonePartitionId(table.internalTable().zoneId(), table.tableId(), table.partition(key, keyMapper));
        } else {
            return new ZonePartitionId(table.internalTable().zoneId(), table.tableId(), table.partition(tuple));
        }
    }
}
