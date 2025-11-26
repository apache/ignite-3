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
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Next worker selector that returns a node that holds a specified key as a next worker. If there is no such node (we lost the majority, for
 * example) the {@code CompletableFuture.completedFuture(null)} will be returned.
 *
 * @param <K> type of the key for the colocated table.
 */
class NextColocatedWorkerSelector<K> extends PrimaryReplicaNextWorkerSelector {
    @Nullable
    private final K key;

    @Nullable
    private final Mapper<K> keyMapper;

    @Nullable
    private final Tuple tuple;

    private final TableViewInternal table;

    NextColocatedWorkerSelector(
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            TableViewInternal table,
            K key,
            Mapper<K> keyMapper
    ) {
        this(placementDriver, topologyService, clock, table, key, keyMapper, null);
    }

    NextColocatedWorkerSelector(
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            TableViewInternal table,
            Tuple tuple
    ) {
        this(placementDriver, topologyService, clock, table, null, null, tuple);
    }

    private NextColocatedWorkerSelector(
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            TableViewInternal table,
            @Nullable K key,
            @Nullable Mapper<K> keyMapper,
            @Nullable Tuple tuple
    ) {
        super(placementDriver, topologyService, clock);
        this.table = table;
        this.key = key;
        this.keyMapper = keyMapper;
        this.tuple = tuple;
    }

    @Override
    protected ZonePartitionId partitionGroupId() {
        if (key != null && keyMapper != null) {
            return new ZonePartitionId(table.zoneId(), table.partitionId(key, keyMapper));
        } else {
            return new ZonePartitionId(table.zoneId(), table.partitionId(tuple));
        }
    }
}
