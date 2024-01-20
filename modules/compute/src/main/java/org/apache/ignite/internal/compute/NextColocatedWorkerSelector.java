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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.IgniteNameUtils;
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
    private static final IgniteLogger LOG = Loggers.forClass(NextColocatedWorkerSelector.class);

    private static final int PRIMARY_REPLICA_ASK_CLOCK_ADDITION_MILLIS = 10_000;

    private static final int AWAIT_FOR_PRIMARY_REPLICA_SECONDS = 15;

    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private final IgniteTablesInternal tables;

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
            IgniteTablesInternal tables,
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            String tableName,
            @Nullable K key,
            @Nullable Mapper<K> keyMapper) {
        this(tables, placementDriver, topologyService, clock, tableName, key, keyMapper, null);
    }

    NextColocatedWorkerSelector(
            IgniteTablesInternal tables,
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            String tableName,
            Tuple tuple) {
        this(tables, placementDriver, topologyService, clock, tableName, null, null, tuple);
    }

    private NextColocatedWorkerSelector(
            IgniteTablesInternal tables,
            PlacementDriver placementDriver,
            TopologyService topologyService,
            HybridClock clock,
            String tableName,
            @Nullable K key,
            @Nullable Mapper<K> keyMapper,
            @Nullable Tuple tuple) {
        this.tables = tables;
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.table = getTableViewInternal(tableName);
        this.clock = clock;
        this.key = key;
        this.keyMapper = keyMapper;
        this.tuple = tuple;
    }

    private TableViewInternal getTableViewInternal(String tableName) {
        TableViewInternal table;
        try {
            table = requiredTable(tableName).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return table;
    }

    private CompletableFuture<ClusterNode> tryToFindPrimaryReplica(TablePartitionId tablePartitionId)
            throws ExecutionException, InterruptedException {
        return placementDriver.awaitPrimaryReplica(
                        tablePartitionId,
                        clock.now().addPhysicalTime(PRIMARY_REPLICA_ASK_CLOCK_ADDITION_MILLIS),
                        AWAIT_FOR_PRIMARY_REPLICA_SECONDS,
                        TimeUnit.SECONDS
                ).thenApply(ReplicaMeta::getLeaseholderId)
                .thenApply(topologyService::getById);
    }

    @Override
    public CompletableFuture<ClusterNode> next() {
        TablePartitionId tablePartitionId = tablePartitionId();
        try {
            return tryToFindPrimaryReplica(tablePartitionId);
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Failed to resolve new primary replica for partition " + tablePartitionId);
        }

        return CompletableFuture.completedFuture(null);
    }

    private TablePartitionId tablePartitionId() {
        TablePartitionId tablePartitionId;
        if (key != null && keyMapper != null) {
            tablePartitionId = new TablePartitionId(table.tableId(), table.partition(key, keyMapper));
        } else {
            tablePartitionId = new TablePartitionId(table.tableId(), table.partition(tuple));
        }
        return tablePartitionId;
    }

    private CompletableFuture<TableViewInternal> requiredTable(String tableName) {
        String parsedName = IgniteNameUtils.parseSimpleName(tableName);

        return tables.tableViewAsync(parsedName)
                .thenApply(table -> {
                    if (table == null) {
                        throw new TableNotFoundException(DEFAULT_SCHEMA_NAME, parsedName);
                    }
                    return table;
                });
    }
}
