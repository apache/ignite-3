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

package org.apache.ignite.internal.tx.impl;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.tx.TransactionException;

/**
 * A helper class to retrieve primary replicas with exception handling.
 */
public class PlacementDriverHelper {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PlacementDriverHelper.class);

    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /**
     * Constructor.
     *
     * @param placementDriver Placement driver.
     * @param clock A hybrid logical clock.
     */
    public PlacementDriverHelper(PlacementDriver placementDriver, HybridClock clock) {
        this.placementDriver = placementDriver;
        this.clock = clock;
    }

    /**
     * Wait for primary replica to appear for the provided partition.
     *
     * @param partitionId Partition id.
     * @return Future that completes with node id that is a primary for the provided partition, or completes with exception if no primary
     *         appeared during the await timeout.
     */
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(TablePartitionId partitionId) {
        HybridTimestamp timestamp = clock.now();

        return placementDriver.awaitPrimaryReplica(partitionId, timestamp, AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS)
                .handle((primaryReplica, e) -> {
                    if (e != null) {
                        LOG.error("Failed to retrieve primary replica for partition {}", partitionId, e);

                        throw withCause(TransactionException::new, REPLICA_UNAVAILABLE_ERR,
                                "Failed to get the primary replica"
                                        + " [tablePartitionId=" + partitionId + ", awaitTimestamp=" + timestamp + ']', e);
                    }

                    return primaryReplica;
                });
    }

    /**
     * Get primary replicas for the provided partitions.
     *
     * @param partitions A collection of partitions.
     * @return A future that completes with a map of node to the partitions the node is primary for and a collection of partitions that we
     *         failed to find the primary for.
     */
    public CompletableFuture<PartitionData> findPrimaryReplicas(Collection<TablePartitionId> partitions) {
        HybridTimestamp timestamp = clock.now();

        Map<TablePartitionId, CompletableFuture<ReplicaMeta>> primaryReplicaFutures = new HashMap<>();

        // Please note that we are using `get primary replica` instead of `await primary replica`.
        // This method is faster, yet we still have the correctness:
        // If the primary replica has not changed, get will return a valid value and we'll send an unlock request to this node.
        // If the primary replica has expired and get returns null (or a different node), the primary node step down logic
        // will automatically release the locks on that node. All we need to do is to clean the storage.
        for (TablePartitionId partitionId : partitions) {
            primaryReplicaFutures.put(partitionId, placementDriver.getPrimaryReplica(partitionId, timestamp));
        }

        return allOf(primaryReplicaFutures.values().toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> {
                    Map<String, Set<TablePartitionId>> partitionsByNode = new HashMap<>();

                    Set<TablePartitionId> partitionsWithoutPrimary = new HashSet<>();

                    for (Entry<TablePartitionId, CompletableFuture<ReplicaMeta>> entry : primaryReplicaFutures.entrySet()) {
                        // Safe to call join, the future has already finished.
                        ReplicaMeta meta = entry.getValue().join();

                        TablePartitionId partition = entry.getKey();

                        if (meta != null) {
                            partitionsByNode.computeIfAbsent(meta.getLeaseholder(), s -> new HashSet<>())
                                    .add(partition);
                        } else {
                            partitionsWithoutPrimary.add(partition);
                        }
                    }
                    return new PartitionData(partitionsByNode, partitionsWithoutPrimary);
                });
    }

    /**
     * The result of retrieving primary replicas for a collection of partitions.
     */
    public static class PartitionData {
        final Map<String, Set<TablePartitionId>> partitionsByNode;

        final Set<TablePartitionId> partitionsWithoutPrimary;

        PartitionData(Map<String, Set<TablePartitionId>> partitionsByNode, Set<TablePartitionId> partitionsWithoutPrimary) {
            this.partitionsByNode = partitionsByNode;
            this.partitionsWithoutPrimary = partitionsWithoutPrimary;
        }
    }
}
