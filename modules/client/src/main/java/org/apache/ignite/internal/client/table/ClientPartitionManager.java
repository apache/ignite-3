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

package org.apache.ignite.internal.client.table;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.client.TcpIgniteClient.unpackClusterNode;
import static org.apache.ignite.internal.client.table.ClientTupleSerializer.getPartitionAwarenessProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionManager;
import org.jetbrains.annotations.Nullable;

/**
 * Client partition manager implementation.
 */
class ClientPartitionManager implements PartitionManager {
    private final ClientTable tbl;

    private final Lock lock = new ReentrantLock();

    private final Map<Partition, ClusterNode> cache = new HashMap<>();

    private long assignmentChangeTimestamp;

    ClientPartitionManager(ClientTable clientTable) {
        this.tbl = clientTable;
    }

    @Override
    public CompletableFuture<ClusterNode> primaryReplicaAsync(Partition partition) {
        if (!(partition instanceof HashPartition)) {
            throw new IllegalArgumentException("Unsupported partition type: " + partition);
        }

        ClusterNode clusterNode = getClusterNode(partition);

        if (clusterNode != null) {
            return completedFuture(clusterNode);
        }

        return primaryReplicasAsync()
                .thenApply(map -> map.get(partition));
    }

    @Override
    @SuppressWarnings("resource")
    public CompletableFuture<Map<Partition, ClusterNode>> primaryReplicasAsync() {
        Map<Partition, ClusterNode> cache = lookupCache();

        if (cache != null) {
            return completedFuture(cache);
        }

        var currentTs = tbl.channel().partitionAssignmentTimestamp();

        return tbl.channel().serviceAsync(ClientOp.PRIMARY_REPLICAS_GET,
                w -> w.out().packInt(tbl.tableId()),
                r -> {
                    ClientMessageUnpacker in = r.in();
                    int size = in.unpackInt();

                    Map<Partition, ClusterNode> res = new HashMap<>(size);

                    for (int i = 0; i < size; i++) {
                        int partition = in.unpackInt();
                        res.put(new HashPartition(partition), unpackClusterNode(r));
                    }

                    return res;
                })
                .thenApply(map -> updateCache(map, currentTs));
    }

    @Override
    public <K> CompletableFuture<Partition> partitionAsync(K key, Mapper<K> mapper) {
        Objects.requireNonNull(key, "Key is null.");
        Objects.requireNonNull(mapper, "Mapper is null.");

        return getPartition(getPartitionAwarenessProvider(null, mapper, key));
    }

    @Override
    public CompletableFuture<Partition> partitionAsync(Tuple key) {
        Objects.requireNonNull(key, "Key is null.");

        return getPartition(getPartitionAwarenessProvider(null, key));
    }

    private @Nullable ClusterNode getClusterNode(Partition partition) {
        lock.lock();

        try {
            //noinspection resource
            if (tbl.channel().partitionAssignmentTimestamp() > assignmentChangeTimestamp) {
                cache.clear();
                return null;
            }

            return cache.get(partition);
        } finally {
            lock.unlock();
        }
    }

    private @Nullable Map<Partition, ClusterNode> lookupCache() {
        lock.lock();

        try {
            //noinspection resource
            if (tbl.channel().partitionAssignmentTimestamp() > assignmentChangeTimestamp) {
                cache.clear();
                return null;
            }

            return Map.copyOf(cache);
        } finally {
            lock.unlock();
        }
    }

    private Map<Partition, ClusterNode> updateCache(Map<Partition, ClusterNode> map, long timestamp) {
        lock.lock();

        try {
            cache.putAll(map);
            assignmentChangeTimestamp = timestamp;
            return map;
        } finally {
            lock.unlock();
        }
    }

    private CompletableFuture<Partition> getPartition(PartitionAwarenessProvider partitionAwarenessProvider) {
        return tbl.getPartitionAssignment()
                .thenCompose(partitions -> tbl.getLatestSchema().thenApply(schema -> {
                    Integer hash = partitionAwarenessProvider.getObjectHashCode(schema);

                    return new HashPartition(Math.abs(hash % partitions.size()));
                }));
    }
}
