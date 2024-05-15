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
import static org.apache.ignite.internal.client.table.ClientTupleSerializer.getPartitionAwarenessProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.client.ClientClusterNode;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.TopologyCache;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionManager;
import org.jetbrains.annotations.Nullable;

/**
 * Client partition manager implementation.
 */
public class ClientPartitionManager implements PartitionManager {
    private final ClientTable tbl;

    private final TopologyCache topologyCache;

    ClientPartitionManager(ClientTable clientTable, TopologyCache topologyCache) {
        this.tbl = clientTable;
        this.topologyCache = topologyCache;
    }

    @Override
    @SuppressWarnings("resource")
    public CompletableFuture<ClusterNode> primaryReplicaAsync(Partition partition) {
        if (!(partition instanceof ClientHashPartition)) {
            throw new IllegalArgumentException();
        }

        ClientHashPartition clientPartition = (ClientHashPartition) partition;
        return tbl.getPartitionAssignment().thenCompose(names -> {
            String node = names.get(clientPartition.partitionId);
            if (node == null) {
                return tbl.channel().serviceAsync(ClientOp.PARTITION_PRIMARY_GET,
                        w -> {
                            w.out().packInt(tbl.tableId());
                            w.out().packInt(clientPartition.partitionId);
                        },
                        ClientPartitionManager::unpackClusterNode);
            } else {
                return completedFuture(topologyCache.get(node));
            }
        });
    }

    @Override
    @SuppressWarnings("resource")
    public CompletableFuture<Map<Partition, ClusterNode>> primaryReplicasAsync() {
        return tbl.channel().serviceAsync(ClientOp.PARTITIONS_PRIMARY_GET,
                w -> w.out().packInt(tbl.tableId()),
                r -> {
                    ClientMessageUnpacker in = r.in();

                    if (in.tryUnpackNil()) {
                        return null;
                    }

                    int size = in.unpackInt();

                    Map<Partition, ClusterNode> res = new HashMap<>(size);

                    for (int i = 0; i < size; i++) {
                        int partition = in.unpackInt();
                        res.put(new ClientHashPartition(partition), unpackClusterNode(r));
                    }

                    return res;
                });
    }

    @Override
    public <K> CompletableFuture<Partition> partitionAsync(K key, Mapper<K> mapper) {
        return getPartition(getPartitionAwarenessProvider(null, mapper, key),
                (schema, w) -> ClientRecordSerializer.writeRecRaw(key, mapper, schema, w.out(), TuplePart.KEY)
        );
    }

    @Override
    public CompletableFuture<Partition> partitionAsync(Tuple key) {
        return getPartition(getPartitionAwarenessProvider(null, key),
                (schema, w) -> ClientTupleSerializer.writeTupleRaw(key, schema, w, true)
        );
    }

    @SuppressWarnings("resource")
    private CompletableFuture<Partition> getPartition(
            PartitionAwarenessProvider partitionAwarenessProvider,
            BiConsumer<ClientSchema, PayloadOutputChannel> keyPack
    ) {
        return tbl.getPartitionAssignment()
                .thenCompose(partitions -> tbl.getLatestSchema().thenApply(schema -> {
                    Integer hash = partitionAwarenessProvider.getObjectHashCode(schema);

                    return Math.abs(hash % partitions.size());
                }))
                .exceptionally(t -> -1)
                .thenCompose(partition -> {
                    if (partition == -1) {
                        return tbl.getLatestSchema()
                                .thenCompose(schema ->
                                        tbl.channel().serviceAsync(ClientOp.KEY_PARTITION_PRIMARY_GET,
                                                w -> {
                                                    w.out().packInt(tbl.tableId());
                                                    keyPack.accept(schema, w);
                                                },
                                                r -> r.in().unpackInt()
                                        ));
                    }

                    return completedFuture(partition);
                }).thenApply(ClientHashPartition::new);
    }

    private static @Nullable ClusterNode unpackClusterNode(PayloadInputChannel r) {
        ClientMessageUnpacker in = r.in();
        if (in.tryUnpackNil()) {
            return null;
        }

        return new ClientClusterNode(
                in.unpackString(),
                in.unpackString(),
                new NetworkAddress(in.unpackString(), in.unpackInt()));
    }

    public static class ClientHashPartition implements Partition {
        private static final long serialVersionUID = -2089375867774271170L;

        private final int partitionId;

        public ClientHashPartition(int partitionId) {
            this.partitionId = partitionId;
            if (partitionId > 2 || partitionId < 0) {
                System.out.println();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ClientHashPartition that = (ClientHashPartition) o;

            return partitionId == that.partitionId;
        }

        @Override
        public int hashCode() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "ClientHashPartition{"
                    + "partitionId=" + partitionId
                    + '}';
        }
    }
}
