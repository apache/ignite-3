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

package org.apache.ignite.internal.table.partition;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionDistribution;
import org.apache.ignite.table.partition.PartitionManager;

/**
 * Implementation of {@link PartitionDistribution} for tables with hash partitions.
 */
public class HashPartitionManagerImpl implements PartitionManager {
    private final InternalTable table;

    private final SchemaRegistry schemaReg;

    private final MarshallersProvider marshallers;

    /**
     * Constructor.
     *
     * @param table Internal table.
     * @param schemaReg Schema registry.
     * @param marshallers Marshallers.
     */
    public HashPartitionManagerImpl(
            InternalTable table,
            SchemaRegistry schemaReg,
            MarshallersProvider marshallers
    ) {
        this.table = table;
        this.schemaReg = schemaReg;
        this.marshallers = marshallers;
    }

    @Override
    public CompletableFuture<List<Partition>> partitionsAsync() {
        return completedFuture(partitions());
    }

    @Override
    public List<Partition> partitions() {
        int partitions = table.partitions();
        var list = new ArrayList<Partition>(partitions);

        for (int i = 0; i < partitions; i++) {
            list.add(new HashPartition(i));
        }

        return list;
    }

    @Override
    public CompletableFuture<Map<Partition, ClusterNode>> primaryReplicasAsync() {
        int partitions = table.partitions();
        @SuppressWarnings("unchecked")
        CompletableFuture<InternalClusterNode>[] futures = new CompletableFuture[partitions];

        for (int i = 0; i < partitions; i++) {
            futures[i] = table.partitionLocation(i);
        }

        return allOf(futures)
                .thenApply(unused -> {
                    Map<Partition, ClusterNode> result = new HashMap<>(partitions);
                    for (int i = 0; i < partitions; i++) {
                        result.put(new HashPartition(i), futures[i].join().toPublicNode());
                    }
                    return result;
                });
    }

    @Override
    public CompletableFuture<List<Partition>> primaryReplicasAsync(ClusterNode node) {
        return primaryReplicasAsync()
                .thenApply(map -> {
                    List<Partition> parts = new ArrayList<>(map.size());

                    for (Map.Entry<Partition, ClusterNode> entry : map.entrySet()) {
                        if (entry.getValue().equals(node)) {
                            parts.add(entry.getKey());
                        }
                    }

                    return parts;
                });
    }

    @Override
    public Map<Partition, ClusterNode> primaryReplicas() {
        return sync(primaryReplicasAsync());
    }

    @Override
    public List<Partition> primaryReplicas(ClusterNode node) {
        return sync(primaryReplicasAsync(node));
    }

    @Override
    public CompletableFuture<ClusterNode> primaryReplicaAsync(Partition partition) {
        return table.partitionLocation(Math.toIntExact(partition.id()))
                .thenApply(InternalClusterNode::toPublicNode);
    }

    @Override
    public ClusterNode primaryReplica(Partition partition) {
        return sync(primaryReplicaAsync(partition));
    }

    @Override
    public <K> CompletableFuture<Partition> partitionAsync(K key, Mapper<K> mapper) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(mapper);

        SchemaDescriptor schema = schemaReg.lastKnownSchema();

        Column valCol = CollectionUtils.first(schema.valueColumns());
        assert valCol != null;

        var marshaller = new KvMarshallerImpl<>(schema, marshallers, mapper, Mapper.of(Void.class, valCol.name()));

        BinaryRowEx keyRow = marshaller.marshal(key);

        return completedFuture(new HashPartition(table.partitionId(keyRow)));
    }

    @Override
    public CompletableFuture<Partition> partitionAsync(Tuple key) {
        Objects.requireNonNull(key);

        // Taking latest schema version for marshaller here because it's only used to calculate colocation hash, and colocation
        // columns never change (so they are the same for all schema versions of the table),
        Row keyRow = new TupleMarshallerImpl(schemaReg.lastKnownSchema()).marshalKey(key);

        return completedFuture(new HashPartition(table.partitionId(keyRow)));
    }

    @Override
    public <K> Partition partition(K key, Mapper<K> mapper) {
        return sync(partitionAsync(key, mapper));
    }

    @Override
    public Partition partition(Tuple key) {
        return sync(partitionAsync(key));
    }
}
