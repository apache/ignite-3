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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.lang.UnsupportedPartitionTypeException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionManager;

/**
 * Implementation of {@link PartitionManager} for tables with hash partitions.
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
    public CompletableFuture<ClusterNode> primaryReplicaAsync(Partition partition) {
        if (!(partition instanceof HashPartition)) {
            throw new UnsupportedPartitionTypeException("Table " + table.name()
                    + " doesn't support any other type of partition except hash partition.");
        }
        HashPartition hashPartition = (HashPartition) partition;
        return table.partitionLocation(new ZonePartitionId(table.zoneId(), table.tableId(), hashPartition.partitionId()));
    }

    @Override
    public CompletableFuture<Map<Partition, ClusterNode>> primaryReplicasAsync() {
        int partitions = table.partitions();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[partitions];

        for (int i = 0; i < partitions; i++) {
            futures[i] = table.partitionLocation(new ZonePartitionId(table.zoneId(), table.tableId(), i));
        }

        return allOf(futures)
                .thenApply(unused -> {
                    Map<Partition, ClusterNode> result = new HashMap<>(partitions);
                    for (int i = 0; i < partitions; i++) {
                        result.put(new HashPartition(i), (ClusterNode) futures[i].join());
                    }
                    return result;
                });
    }

    @Override
    public <K> CompletableFuture<Partition> partitionAsync(K key, Mapper<K> mapper) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(mapper);

        var marshaller = new KvMarshallerImpl<>(schemaReg.lastKnownSchema(), marshallers, mapper, mapper);
        try {
            BinaryRowEx keyRow = marshaller.marshal(key);

            return completedFuture(new HashPartition(table.partition(keyRow)));
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    @Override
    public CompletableFuture<Partition> partitionAsync(Tuple key) {
        Objects.requireNonNull(key);

        try {
            // Taking latest schema version for marshaller here because it's only used to calculate colocation hash, and colocation
            // columns never change (so they are the same for all schema versions of the table),
            Row keyRow = new TupleMarshallerImpl(schemaReg.lastKnownSchema()).marshalKey(key);

            return completedFuture(new HashPartition(table.partition(keyRow)));
        } catch (TupleMarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }
}
