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

package org.apache.ignite.table.partition;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * The partition manager provides the ability to obtain information about table partitions.
 * This interface can be used to get all partitions of a table,
 * the location of the primary replica of a partition,
 * the partition for a specific table key.
 */
public interface PartitionManager {
    /**
     * Returns location of primary replica for provided partition.
     *
     * @param partition Partition instance.
     * @return Cluster node where primary replica of provided partition is located.
     */
    CompletableFuture<ClusterNode> primaryReplicaAsync(Partition partition);

    /**
     * Returns map with all partitions and their locations.
     *
     * @return Map from partition to cluster node where primary replica of the partition is located.
     */
    CompletableFuture<Map<Partition, ClusterNode>> primaryReplicasAsync();

    /**
     * Returns partition instance for provided table key.
     *
     * @param key Table key.
     * @param mapper Table key mapper.
     * @param <K> Key type.
     * @return Partition instance which contains provided key.
     */
    <K> CompletableFuture<Partition> partitionAsync(K key, Mapper<K> mapper);

    /**
     * Returns partition instance for provided table key.
     *
     * @param key Table key tuple.
     * @return Partition instance which contains provided key.
     */
    CompletableFuture<Partition> partitionAsync(Tuple key);
}
