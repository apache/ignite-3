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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * The partition distribution provides the ability to obtain information about table partitions.
 * This interface can be used to get all partitions of a table, the location of the primary replica of a partition,
 * the partition for a specific table key.
 */
public interface PartitionDistribution {
    /**
     * Asynchronously gets a list with all partitions.
     *
     * @return Future with list with all partitions.
     */
    CompletableFuture<List<Partition>> partitionsAsync();

    /**
     * Gets a list with all partitions.
     *
     * @return List with all partitions.
     */
    List<Partition> partitions();

    /**
     * Asynchronously gets map with all partitions and their locations as of the time of the call.
     *
     * <p>Note: This assignment may become outdated if a re-assigment happens on the cluster.
     *
     * @return Future with map from partition to cluster node where primary replica of the partition is located as of the time of the call.
     */
    CompletableFuture<Map<Partition, ClusterNode>> primaryReplicasAsync();

    /**
     * Asynchronously gets all partitions hosted by the specified node as a primary replica.
     *
     * @return Future with list with all partitions hosted by the specified node as a primary replica.
     */
    CompletableFuture<List<Partition>> primaryReplicasAsync(ClusterNode node);

    /**
     * Gets map with all partitions and their locations as of the time of the call.
     *
     * <p>Note: This assignment may become outdated if a re-assigment happens on the cluster.
     *
     * @return Map from partition to cluster node where primary replica of the partition is located as of the time of the call.
     */
    Map<Partition, ClusterNode> primaryReplicas();

    /**
     * Gets all partitions hosted by the specified node as a primary replica.
     *
     * @return List with all partitions hosted by the specified node as a primary replica.
     */
    List<Partition> primaryReplicas(ClusterNode node);

    /**
     * Asynchronously gets the current location of the primary replica for the provided partition.
     *
     * <p>Note: This assignment may become outdated if a re-assigment happens on the cluster.
     *
     * @param partition Partition instance.
     * @return Future that represents the pending completion of the operation.
     * @see #primaryReplica(Partition)
     */
    CompletableFuture<ClusterNode> primaryReplicaAsync(Partition partition);

    /**
     * Gets the current location of the primary replica for the provided partition.
     *
     * <p>Note: This assignment may become outdated if a re-assigment happens on the cluster.
     *
     * @param partition Partition instance.
     * @return Cluster node where primary replica of provided partition is located as of the time of the call.
     */
    ClusterNode primaryReplica(Partition partition);

    /**
     * Asynchronously gets partition instance for provided table key.
     *
     * @param key Table key.
     * @param mapper Table key mapper.
     * @param <K> Key type.
     * @return Future with partition instance which contains provided key.
     */
    <K> CompletableFuture<Partition> partitionAsync(K key, Mapper<K> mapper);

    /**
     * Asynchronously gets partition instance for provided table key.
     *
     * @param key Table key tuple.
     * @return Future with partition instance which contains provided key.
     */
    CompletableFuture<Partition> partitionAsync(Tuple key);

    /**
     * Gets partition instance for provided table key.
     *
     * @param key Table key.
     * @param mapper Table key mapper.
     * @param <K> Key type.
     * @return Partition instance which contains provided key.
     */
    <K> Partition partition(K key, Mapper<K> mapper);

    /**
     * Gets partition instance for provided table key.
     *
     * @param key Table key tuple.
     * @return Partition instance which contains provided key.
     */
    Partition partition(Tuple key);
}
