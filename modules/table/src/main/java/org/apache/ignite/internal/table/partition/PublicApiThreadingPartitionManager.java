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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionManager;

/**
 * Wrapper around {@link PartitionManager} that maintains public API invariants relating to threading.
 * That is, it adds protection against thread hijacking by users.
 *
 * @see PublicApiThreading#preventThreadHijack(CompletableFuture, Executor)
 */
public class PublicApiThreadingPartitionManager implements PartitionManager {
    private final PartitionManager partitionManager;
    private final Executor asyncContinuationExecutor;

    public PublicApiThreadingPartitionManager(PartitionManager partitionManager, Executor asyncContinuationExecutor) {
        this.partitionManager = partitionManager;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public CompletableFuture<ClusterNode> primaryReplicaAsync(Partition partition) {
        return preventThreadHijack(partitionManager.primaryReplicaAsync(partition));
    }

    @Override
    public CompletableFuture<Map<Partition, ClusterNode>> primaryReplicasAsync() {
        return preventThreadHijack(partitionManager.primaryReplicasAsync());
    }

    @Override
    public <K> CompletableFuture<Partition> partitionAsync(K key, Mapper<K> mapper) {
        return preventThreadHijack(partitionManager.partitionAsync(key, mapper));
    }

    @Override
    public CompletableFuture<Partition> partitionAsync(Tuple key) {
        return preventThreadHijack(partitionManager.partitionAsync(key));
    }

    private <T> CompletableFuture<T> preventThreadHijack(CompletableFuture<T> originalFuture) {
        return PublicApiThreading.preventThreadHijack(originalFuture, asyncContinuationExecutor);
    }
}
