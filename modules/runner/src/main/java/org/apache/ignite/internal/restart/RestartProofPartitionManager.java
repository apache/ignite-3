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

package org.apache.ignite.internal.restart;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionDistribution;
import org.apache.ignite.table.partition.PartitionManager;

/**
 * Reference to {@link PartitionDistribution} under a swappable {@link Ignite} instance. When a restart happens,
 * this switches to the new Ignite instance.
 *
 * <p>API operations on this are linearized with respect to node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
class RestartProofPartitionManager extends RestartProofApiObject<PartitionDistribution> implements PartitionManager {
    RestartProofPartitionManager(
            IgniteAttachmentLock attachmentLock,
            Ignite initialIgnite,
            Function<Ignite, PartitionDistribution> factory
    ) {
        super(attachmentLock, initialIgnite, factory);
    }

    @Override
    public CompletableFuture<ClusterNode> primaryReplicaAsync(Partition partition) {
        return attachedAsync(view -> view.primaryReplicaAsync(partition));
    }

    @Override
    public CompletableFuture<Map<Partition, ClusterNode>> primaryReplicasAsync() {
        return attachedAsync(PartitionDistribution::primaryReplicasAsync);
    }

    @Override
    public <K> CompletableFuture<Partition> partitionAsync(K key, Mapper<K> mapper) {
        return attachedAsync(view -> view.partitionAsync(key, mapper));
    }

    @Override
    public CompletableFuture<Partition> partitionAsync(Tuple key) {
        return attachedAsync(view -> view.partitionAsync(key));
    }
}
