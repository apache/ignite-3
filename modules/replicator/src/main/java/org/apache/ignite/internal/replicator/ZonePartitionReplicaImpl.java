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

package org.apache.ignite.internal.replicator;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.TableAware;

/**
 * Replica for the zone based partitions.
 */
public class ZonePartitionReplicaImpl implements Replica {
    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionReplicaImpl.class);

    private final ReplicationGroupId replicaGrpId;

    private final ReplicaListener listener;

    private final Map<TablePartitionId, ReplicaListener> replicas = new ConcurrentHashMap<>();

    public ZonePartitionReplicaImpl(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener
    )  {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
    }

    @Override
    public ReplicaListener listener() {
        return listener;
    }

    @Override
    public TopologyAwareRaftGroupService raftClient() {
        throw new UnsupportedOperationException("raftClient");
    }

    @Override
    public CompletableFuture<ReplicaResult> processRequest(ReplicaRequest request, String senderId) {
        if (!(request instanceof TableAware)) {
            LOG.info("Non table request is not supported by the zone partition yet " + request);

            return nullCompletedFuture();
        } else {
            int partitionId;

            ReplicationGroupId replicationGroupId = request.groupId();

            // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 Refine this code when the zone based replication will done.
            if (replicationGroupId instanceof  TablePartitionId) {
                partitionId = ((TablePartitionId) replicationGroupId).partitionId();
            } else if (replicationGroupId instanceof ZonePartitionId) {
                partitionId = ((ZonePartitionId) replicationGroupId).partitionId();
            } else {
                throw new IllegalArgumentException("Requests with replication group type "
                        + request.groupId().getClass() + " is not supported");
            }

            return replicas.get(new TablePartitionId(((TableAware) request).tableId(), partitionId))
                    .invoke(request, senderId);
        }
    }

    @Override
    public ReplicationGroupId groupId() {
        return replicaGrpId;
    }

    @Override
    public CompletableFuture<? extends NetworkMessage> processPlacementDriverMessage(PlacementDriverReplicaMessage msg) {
        throw new UnsupportedOperationException("processPlacementDriverMessage");
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return nullCompletedFuture();
    }

    /**
     * Add table replica.
     *
     * @param partitionId Table partition id.
     * @param replicaListener Table replica listener.
     */
    public void addTableReplicaListener(TablePartitionId partitionId, ReplicaListener replicaListener) {
        replicas.put(partitionId, replicaListener);
    }
}
