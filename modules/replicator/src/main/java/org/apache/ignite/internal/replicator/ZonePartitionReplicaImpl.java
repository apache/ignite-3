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
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;

/**
 * Replica for the zone based partitions.
 */
public class ZonePartitionReplicaImpl implements Replica {

    private final ReplicationGroupId replicaGrpId;

    private final ReplicaListener listener;

    private final Map<TablePartitionId, Replica> replicas = new ConcurrentHashMap<>();

    public ZonePartitionReplicaImpl(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener
    )  {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
    }

    @Override
    public TopologyAwareRaftGroupService raftClient() {
        throw new UnsupportedOperationException("raftClient");
    }

    @Override
    public CompletableFuture<ReplicaResult> processRequest(ReplicaRequest request, String senderId) {
        if (!(request.groupId() instanceof TablePartitionId)) {
            System.out.println("KKK non table message " + request);
            return listener.invoke(request, senderId);
//            return replicas.get(request.groupId()).processRequest(request, senderId);
        }

        return replicas.get(request.groupId()).processRequest(request, senderId);
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

    public void addReplica(TablePartitionId partitionId, Replica replica) {
        replicas.put(partitionId, replica);
    }
}
