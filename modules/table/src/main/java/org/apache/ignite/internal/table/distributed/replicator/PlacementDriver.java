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

package org.apache.ignite.internal.table.distributed.replicator;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxStateReplicaRequest;
import org.apache.ignite.network.ClusterNode;

/**
 * Placement driver.
 */
public class PlacementDriver {
    /** Assignment nodes per replication group. */
    private final Map<ReplicationGroupId, LinkedHashSet<ClusterNode>> primaryReplicaMapping = new ConcurrentHashMap<>();

    /** Replication service. */
    private final ReplicaService replicaService;

    /**
     * The constructor.
     *
     * @param replicaService Replication service.
     */
    public PlacementDriver(ReplicaService replicaService) {
        this.replicaService = replicaService;
    }

    /**
     * Sends a transaction sate request to the primary replica.
     *
     * @param replicaGrp Replication group id.
     * @param request Status request.
     * @return Result future.
     */
    public CompletableFuture<TxMeta> sendMetaRequest(ReplicationGroupId replicaGrp, TxStateReplicaRequest request) {
        CompletableFuture<TxMeta> resFut = new CompletableFuture<>();

        sendAndRetry(resFut, replicaGrp, request);

        return resFut;
    }

    /**
     * Updates an assignment for the specific replication group.
     *
     * @param replicaGrpId Replication group id.
     * @param assignment Assignment.
     */
    public void updateAssignment(ReplicationGroupId replicaGrpId, Collection<ClusterNode> assignment) {
        primaryReplicaMapping.put(replicaGrpId, new LinkedHashSet<>(assignment));
    }

    /**
     * Tries to send a request to primary replica of the replication group.
     * If the first node turns up not a primary one the logic sends the same request to a new primary node.
     *
     * @param resFut Response future.
     * @param replicaGrp Replication group id.
     * @param request Request.
     */
    private void sendAndRetry(CompletableFuture<TxMeta> resFut, ReplicationGroupId replicaGrp, TxStateReplicaRequest request) {
        ClusterNode nodeToSend = primaryReplicaMapping.get(replicaGrp).iterator().next();

        replicaService.invoke(nodeToSend, request).thenAccept(resp -> {
            assert resp instanceof LeaderOrTxState : "Unsupported response type [type=" + resp.getClass().getSimpleName() + ']';

            LeaderOrTxState stateAndLeader = (LeaderOrTxState) resp;

            ClusterNode nextNodeToSend = stateAndLeader.leader();

            if (nextNodeToSend == null) {
                resFut.complete(stateAndLeader.txMeta());
            } else {
                LinkedHashSet<ClusterNode> newAssignment = new LinkedHashSet<>();

                newAssignment.add(nextNodeToSend);
                newAssignment.addAll(primaryReplicaMapping.get(replicaGrp));

                primaryReplicaMapping.put(replicaGrp, newAssignment);

                sendAndRetry(resFut, replicaGrp, request);
            }
        });
    }
}
