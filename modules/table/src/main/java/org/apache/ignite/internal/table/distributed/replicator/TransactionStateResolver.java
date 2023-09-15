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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxStateReplicaRequest;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;

/**
 * Helper class that allows to resolve transaction state.
 */
public class TransactionStateResolver {
    /** Assignment node names per replication group. */
    private final Map<ReplicationGroupId, LinkedHashSet<String>> primaryReplicaMapping = new ConcurrentHashMap<>();

    /** Replication service. */
    private final ReplicaService replicaService;

    /** Function that resolves a node consistent ID to a cluster node. */
    private final Function<String, ClusterNode> clusterNodeResolver;

    /**
     * The constructor.
     *
     * @param replicaService Replication service.
     */
    public TransactionStateResolver(ReplicaService replicaService, Function<String, ClusterNode> clusterNodeResolver) {
        this.replicaService = replicaService;
        this.clusterNodeResolver = clusterNodeResolver;
    }

    /**
     * Sends a transaction state request to the primary replica.
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
     * @param nodeNames Assignment node names.
     */
    public void updateAssignment(ReplicationGroupId replicaGrpId, Collection<String> nodeNames) {
        primaryReplicaMapping.put(replicaGrpId, new LinkedHashSet<>(nodeNames));
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
        ClusterNode nodeToSend = primaryReplicaMapping.get(replicaGrp).stream()
                .map(clusterNodeResolver)
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new IgniteInternalException("All replica nodes are unavailable"));

        replicaService.invoke(nodeToSend, request).thenAccept(resp -> {
            assert resp instanceof LeaderOrTxState : "Unsupported response type [type=" + resp.getClass().getSimpleName() + ']';

            LeaderOrTxState stateOrLeader = (LeaderOrTxState) resp;

            String nextNodeToSend = stateOrLeader.leaderName();

            if (nextNodeToSend == null) {
                resFut.complete(stateOrLeader.txMeta());
            } else {
                LinkedHashSet<String> newAssignment = new LinkedHashSet<>();

                newAssignment.add(nextNodeToSend);
                newAssignment.addAll(primaryReplicaMapping.get(replicaGrp));

                primaryReplicaMapping.put(replicaGrp, newAssignment);

                sendAndRetry(resFut, replicaGrp, request);
            }
        });
    }
}
