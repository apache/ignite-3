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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.service.RaftGroupService;

/**
 * Replica server.
 */
public class Replica {
    /** Replica group identity, this id is the same as the considered partition's id. */
    private final ReplicationGroupId replicaGrpId;

    /** Replica listener. */
    private final ReplicaListener listener;

    private final RaftGroupService raftClient;

    /** Resolver that resolves a network address to cluster node. */
    private final Function<NetworkAddress, ClusterNode> clusterNodeResolver;

    /** Supplier of instance of {@link ClusterNode} which represents the local node. */
    private final Supplier<ClusterNode> localNodeSupplier;

    /**
     * The constructor of a replica server.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     * @param raftClient Raft client.
     * @param clusterNodeResolver Resolver that resolves a network address to cluster node.
     * @param localNodeSupplier Supplier of instance of {@link ClusterNode} which represents the local node.
     */
    public Replica(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener,
            // TODO https://issues.apache.org/jira/browse/IGNITE-17256 remove raftClient, clusterNodeResolver, localNodeSupplier
            RaftGroupService raftClient,
            Function<NetworkAddress, ClusterNode> clusterNodeResolver,
            Supplier<ClusterNode> localNodeSupplier
    ) {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
        this.raftClient = raftClient;
        this.clusterNodeResolver = clusterNodeResolver;
        this.localNodeSupplier = localNodeSupplier;
    }

    /**
     * Processes a replication request on the replica.
     *
     * @param request Request to replication.
     * @return Response.
     */
    public CompletableFuture<Object> processRequest(ReplicaRequest request) {
        assert replicaGrpId.equals(request.groupId()) : IgniteStringFormatter.format(
                "Partition mismatch: request does not match the replica [reqReplicaGrpId={}, replicaGrpId={}]",
                request.groupId(),
                replicaGrpId);

        return listener.invoke(request);
    }

    /**
     * Whether this replica on this node is primary.
     *
     * @return Whether is primary.
     */
    public boolean isPrimary() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-17256
        return raftClient.refreshAndGetLeaderWithTerm()
            .thenApply(leaderAndTerm -> clusterNodeResolver.apply(leaderAndTerm.get1().address()).equals(localNodeSupplier.get()))
            .join();
    }

    /**
     * Propagates safe time on non-primary replicas, if this replica is primary.
     */
    public void propagateSafeTime() {
        if (!isPrimary()) {
            return;
        }

        raftClient.run(new SafeTimeSyncCommand());
    }
}
