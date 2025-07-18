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

import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.TestOnly;

/** Utilities for working with replicas and replicas manager in tests. */
public final class ReplicaTestUtils {
    /**
     * Returns raft-client if exists.
     *
     * @param node Ignite node that hosts the raft-client.
     * @param tableOrZoneId Desired table or zone ID.
     * @param partId Desired partition's ID.
     *
     * @return Optional with raft-client if exists on the node by given identifiers.
     */
    @TestOnly
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 tableOrZoneId -> zoneId
    public static Optional<RaftGroupService> getRaftClient(Ignite node, int tableOrZoneId, int partId) {
        return getRaftClient(getReplicaManager(node), tableOrZoneId, partId);
    }

    /**
     * Returns raft-client if exists.
     *
     * @param replicaManager Ignite node's replica manager with replica that should contains a raft client.
     * @param tableOrZoneId Desired table or zone ID.
     * @param partId Desired partition's ID.
     *
     * @return Optional with raft-client if exists on the node by given identifiers.
     */
    @TestOnly
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 tableOrZoneId -> zoneId
    public static Optional<RaftGroupService> getRaftClient(ReplicaManager replicaManager, int tableOrZoneId, int partId) {
        CompletableFuture<Replica> replicaFut = replicaManager
                .replica(colocationEnabled() ? new ZonePartitionId(tableOrZoneId, partId) : new TablePartitionId(tableOrZoneId, partId));

        if  (replicaFut == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(replicaFut.get(15, TimeUnit.SECONDS).raftClient());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            return Optional.empty();
        }
    }

    /**
     * Extracts {@link ReplicaManager} from the given {@link Ignite} node.
     *
     * @param node The given node with desired replica manager.
     *
     * @return Replica manager component from given node.
     */
    @TestOnly
    public static ReplicaManager getReplicaManager(Ignite node) {
        return IgniteTestUtils.getFieldValue(node, "replicaMgr");
    }

    /**
     * Extracts {@link TopologyService} from the given {@link Ignite} node.
     *
     * @param node The given node with desired topology service.
     *
     * @return Topology service component from given node.
     */
    @TestOnly
    private static TopologyService getTopologyService(Ignite node) {
        ClusterService clusterService = IgniteTestUtils.getFieldValue(node, "clusterSvc");
        return clusterService.topologyService();
    }

    /**
     * Returns cluster node that is the leader of the corresponding partition group or throws an exception if it cannot be found.
     *
     * @param node Ignite node with raft client.
     * @param tableOrZoneId Table or zone identifier.
     * @param partId Partition number.
     *
     * @return Leader node of the partition group corresponding to the partition
     */
    @TestOnly
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 tableOrZoneId -> zoneId
    public static ClusterNode leaderAssignment(Ignite node, int tableOrZoneId, int partId) {
        return leaderAssignment(getReplicaManager(node), getTopologyService(node), tableOrZoneId, partId);
    }

    /**
     * Returns cluster node that is the leader of the corresponding partition group or throws an exception if it cannot be found.
     *
     * @param replicaManager Ignite node's replica manager with replica that should contains a raft client.
     * @param topologyService Ignite node's topology service that should find and return leader cluster node.
     * @param tableOrZoneId Table or zone identifier.
     * @param partId Partition number.
     *
     * @return Leader node of the partition group corresponding to the partition
     */
    @TestOnly
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 tableOrZoneId -> zoneId
    public static ClusterNode leaderAssignment(
            ReplicaManager replicaManager,
            TopologyService topologyService,
            int tableOrZoneId,
            int partId
    ) {
        RaftGroupService raftClient = getRaftClient(replicaManager, tableOrZoneId, partId)
                // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Pay attention to " in table or zone "
                .orElseThrow(() -> new IgniteInternalException("No such partition " + partId + " in table or zone " + tableOrZoneId));

        if (raftClient.leader() == null) {
            try {
                raftClient.refreshLeader().get(15, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new IgniteInternalException("Couldn't get a leader for partition " + partId + " in table " + tableOrZoneId, e);
            }
        }

        return topologyService.getByConsistentId(raftClient.leader().consistentId());
    }
}
