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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;

/**
 * Interface for replica server.
 */
public interface Replica {
    /**
     * Returns replica's RAFT client.
     *
     * @return RAFT client.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-23974
    @Deprecated(forRemoval = true)
    TopologyAwareRaftGroupService raftClient();

    /**
     * Returns replica's listener.
     *
     * @return Replica's listener.
     */
    @Deprecated(forRemoval = true)
    ReplicaListener listener();

    /**
     * Processes a replication request on the replica.
     *
     * @param request Request to replication.
     * @param senderId Sender id.
     * @return Response.
     */
    CompletableFuture<ReplicaResult> processRequest(ReplicaRequest request, UUID senderId);

    /**
     * Replica group identity, this id is the same as the considered partition's id.
     *
     * @return Group id.
     */
    ReplicationGroupId groupId();

    /**
     * Process placement driver message.
     *
     * @param msg Message to process.
     * @return Future that contains a result.
     */
    CompletableFuture<? extends NetworkMessage> processPlacementDriverMessage(PlacementDriverReplicaMessage msg);

    /**
     * Shutdowns the replica.
     */
    CompletableFuture<Void> shutdown();

    /**
     * Updates local peers and learners.
     *
     * @param peersAndLearners Peers and learners.
     */
    void updatePeersAndLearners(PeersAndLearners peersAndLearners);

    /**
     * Creates a snapshot on a given group member.
     *
     * @param targetMember Group member that will create a snapshot.
     * @param forced {@code True} to force snapshot and log truncation.
     * @return Future that gets completed when the target member creates a snapshot.
     */
    CompletableFuture<Void> createSnapshotOn(Member targetMember, boolean forced);

    /**
     * Transfers leadership from the current group leader to the target group member.
     *
     * @param targetConsistentId Name of the group member that will become a new leader. Must be a voting member of the replication group.
     * @return Future that gets completed when the current leader executes the request and steps down. Note that this does not imply any
     *         happens-before relationship between the completion of the future and the target member becoming the new group leader.
     */
    CompletableFuture<Void> transferLeadershipTo(String targetConsistentId);
}
