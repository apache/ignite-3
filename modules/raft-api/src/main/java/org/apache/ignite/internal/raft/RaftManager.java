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

package org.apache.ignite.internal.raft;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Raft manager.
 */
public interface RaftManager extends IgniteComponent {
    /**
     * Creates a raft group service providing operations on a raft group.
     *
     * @param groupId Raft group id.
     * @param serverPeer Local peer that will host the Raft node. If {@code null} - no nodes will be started, but only the Raft client.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnrSupplier Raft group listener supplier.
     * @return Future representing pending completion of the operation.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> prepareRaftGroup(
            ReplicationGroupId groupId,
            @Nullable Peer serverPeer,
            PeersAndLearners configuration,
            Supplier<RaftGroupListener> lsnrSupplier
    ) throws NodeStoppingException;

    /**
     * Creates a raft group service providing operations on a raft group.
     *
     * @param groupId Raft group id.
     * @param serverPeer Local peer that will host the Raft node. If {@code null} - no nodes will be started, but only the Raft client.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnrSupplier Raft group listener supplier.
     * @param raftGrpEvtsLsnrSupplier Raft group events listener supplier.
     * @return Future representing pending completion of the operation.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> prepareRaftGroup(
            ReplicationGroupId groupId,
            @Nullable Peer serverPeer,
            PeersAndLearners configuration,
            Supplier<RaftGroupListener> lsnrSupplier,
            Supplier<RaftGroupEventsListener> raftGrpEvtsLsnrSupplier
    ) throws NodeStoppingException;

    /**
     * Stops a given local Raft node.
     *
     * @param groupId Raft group id.
     * @return {@code true} if the node has been stopped, {@code false} otherwise.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    boolean stopRaftNode(RaftGroupId groupId) throws NodeStoppingException;

    /**
     * Stops all local nodes running the given Raft group.
     *
     * @param replicationGroupId Raft group name.
     * @return {@code true} if at least one node has been stopped, {@code false} otherwise.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    boolean stopRaftNodes(ReplicationGroupId replicationGroupId) throws NodeStoppingException;

    /**
     * Creates and starts a raft group service providing operations on a raft group.
     *
     * @param groupId Raft group id.
     * @param configuration Peers and Learners of the Raft group.
     * @return Future that will be completed with an instance of RAFT group service.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners configuration
    ) throws NodeStoppingException;
}
