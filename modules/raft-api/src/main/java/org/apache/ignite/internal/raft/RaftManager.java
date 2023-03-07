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
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Raft manager.
 */
public interface RaftManager extends IgniteComponent {
    /**
     * Starts a Raft group and a Raft service on the current node.
     *
     * @param nodeId Raft node ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnr Raft group listener.
     * @param eventsLsnr Raft group events listener.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> startRaftGroupNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr
    ) throws NodeStoppingException;

    /**
     * Starts a Raft group and a Raft service on the current node, using the given raft group service.
     *
     * @param nodeId Raft node ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param lsnr Raft group listener.
     * @param eventsLsnr Raft group events listener.
     * @param factory Service factory.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> startRaftGroupNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupEventsListener eventsLsnr,
            RaftServiceFactory factory
    ) throws NodeStoppingException;

    /**
     * Stops a given local Raft node.
     *
     * @param nodeId Raft node ID.
     * @return {@code true} if the node has been stopped, {@code false} otherwise.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    boolean stopRaftNode(RaftNodeId nodeId) throws NodeStoppingException;

    /**
     * Stops all local nodes running the given Raft group.
     *
     * <p>This method is different from {@link #stopRaftNode} as it stops all nodes that belong to the same Raft group. This can happen
     * when a Peer and a Learner are started on the same Ignite node.
     *
     * @param groupId Raft group ID.
     * @return {@code true} if at least one node has been stopped, {@code false} otherwise.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    boolean stopRaftNodes(ReplicationGroupId groupId) throws NodeStoppingException;

    /**
     * Creates a Raft group service providing operations on a Raft group.
     *
     * @param groupId Raft group ID.
     * @param configuration Peers and Learners of the Raft group.
     * @return Future that will be completed with an instance of a Raft group service.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners configuration
    ) throws NodeStoppingException;

    /**
     * Creates a Raft group service providing operations on a Raft group, using the given factory.
     *
     * @param groupId Raft group ID.
     * @param configuration Peers and Learners of the Raft group.
     * @param factory Factory that should be used to create raft service.
     * @return Future that will be completed with an instance of a Raft group service.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<RaftGroupService> startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners configuration,
            RaftServiceFactory factory
    ) throws NodeStoppingException;
}
