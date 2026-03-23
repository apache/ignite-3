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

package org.apache.ignite.internal.raft.server;

import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The RAFT protocol based replication server. Supports multiple RAFT groups. The server listens for client commands, submits them to a
 * replicated log and calls {@link RaftGroupListener} {@code onRead} and {@code onWrite} methods after the command was committed to the
 * log.
 */
public interface RaftServer extends IgniteComponent {
    /**
     * Returns cluster service.
     */
    @TestOnly
    ClusterService clusterService();

    /**
     * Starts a Raft node bound to this cluster node.
     *
     * @param nodeId Raft node ID.
     * @param configuration Raft configuration.
     * @param lsnr Listener for state machine events.
     * @param groupOptions Options to apply to the group.
     * @return {@code true} if a group was successfully started, {@code false} if a group with given name already exists.
     */
    boolean startRaftNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupOptions groupOptions
    );

    /**
     * Starts a Raft group bound to this cluster node.
     *
     * @param nodeId Raft node ID.
     * @param configuration Raft configuration.
     * @param evLsnr Listener for group membership and other events.
     * @param lsnr Listener for state machine events.
     * @param groupOptions Options to apply to the group.
     * @return {@code True} if a group was successfully started, {@code False} when the group with given name is already exists.
     */
    boolean startRaftNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupEventsListener evLsnr,
            RaftGroupListener lsnr,
            RaftGroupOptions groupOptions
    );

    /**
     * Check if the node is started.
     *
     * @param nodeId Raft node ID.
     * @return True if the node is started.
     */
    boolean isStarted(RaftNodeId nodeId);

    /**
     * Stops a given local Raft node if it exists.
     *
     * @param nodeId Raft node ID.
     * @return {@code true} if the node has been stopped, {@code false} otherwise.
     */
    boolean stopRaftNode(RaftNodeId nodeId);

    /**
     * Stops all local nodes running the given Raft group.
     *
     * @param groupId Raft group ID.
     * @return {@code true} if at least one node has been stopped, {@code false} otherwise.
     */
    boolean stopRaftNodes(ReplicationGroupId groupId);

    /**
     * Destroys Raft group node storages (log storage, metadata storage and snapshots storage).
     *
     * <p>No durability guarantees are provided. If a node crashes, the storage may come to life.
     *
     * @param nodeId ID of the Raft node.
     * @param groupOptions Options for this group.
     */
    void destroyRaftNodeStorages(RaftNodeId nodeId, RaftGroupOptions groupOptions);

    /**
     * Destroys Raft group node storages (log storage, metadata storage and snapshots storage).
     *
     * <p>Destruction is durable: that is, if this method returns and after that the node crashes, after it starts up, the storage
     * will not be there.
     *
     * @param nodeId ID of the Raft node.
     * @param groupOptions Options for this group.
     */
    void destroyRaftNodeStoragesDurably(RaftNodeId nodeId, RaftGroupOptions groupOptions);

    /**
     * Returns information about index and term of the given node, or {@code null} if the group is not started.
     *
     * @param nodeId ID of the Raft node.
     */
    @Nullable IndexWithTerm raftNodeIndex(RaftNodeId nodeId);

    /**
     * Returns local nodes running the given Raft group.
     *
     * @param groupId Raft group ID.
     * @return List of peers (can be empty if no local Raft nodes have been started).
     */
    List<Peer> localPeers(ReplicationGroupId groupId);

    /**
     * Returns a set of locally running Raft nodes.
     *
     * @return Set of Raft node IDs (can be empty if no local Raft nodes have been started).
     */
    @TestOnly
    Set<RaftNodeId> localNodes();

    /**
     * Get a raft server options.
     *
     * @return Raft server options.
     */
    NodeOptions options();
}
