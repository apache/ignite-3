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
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftGroupId;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.network.ClusterService;
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
    ClusterService clusterService();

    /**
     * Starts a raft group bound to this cluster node.
     *
     * @param groupId Group id.
     * @param configuration Raft peers configuration.
     * @param lsnr Listener for state machine events.
     * @param groupOptions Options to apply to the group.
     * @return {@code True} if a group was successfully started, {@code False} when the group with given name is already exists.
     */
    boolean startRaftGroup(
            RaftGroupId groupId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupOptions groupOptions
    );

    /**
     * Starts a raft group bound to this cluster node.
     *
     * @param groupId Group id.
     * @param configuration Raft peers configuration.
     * @param evLsnr Listener for group membership and other events.
     * @param lsnr Listener for state machine events.
     * @param groupOptions Options to apply to the group.
     * @return {@code True} if a group was successfully started, {@code False} when the group with given name is already exists.
     */
    boolean startRaftGroup(
            RaftGroupId groupId,
            PeersAndLearners configuration,
            RaftGroupEventsListener evLsnr,
            RaftGroupListener lsnr,
            RaftGroupOptions groupOptions
    );

    /**
     * Stops a given local Raft node if it exists.
     *
     * @param groupId Group id.
     * @return {@code true} if the node has been stopped, {@code false} otherwise.
     */
    boolean stopRaftNode(RaftGroupId groupId);

    /**
     * Stops all local nodes running the given Raft group.
     *
     * @param replicationGroupId Raft group name.
     * @return {@code true} if at least one node has been stopped, {@code false} otherwise.
     */
    boolean stopRaftNodes(ReplicationGroupId replicationGroupId);

    /**
     * Returns local nodes running the given Raft group.
     *
     * @param groupId Group id.
     * @return List of peers (can be empty if no local Raft nodes have been started).
     */
    List<Peer> localPeers(ReplicationGroupId groupId);

    /**
     * Returns a set of started partition groups.
     *
     * @return Started groups.
     */
    @TestOnly
    Set<RaftGroupId> startedGroups();
}
