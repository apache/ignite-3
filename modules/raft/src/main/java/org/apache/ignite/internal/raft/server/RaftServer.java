/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupListener;
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
    ClusterService clusterService();

    /**
     * Starts a raft group bound to this cluster node.
     *
     * @param groupId     Group id.
     * @param lsnr        The listener.
     * @param initialConf Inititial group configuration.
     * @return {@code True} if a group was successfully started, {@code False} when the group with given name is already exists.
     */
    boolean startRaftGroup(String groupId, RaftGroupListener lsnr, List<Peer> initialConf);

    /**
     * Starts a raft group bound to this cluster node.
     *
     * @param groupId     Group id.
     * @param evLsnr      Listener for group membership and other events.
     * @param lsnr        Listener for state machine events.
     * @param initialConf Inititial group configuration.
     * @return {@code True} if a group was successfully started, {@code False} when the group with given name is already exists.
     */
    boolean startRaftGroup(String groupId, RaftGroupEventsListener evLsnr,
            RaftGroupListener lsnr, List<Peer> initialConf);

    /**
     * Synchronously stops a raft group if any.
     *
     * @param groupId Group id.
     * @return {@code True} if a group was successfully stopped.
     */
    boolean stopRaftGroup(String groupId);

    /**
     * Returns a local peer.
     *
     * @param groupId Group id.
     * @return Local peer or null if the group is not started.
     */
    @Nullable Peer localPeer(String groupId);

    /**
     * Returns a set of started partition groups.
     *
     * @return Started groups.
     */
    @TestOnly
    Set<String> startedGroups();
}
