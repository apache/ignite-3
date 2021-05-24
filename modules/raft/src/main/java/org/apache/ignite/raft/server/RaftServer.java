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

package org.apache.ignite.raft.server;

import java.util.List;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.jetbrains.annotations.Nullable;

/**
 * The RAFT protocol based replication server.
 * <p>
 * Supports multiple RAFT groups.
 * <p>
 * The server listens for client commands, submits them to a replicated log and calls {@link RaftGroupListener}
 * {@code onRead} and {@code onWrite} methods then after the command was committed to the log.
 */
public interface RaftServer {
    /**
     * @return Cluster service.
     */
    ClusterService clusterService();

    /**
     * Returns a path to the server persistence directory for a raft group.
     * @param groupId Group id.
     * @return The path.
     */
    String getServerDataPath(String groupId);

    /**
     * Starts a raft group on this cluster node.
     * @param groupId Group id.
     * @param lsnr The listener.
     * @param initialConf Inititial group configuration.
     *
     * @return {@code True} if a group was successfully started.
     * @throws IgniteInternalException If a group can't be started.
     */
    boolean startRaftGroup(String groupId, RaftGroupListener lsnr, List<Peer> initialConf);

    /**
     * Synchronously stops a raft group.
     * @param groupId Group id.
     * @return {@code True} if a group was successfully stopped.
     */
    boolean stopRaftGroup(String groupId);

    /**
     * Returns a local peer.
     * @param Group id.
     * @return Local peer, if presents.
     */
    @Nullable Peer localPeer(String groupId);

    /**
     * Shutdown a server.
     *
     * @throws Exception
     */
    void shutdown() throws Exception;
}
