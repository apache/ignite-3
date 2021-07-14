/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;

/**
 * Best raft manager ever since 1982.
 */
public class Loza {
    /** Factory. */
    private static final RaftClientMessagesFactory FACTORY = new RaftClientMessagesFactory();

    /** Timeout. */
    private static final int TIMEOUT = 1000;

    /** Retry delay. */
    private static final int DELAY = 200;

    /** Cluster network service. */
    private final ClusterService clusterNetSvc;

    /** Raft server. */
    private final RaftServer raftServer;

    /** Started groups. */
    private ConcurrentMap<String, RaftGroupServiceImpl> groups = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param clusterNetSvc Cluster network service.
     */
    public Loza(ClusterService clusterNetSvc, String dataPath) {
        this.clusterNetSvc = clusterNetSvc;

        this.raftServer = new JRaftServerImpl(clusterNetSvc, dataPath, FACTORY);
    }

    /**
     * Creates a RAFT group.
     *
     * @param groupId RAFT group id.
     * @param nodes Group nodes.
     * @param lsnr Group listener.
     * @return A RAFT group client.
     */
    public RaftGroupService startRaftGroup(String groupId, List<ClusterNode> nodes, RaftGroupListener lsnr) {
        assert !nodes.isEmpty();

        if (groups.containsKey(groupId))
            return groups.get(groupId);

        List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

        raftServer.startRaftGroup(groupId, lsnr, peers);

        groups.put(groupId, new RaftGroupServiceImpl(
            groupId,
            clusterNetSvc,
            FACTORY,
            TIMEOUT,
            peers,
            true,
            DELAY
        ));

        return groups.get(groupId);
    }

    /**
     * Stops a RAFT group.
     *
     * @param groupId RAFT group id.
     */
    public void stopRaftGroup(String groupId) {
        if (!groups.containsKey(groupId))
            return;

        raftServer.stopRaftGroup(groupId);

        groups.remove(groupId);
    }
}
