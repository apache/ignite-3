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
package org.apache.ignite.raft.jraft.counter;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.raft.jraft.counter.rpc.GetValueRequestProcessor;
import org.apache.ignite.raft.jraft.counter.rpc.IncrementAndGetRequestProcessor;
import org.apache.ignite.raft.jraft.counter.rpc.ValueResponse;
import org.apache.ignite.raft.jraft.rpc.TestIgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;

/**
 * Counter server that keeps a counter value in a raft group.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:51:02 PM
 */
public class CounterServer {
    private static Set<CounterServer> servers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private RaftGroupService raftGroupService;
    private Node node;
    private CounterStateMachine fsm;

    public CounterServer(final String dataPath, final String groupId, final PeerId serverId,
                         final NodeOptions nodeOptions) throws IOException {
        new File(dataPath).mkdirs();

        NodeManager nodeManager = new NodeManager();

        Configuration initialConf = nodeOptions.getInitialConf();

        List<PeerId> peers = initialConf.getPeers();

        List<String> addrs = peers.stream().map(p -> p.getEndpoint().toString()).collect(Collectors.toList());

        final TestIgniteRpcServer rpcServer = new TestIgniteRpcServer(serverId.getEndpoint(), addrs, nodeManager);
        nodeOptions.setRpcClient(new IgniteRpcClient(rpcServer.clusterService(), true));

        CounterService counterService = new CounterServiceImpl(this);
        rpcServer.registerProcessor(new GetValueRequestProcessor(counterService));
        rpcServer.registerProcessor(new IncrementAndGetRequestProcessor(counterService));

        this.fsm = new CounterStateMachine();

        nodeOptions.setFsm(this.fsm);
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");

        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer, nodeManager);
        this.node = this.raftGroupService.start();
    }

    public static void stopAll() throws InterruptedException {
        for (CounterServer server : servers) {
            server.shutdown();
        }
    }

    public void shutdown() throws InterruptedException {
        raftGroupService.shutdown();

        servers.remove(this);
    }

    public CounterStateMachine getFsm() {
        return this.fsm;
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService raftGroupService() {
        return this.raftGroupService;
    }

    /**
     * Redirect request to new leader
     */
    public ValueResponse redirect() {
        final ValueResponse response = new ValueResponse();
        response.setSuccess(false);
        if (this.node != null) {
            final PeerId leader = this.node.getLeaderId();
            if (leader != null) {
                response.setRedirect(leader.toString());
            }
        }
        return response;
    }

    public static CounterServer start(String dataPath, String groupId, PeerId serverId, Configuration initConf) throws IOException {
        final NodeOptions nodeOptions = new NodeOptions();

        nodeOptions.setElectionTimeoutMs(1000);
        nodeOptions.setDisableCli(false);
        nodeOptions.setSnapshotIntervalSecs(30);
        nodeOptions.setInitialConf(initConf);

        File serverData = new File(dataPath, serverId.toString().replaceAll("\\W+", ""));

        if (!serverData.exists()) {
            if (!serverData.mkdirs())
                throw new IllegalArgumentException("Failed to create server data path:" + serverData);
        }

        final CounterServer counterServer = new CounterServer(serverData.getPath(), groupId, serverId, nodeOptions);
        System.out.println("Started counter server at port:"
                           + counterServer.getNode().getNodeId().getPeerId().getPort());

        servers.add(counterServer);

        return counterServer;
    }
}
