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
package com.alipay.sofa.jraft.counter;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.counter.rpc.GetValueRequestProcessor;
import com.alipay.sofa.jraft.counter.rpc.IncrementAndGetRequestProcessor;
import com.alipay.sofa.jraft.counter.rpc.ValueResponse;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
        // 初始化路径
        new File(dataPath).mkdirs();

        // 这里让 raft RPC 和业务 RPC 使用同一个 RPC server, 通常也可以分开
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        // 注册业务处理器
        CounterService counterService = new CounterServiceImpl(this);
        rpcServer.registerProcessor(new GetValueRequestProcessor(counterService));
        rpcServer.registerProcessor(new IncrementAndGetRequestProcessor(counterService));
        // 初始化状态机
        this.fsm = new CounterStateMachine();
        // 设置状态机到启动参数
        nodeOptions.setFsm(this.fsm);
        // 设置存储路径
        // 日志, 必须
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // 元信息, 必须
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // snapshot, 可选, 一般都推荐
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // 初始化 raft group 服务框架
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
        // 启动
        this.node = this.raftGroupService.start();
    }

    public static void stopAll() throws InterruptedException {
        for (CounterServer server : servers) {
            server.shutdown();
        }
    }

    public void shutdown() throws InterruptedException {
        node.shutdown();
        node.join();

        servers.remove(this);
    }

    public CounterStateMachine getFsm() {
        return this.fsm;
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService RaftGroupService() {
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

    public static CounterServer start(String dataPath, String groupId, String serverIdStr, String initConfStr) throws IOException {
        final NodeOptions nodeOptions = new NodeOptions();

        nodeOptions.setElectionTimeoutMs(1000);
        nodeOptions.setDisableCli(false);
        nodeOptions.setSnapshotIntervalSecs(30);
        final PeerId serverId = new PeerId();
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        final Configuration initConf = new Configuration();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        nodeOptions.setInitialConf(initConf);

        File serverData = new File(dataPath, serverIdStr.replaceAll("\\W+", ""));

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
