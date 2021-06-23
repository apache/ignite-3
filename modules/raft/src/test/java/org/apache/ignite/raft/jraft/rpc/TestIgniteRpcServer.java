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

package org.apache.ignite.raft.jraft.rpc;

import java.util.List;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;

/**
 * RPC server configured for integration tests.
 */
public class TestIgniteRpcServer extends IgniteRpcServer {
    /**
     * @param clusterService Cluster service.
     * @param servers Server list.
     * @param nodeManager Node manager
     * @param nodeOptions Node options.
     */
    public TestIgniteRpcServer(ClusterService clusterService, List<String> servers, NodeManager nodeManager, NodeOptions nodeOptions) {
        super(
            clusterService,
            nodeManager,
            new RaftClientMessagesFactory(),
            JRaftUtils.createRequestExecutor(nodeOptions)
        );
    }
}
