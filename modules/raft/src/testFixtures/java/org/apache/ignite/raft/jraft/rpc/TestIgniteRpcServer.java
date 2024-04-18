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

package org.apache.ignite.raft.jraft.rpc;

import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.server.impl.RaftServiceEventInterceptor;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.NullActionRequestInterceptor;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.raft.jraft.rpc.impl.core.NullAppendEntriesRequestInterceptor;
import org.apache.ignite.raft.messages.TestMessageGroup;

/**
 * RPC server configured for integration tests.
 */
public class TestIgniteRpcServer extends IgniteRpcServer {
    /**
     * @param clusterService Cluster service.
     * @param nodeManager Node manager.
     * @param nodeOptions Node options.
     * @param requestExecutor Requests executor.
     */
    public TestIgniteRpcServer(ClusterService clusterService, NodeManager nodeManager, NodeOptions nodeOptions,
            ExecutorService requestExecutor) {
        super(
                clusterService,
                nodeManager,
                nodeOptions.getRaftMessagesFactory(),
                requestExecutor,
                new RaftServiceEventInterceptor(),
                new RaftGroupEventsClientListener(),
                new NullAppendEntriesRequestInterceptor(),
                new NullActionRequestInterceptor()
        );

        clusterService.messagingService().addMessageHandler(TestMessageGroup.class, new RpcMessageHandler());
    }
}
