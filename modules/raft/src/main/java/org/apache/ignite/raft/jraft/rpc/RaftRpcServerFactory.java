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

import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.rpc.impl.PingRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.AddLearnersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.AddPeerRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.ChangePeersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.GetLeaderRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.GetPeersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.RemoveLearnersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.RemovePeerRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.ResetLearnersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.ResetPeerRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.SnapshotRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.TransferLeaderRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.GetFileRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.InstallSnapshotRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.ReadIndexRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.RequestVoteRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.TimeoutNowRequestProcessor;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.RpcFactoryHelper;

/**
 * Raft RPC server factory.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public class RaftRpcServerFactory {

    /**
     * Creates a raft RPC server with default request executors.
     *
     * @param endpoint server address to bind
     * @return a rpc server instance
     */
    public static RpcServer createRaftRpcServer(final Endpoint endpoint) {
        return createRaftRpcServer(endpoint, null, null);
    }

    /**
     * Creates a raft RPC server with executors to handle requests.
     *
     * @param endpoint      server address to bind
     * @param raftExecutor  executor to handle RAFT requests.
     * @param cliExecutor   executor to handle CLI service requests.
     * @return a rpc server instance
     */
    public static RpcServer createRaftRpcServer(final Endpoint endpoint, final Executor raftExecutor,
                                                final Executor cliExecutor) {
        final RpcServer rpcServer = RpcFactoryHelper.rpcFactory().createRpcServer(endpoint);
        addRaftRequestProcessors(rpcServer, raftExecutor, cliExecutor);
        return rpcServer;
    }

    /**
     * Adds RAFT and CLI service request processors with default executor.
     *
     * @param rpcServer rpc server instance
     */
    public static void addRaftRequestProcessors(final RpcServer rpcServer) {
        addRaftRequestProcessors(rpcServer, null, null);
    }

    /**
     * Adds RAFT and CLI service request processors.
     *
     * @param rpcServer    rpc server instance
     * @param raftExecutor executor to handle RAFT requests.
     * @param cliExecutor  executor to handle CLI service requests.
     */
    public static void addRaftRequestProcessors(final RpcServer rpcServer, final Executor raftExecutor,
                                                final Executor cliExecutor) {
        // raft core processors
        final AppendEntriesRequestProcessor appendEntriesRequestProcessor = new AppendEntriesRequestProcessor(
            raftExecutor);
        rpcServer.registerConnectionClosedEventListener(appendEntriesRequestProcessor);
        rpcServer.registerProcessor(appendEntriesRequestProcessor);
        rpcServer.registerProcessor(new GetFileRequestProcessor(raftExecutor));
        rpcServer.registerProcessor(new InstallSnapshotRequestProcessor(raftExecutor));
        rpcServer.registerProcessor(new RequestVoteRequestProcessor(raftExecutor));
        rpcServer.registerProcessor(new PingRequestProcessor());
        rpcServer.registerProcessor(new TimeoutNowRequestProcessor(raftExecutor));
        rpcServer.registerProcessor(new ReadIndexRequestProcessor(raftExecutor));
        // raft cli service
        rpcServer.registerProcessor(new AddPeerRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new RemovePeerRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new ResetPeerRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new ChangePeersRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new GetLeaderRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new SnapshotRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new TransferLeaderRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new GetPeersRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new AddLearnersRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new RemoveLearnersRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new ResetLearnersRequestProcessor(cliExecutor));
    }

    /**
     * Creates a raft RPC server and starts it.
     *
     * @param endpoint server address to bind
     * @return a rpc server instance
     */
    public static RpcServer createAndStartRaftRpcServer(final Endpoint endpoint) {
        return createAndStartRaftRpcServer(endpoint, null, null);
    }

    /**
     * Creates a raft RPC server and starts it.
     *
     * @param endpoint     server address to bind
     * @param raftExecutor executor to handle RAFT requests.
     * @param cliExecutor  executor to handle CLI service requests.
     * @return a rpc server instance
     */
    public static RpcServer createAndStartRaftRpcServer(final Endpoint endpoint, final Executor raftExecutor,
                                                        final Executor cliExecutor) {
        final RpcServer server = createRaftRpcServer(endpoint, raftExecutor, cliExecutor);
        server.init(null);
        return server;
    }
}
