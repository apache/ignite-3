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
package org.apache.ignite.raft.jraft.rpc.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.cli.AddLearnersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.AddPeerRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.ChangePeersAsyncRequestProcessor;
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
import org.jetbrains.annotations.Nullable;

/**
 * TODO https://issues.apache.org/jira/browse/IGNITE-14519 Unsubscribe on shutdown
 */
public class IgniteRpcServer implements RpcServer<Void> {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteRpcServer.class);

    private final ClusterService service;

    private final NodeManager nodeManager;

    private final Executor rpcExecutor;

    private final List<ConnectionClosedEventListener> listeners = new CopyOnWriteArrayList<>();

    private final Map<String, RpcProcessor> processors = new ConcurrentHashMap<>();

    /**
     * @param service The cluster service.
     * @param nodeManager The node manager.
     * @param raftMessagesFactory Message factory.
     * @param rpcExecutor The executor for RPC requests.
     */
    public IgniteRpcServer(
        ClusterService service,
        NodeManager nodeManager,
        RaftMessagesFactory raftMessagesFactory,
        Executor rpcExecutor
    ) {
        this.service = service;
        this.nodeManager = nodeManager;
        this.rpcExecutor = rpcExecutor;

        // raft server RPC
        AppendEntriesRequestProcessor appendEntriesRequestProcessor =
            new AppendEntriesRequestProcessor(rpcExecutor, raftMessagesFactory);
        registerConnectionClosedEventListener(appendEntriesRequestProcessor);
        registerProcessor(appendEntriesRequestProcessor);
        registerProcessor(new GetFileRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new InstallSnapshotRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new RequestVoteRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new PingRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new TimeoutNowRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new ReadIndexRequestProcessor(rpcExecutor, raftMessagesFactory));
        // raft native cli service
        registerProcessor(new AddPeerRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new RemovePeerRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new ResetPeerRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new ChangePeersRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new ChangePeersAsyncRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new GetLeaderRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new SnapshotRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new TransferLeaderRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new GetPeersRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new AddLearnersRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new RemoveLearnersRequestProcessor(rpcExecutor, raftMessagesFactory));
        registerProcessor(new ResetLearnersRequestProcessor(rpcExecutor, raftMessagesFactory));
        // common client integration
        registerProcessor(new ActionRequestProcessor(rpcExecutor, raftMessagesFactory));

        var messageHandler = new RpcMessageHandler();

        // Add the handler after all processors are set up.
        service.messagingService().addMessageHandler(RaftMessageGroup.class, messageHandler);

        service.topologyService().addEventHandler(new TopologyEventHandler() {
            @Override public void onAppeared(ClusterNode member) {
                // TODO https://issues.apache.org/jira/browse/IGNITE-14837
                // Perhaps, We can remove checking for dead nodes and replace it with SWIM node alive event
                // and start replicator when the event is received.
            }

            @Override public void onDisappeared(ClusterNode member) {
                for (ConnectionClosedEventListener listener : listeners)
                    listener.onClosed(service.topologyService().localMember().name(), member.name());
            }
        });
    }

    /**
     * Implementation of a message handler that dispatches the incoming requests to a suitable {@link RpcProcessor}.
     */
    public class RpcMessageHandler implements NetworkMessageHandler {
        /** {@inheritDoc} */
        @Override public void onReceived(NetworkMessage message, NetworkAddress senderAddr, @Nullable Long correlationId) {
            Class<? extends NetworkMessage> cls = message.getClass();
            RpcProcessor<NetworkMessage> prc = processors.get(cls.getName());

            // TODO asch cache mapping https://issues.apache.org/jira/browse/IGNITE-14832
            if (prc == null) {
                for (Class<?> iface : cls.getInterfaces()) {
                    prc = processors.get(iface.getName());

                    if (prc != null)
                        break;
                }
            }

            if (prc == null)
                return;

            RpcProcessor.ExecutorSelector selector = prc.executorSelector();

            Executor executor = null;

            if (selector != null)
                executor = selector.select(prc.getClass().getName(), message, nodeManager);

            if (executor == null)
                executor = prc.executor();

            if (executor == null)
                executor = rpcExecutor;

            RpcProcessor<NetworkMessage> finalPrc = prc;

            try {
                executor.execute(() -> {
                    var context = new RpcContext() {
                        @Override public NodeManager getNodeManager() {
                            return nodeManager;
                        }

                        @Override public void sendResponse(Object responseObj) {
                            service.messagingService().respond(senderAddr, (NetworkMessage) responseObj, correlationId);
                        }

                        @Override public NetworkAddress getRemoteAddress() {
                            return senderAddr;
                        }

                        @Override public NetworkAddress getLocalAddress() {
                            return service.topologyService().localMember().address();
                        }
                    };

                    finalPrc.handleRequest(context, message);
                });
            } catch (RejectedExecutionException e) {
                // The rejection is ok if an executor has been stopped, otherwise it shouldn't happen.
                LOG.warn("A request execution was rejected [sender={} req={} reason={}]", senderAddr, S.toString(message), e.getMessage());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void registerConnectionClosedEventListener(ConnectionClosedEventListener listener) {
        if (!listeners.contains(listener))
            listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public void registerProcessor(RpcProcessor<?> processor) {
        processors.put(processor.interest(), processor);
    }

    /** {@inheritDoc} */
    @Override public int boundPort() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean init(Void opts) {
        return true;
    }

    public ClusterService clusterService() {
        return service;
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        // Should deregister listeners.
    }
}
