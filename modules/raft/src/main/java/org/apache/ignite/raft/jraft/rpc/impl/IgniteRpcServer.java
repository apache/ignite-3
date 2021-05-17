package org.apache.ignite.raft.jraft.rpc.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.RpcUtils;
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

/**
 * TODO https://issues.apache.org/jira/browse/IGNITE-14519 Unsubscribe on shutdown
 */
public class IgniteRpcServer implements RpcServer<Void> {
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteRpcServer.class);

    /** The {@code true} to reuse cluster service. */
    private final boolean reuse;

    private final ClusterService service;

    private List<ConnectionClosedEventListener> listeners = new CopyOnWriteArrayList<>();

    private Map<String, RpcProcessor> processors = new ConcurrentHashMap<>();

    private final NodeManager nodeManager; // TODO asch refactor (replace with a reference to self)

    public IgniteRpcServer(ClusterService service, boolean reuse, NodeManager nodeManager) {
        this.reuse = reuse;
        this.nodeManager = nodeManager;
        this.service = service;

        // TODO asch configure executors ?
        final Executor raftExecutor = null;
        final Executor cliExecutor = null;

        AppendEntriesRequestProcessor appendEntriesRequestProcessor = new AppendEntriesRequestProcessor(raftExecutor);
        registerConnectionClosedEventListener(appendEntriesRequestProcessor);
        registerProcessor(appendEntriesRequestProcessor);
        registerProcessor(new GetFileRequestProcessor(raftExecutor));
        registerProcessor(new InstallSnapshotRequestProcessor(raftExecutor));
        registerProcessor(new RequestVoteRequestProcessor(raftExecutor));
        registerProcessor(new PingRequestProcessor());
        registerProcessor(new TimeoutNowRequestProcessor(raftExecutor));
        registerProcessor(new ReadIndexRequestProcessor(raftExecutor));
        // raft cli service
        registerProcessor(new AddPeerRequestProcessor(cliExecutor));
        registerProcessor(new RemovePeerRequestProcessor(cliExecutor));
        registerProcessor(new ResetPeerRequestProcessor(cliExecutor));
        registerProcessor(new ChangePeersRequestProcessor(cliExecutor));
        registerProcessor(new GetLeaderRequestProcessor(cliExecutor));
        registerProcessor(new SnapshotRequestProcessor(cliExecutor));
        registerProcessor(new TransferLeaderRequestProcessor(cliExecutor));
        registerProcessor(new GetPeersRequestProcessor(cliExecutor));
        registerProcessor(new AddLearnersRequestProcessor(cliExecutor));
        registerProcessor(new RemoveLearnersRequestProcessor(cliExecutor));
        registerProcessor(new ResetLearnersRequestProcessor(cliExecutor));

        service.messagingService().addMessageHandler(new NetworkMessageHandler() {
            @Override public void onReceived(NetworkMessage msg, ClusterNode sender, String corellationId) {
                Class<? extends NetworkMessage> cls = msg.getClass();
                RpcProcessor prc = processors.get(cls.getName());

                // TODO asch cache it.
                if (prc == null) {
                    for (Class<?> iface : cls.getInterfaces()) {
                        prc = processors.get(iface.getName());

                        if (prc != null)
                            break;
                    }
                }

                if (prc == null)
                    return; // TODO asch use single message handler.

                RpcProcessor.ExecutorSelector selector = prc.executorSelector();

                Executor executor = null;

                if (selector != null) {
                    executor = selector.select(null, msg, nodeManager);
                }

                if (executor == null)
                    executor = RpcUtils.RPC_CLOSURE_EXECUTOR;

                RpcProcessor finalPrc = prc;

                executor.execute(() -> {
                    finalPrc.handleRequest(new RpcContext() {
                        @Override public NodeManager getNodeManager() {
                            return nodeManager;
                        }

                        @Override public void sendResponse(Object responseObj) {
                            service.messagingService().send(sender, (NetworkMessage) responseObj, corellationId);
                        }

                        @Override public String getRemoteAddress() {
                            return sender.address();
                        }
                    }, msg);
                });
            }
        });

        service.topologyService().addEventHandler(new TopologyEventHandler() {
            @Override public void onAppeared(ClusterNode member) {
                // TODO asch optimize start replicator ?
            }

            @Override public void onDisappeared(ClusterNode member) {
                for (ConnectionClosedEventListener listener : listeners)
                    listener.onClosed(service.topologyService().localMember().name(), member.name());
            }
        });
    }

    @Override public void registerConnectionClosedEventListener(ConnectionClosedEventListener listener) {
        if (!listeners.contains(listeners))
            listeners.add(listener);
    }

    @Override public void registerProcessor(RpcProcessor<?> processor) {
        processors.put(processor.interest(), processor);
    }

    @Override public int boundPort() {
        return 0;
    }

    @Override public boolean init(Void opts) {
        if (!reuse)
            service.start();

        return true;
    }

    public ClusterService clusterService() {
        return service;
    }

    @Override public void shutdown() {
        if (reuse)
            return;

        try {
            service.shutdown();
        }
        catch (Exception e) {
            throw new IgniteInternalException(e);
        }
    }
}
