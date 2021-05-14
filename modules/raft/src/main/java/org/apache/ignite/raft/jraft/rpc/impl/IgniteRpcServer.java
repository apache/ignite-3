package org.apache.ignite.raft.jraft.rpc.impl;

import io.scalecube.cluster.ClusterConfig;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.network.scalecube.message.ScaleCubeMessage;
import org.apache.ignite.network.scalecube.message.ScaleCubeMessageSerializationFactory;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.RpcUtils;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.GetFileRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.InstallSnapshotRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.ReadIndexRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.RequestVoteRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.TimeoutNowRequestProcessor;
import org.apache.ignite.raft.jraft.util.Endpoint;

public class IgniteRpcServer implements RpcServer<Void> {
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteRpcServer.class);

    /** The {@code true} to reuse cluster service. */
    private final boolean reuse;

    private ClusterService service;

    private List<ConnectionClosedEventListener> listeners = new CopyOnWriteArrayList<>();

    private Map<String, RpcProcessor> processors = new ConcurrentHashMap<>();

    private final NodeManager nodeManager;

    public IgniteRpcServer(Endpoint endpoint, NodeManager nodeManager) {
        this(endpoint.getIp() + ":" + endpoint.getPort(), endpoint.getPort(), List.of(), nodeManager);
    }

    public IgniteRpcServer(Endpoint endpoint, List<String> servers, NodeManager nodeManager) {
        this(endpoint.getIp() + ":" + endpoint.getPort(), endpoint.getPort(), servers, nodeManager);
    }

    // TODO asch Should always use injected cluster service.
    public IgniteRpcServer(String name, int port, List<String> servers, NodeManager nodeManager) {
        var serializationRegistry = new MessageSerializationRegistry()
            .registerFactory(ScaleCubeMessage.TYPE, new ScaleCubeMessageSerializationFactory());

        var context = new ClusterLocalConfiguration(name, port, servers, serializationRegistry);
        var factory = new ScaleCubeClusterServiceFactory() {
            @Override protected ClusterConfig defaultConfig() {
                return super.defaultConfig();
            }
        };

        reuse = false;
        this.nodeManager = nodeManager;

        init(factory.createClusterService(context));
    }

    public IgniteRpcServer(ClusterService service, boolean reuse, NodeManager nodeManager) {
        this.reuse = reuse;
        this.nodeManager = nodeManager;

        init(service);
    }

    public ClusterService clusterService() {
        return service;
    }

    private void init(ClusterService service) {
        this.service = service;

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
//        registerProcessor(new AddPeerRequestProcessor(cliExecutor));
//        registerProcessor(new RemovePeerRequestProcessor(cliExecutor));
//        registerProcessor(new ResetPeerRequestProcessor(cliExecutor));
//        registerProcessor(new ChangePeersRequestProcessor(cliExecutor));
//        registerProcessor(new GetLeaderRequestProcessor(cliExecutor));
//        registerProcessor(new SnapshotRequestProcessor(cliExecutor));
//        registerProcessor(new TransferLeaderRequestProcessor(cliExecutor));
//        registerProcessor(new GetPeersRequestProcessor(cliExecutor));
//        registerProcessor(new AddLearnersRequestProcessor(cliExecutor));
//        registerProcessor(new RemoveLearnersRequestProcessor(cliExecutor));
//        registerProcessor(new ResetLearnersRequestProcessor(cliExecutor));

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
        service.start();

        return true;
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
