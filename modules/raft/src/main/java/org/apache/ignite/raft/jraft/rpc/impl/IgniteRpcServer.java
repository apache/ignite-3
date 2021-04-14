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
import org.apache.ignite.raft.jraft.rpc.Connection;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.RpcUtils;

public class IgniteRpcServer implements RpcServer<Void> {
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteRpcServer.class);

    private final boolean reuse;

    private ClusterService service;

    private List<ConnectionClosedEventListener> listeners = new CopyOnWriteArrayList<>();

    private Map<String, RpcProcessor> processors = new ConcurrentHashMap<>();

    public IgniteRpcServer(ClusterService service, boolean reuse) {
        this.service = service;
        this.reuse = reuse;

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

                RpcProcessor.ExecutorSelector selector = prc.executorSelector();

                Executor executor = null;

                if (selector != null) {
                    executor = selector.select(null, msg);
                }

                if (executor == null)
                    executor = RpcUtils.RPC_CLOSURE_EXECUTOR;

                RpcProcessor finalPrc = prc;

                executor.execute(() -> {
                    finalPrc.handleRequest(new RpcContext() {
                        @Override public void sendResponse(Object responseObj) {
                            service.messagingService().send(sender, (NetworkMessage) responseObj, corellationId);
                        }

                        @Override public Connection getConnection() {
                            return null;
                        }

                        @Override public String getRemoteAddress() {
                            return sender.name();
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

        if (!reuse)
            service.start();
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
