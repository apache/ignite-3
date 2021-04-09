package org.apache.ignite.raft.jraft.rpc.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkClusterEventHandler;
import org.apache.ignite.network.NetworkHandlersProvider;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.raft.jraft.rpc.Connection;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.RpcUtils;

public class IgniteRpcServer implements RpcServer<Void> {
    private static final LogWrapper LOG = new LogWrapper(IgniteRpcServer.class);

    private NetworkCluster server;

    private List<ConnectionClosedEventListener> listeners = new CopyOnWriteArrayList<>();

    private Map<String, RpcProcessor> processors = new ConcurrentHashMap<>();

    public IgniteRpcServer(NetworkCluster server) {
        this.server = server;

        server.addHandlersProvider(new NetworkHandlersProvider() {
            @Override public NetworkMessageHandler messageHandler() {
                return new NetworkMessageHandler() {
                    @Override public void onReceived(NetworkMessage msg, NetworkMember sender, String corellationId) {
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
                                    server.send(sender, (NetworkMessage) responseObj, corellationId);
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
                };
            }

            @Override public NetworkClusterEventHandler clusterEventHandler() {
                return new NetworkClusterEventHandler() {
                    @Override public void onAppeared(NetworkMember member) {
                        // No-op.
                    }

                    @Override public void onDisappeared(NetworkMember member) {
                        for (ConnectionClosedEventListener listener : listeners)
                            listener.onClosed(server.localMember().name(), member.name());
                    }
                };
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
        return false;
    }

    @Override public void shutdown() {
    }
}
