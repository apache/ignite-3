package org.apache.ignite.raft.jraft.rpc.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkClusterEventHandler;
import org.apache.ignite.network.NetworkHandlersProvider;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcServer;

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
                    @Override public void onReceived(NetworkMessage message, NetworkMember sender, String corellationId) {

                    }
                };
            }

            @Override public NetworkClusterEventHandler clusterEventHandler() {
                return new NetworkClusterEventHandler() {
                    @Override public void onAppeared(NetworkMember member) {

                    }

                    @Override public void onDisappeared(NetworkMember member) {
//                        for (ConnectionClosedEventListener listener : listeners) {
//                            listener.onClosed(member.name(), );
//                        }
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
