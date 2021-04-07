package org.apache.ignite.raft.jraft.rpc.impl;

import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcServer;

public class IgniteRpcServer implements RpcServer {
    @Override public void registerConnectionClosedEventListener(ConnectionClosedEventListener listener) {
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
