package com.alipay.sofa.jraft.rpc.impl;

import com.alipay.sofa.jraft.rpc.Connection;
import com.alipay.sofa.jraft.util.Endpoint;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalConnection implements Connection {
    private Map<String, Object> attrs = new ConcurrentHashMap<>();

    final LocalRpcClient client;
    final Endpoint srv;

    public LocalConnection(LocalRpcClient client, Endpoint srv) {
        this.client = client;
        this.srv = srv;
    }

    @Override public Object getAttribute(String key) {
        return attrs.get(key);
    }

    @Override public void setAttribute(String key, Object value) {
        attrs.put(key, value);
    }

    @Override public Object setAttributeIfAbsent(String key, Object value) {
        return attrs.putIfAbsent(key, value);
    }

    @Override public void close() {
        LocalRpcServer.closeConnection(client, srv);
    }
}
