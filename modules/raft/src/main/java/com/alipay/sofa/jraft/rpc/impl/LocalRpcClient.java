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
package com.alipay.sofa.jraft.rpc.impl;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.InvokeTimeoutException;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.Message;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.RpcUtils;
import com.alipay.sofa.jraft.util.Endpoint;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local rpc client impl.
 *
 * @author ascherbakov.
 */
public class LocalRpcClient implements RpcClient {
    private static final Logger LOG                    = LoggerFactory.getLogger(LocalRpcClient.class);

    public volatile ReplicatorGroup replicatorGroup = null;

    public static Consumer<LocalConnection> onCreated = null;

    @Override public boolean checkConnection(Endpoint endpoint) {
        return LocalRpcServer.connect(this, endpoint, false, null);
    }

    @Override public boolean checkConnection(Endpoint endpoint, boolean createIfAbsent) {
        return LocalRpcServer.connect(this, endpoint, createIfAbsent, this::onCreated);
    }

    @Override public void closeConnection(Endpoint endpoint) {
        LocalRpcServer.closeConnection(this, endpoint);
    }

    @Override public void registerConnectEventListener(ReplicatorGroup replicatorGroup) {
        this.replicatorGroup = replicatorGroup;
    }

    private void onCreated(LocalConnection conn) {
        if (replicatorGroup != null) {
            final PeerId peer = new PeerId();
            if (peer.parse(conn.srv.local.toString())) {
                RpcUtils.runInThread(() -> replicatorGroup.checkReplicator(peer, true)); // Avoid deadlock.
            }
            else
                LOG.warn("Failed to parse peer: {}", peer); // TODO asch how to handle ?
        }

        if (onCreated != null)
            onCreated.accept(conn);
    }

    @Override public Object invokeSync(Endpoint endpoint, Object request, InvokeContext ctx, long timeoutMs) throws InterruptedException, RemotingException {
        if (!checkConnection(endpoint, true))
            throw new RemotingException("Server is dead " + endpoint);

        LocalRpcServer srv = LocalRpcServer.servers.get(endpoint);
        if (srv == null)
            throw new RemotingException("Server is dead " + endpoint);

        LocalConnection locConn = srv.conns.get(this);
        if (locConn == null)
            throw new RemotingException("Server is dead " + endpoint);

        CompletableFuture<Object> fut = new CompletableFuture();

        locConn.onBeforeRequestSend((Message) request, fut);

        try {
            return fut.whenComplete((res, err) -> locConn.onAfterResponseSend((Message) res, err)).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new RemotingException(e);
        } catch (TimeoutException e) {
            throw new InvokeTimeoutException(e);
        }
    }

    @Override public void invokeAsync(Endpoint endpoint, Object request, InvokeContext ctx, InvokeCallback callback, long timeoutMs) throws InterruptedException, RemotingException {
        if (!checkConnection(endpoint, true))
            throw new RemotingException("Server is dead " + endpoint);

        LocalRpcServer srv = LocalRpcServer.servers.get(endpoint);
        if (srv == null)
            throw new RemotingException("Server is dead " + endpoint);

        LocalConnection locConn = srv.conns.get(this);
        if (locConn == null)
            throw new RemotingException("Server is dead " + endpoint);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        locConn.onBeforeRequestSend((Message) request, fut);

        fut.whenComplete((res, err) -> {
            locConn.onAfterResponseSend((Message) res, err);
            RpcUtils.runInThread(() -> callback.complete(res, err)); // Avoid deadlocks if a closure has completed in the same thread.
        }).orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override public boolean init(RpcOptions opts) {
        return true;
    }

    @Override public void shutdown() {
        // Close all connection from this peer.
        for (LocalRpcServer value : LocalRpcServer.servers.values())
            LocalRpcServer.closeConnection(this, value.local);
    }


}
