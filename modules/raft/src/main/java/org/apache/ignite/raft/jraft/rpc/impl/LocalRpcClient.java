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
package org.apache.ignite.raft.jraft.rpc.impl;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;
import org.apache.ignite.raft.jraft.ReplicatorGroup;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.InvokeTimeoutException;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.InvokeCallback;
import org.apache.ignite.raft.jraft.rpc.InvokeContext;
import org.apache.ignite.raft.jraft.rpc.RpcClientEx;
import org.apache.ignite.raft.jraft.rpc.RpcUtils;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local rpc client impl.
 *
 * @author ascherbakov.
 */
public class LocalRpcClient implements RpcClientEx {
    private static final Logger LOG                    = LoggerFactory.getLogger(LocalRpcClient.class);

    public volatile ReplicatorGroup replicatorGroup = null;

    private volatile BiPredicate<Object, String> recordPred;
    private BiPredicate<Object, String> blockPred;

    private LinkedBlockingQueue<Object[]> blockedMsgs = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Object[]> recordedMsgs = new LinkedBlockingQueue<>();

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

        if (onConnCreated[0] != null)
            onConnCreated[0].accept(conn);
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

        // Future hashcode used as corellation id.
        if (recordPred != null && recordPred.test(request, endpoint.toString()))
            recordedMsgs.add(new Object[]{request, endpoint.toString(), fut.hashCode(), System.currentTimeMillis(), null});

        boolean wasBlocked;

        synchronized (this) {
            wasBlocked = blockPred != null && blockPred.test(request, endpoint.toString());

            if (wasBlocked)
                blockedMsgs.add(new Object[]{request, endpoint.toString(), fut.hashCode(), System.currentTimeMillis(), (Runnable) () -> locConn.send(request, fut)});
        }

        if (!wasBlocked)
            locConn.send(request, fut);

        try {
            return fut.whenComplete((res, err) -> {
                if (err == null && recordPred != null && recordPred.test(res, this.toString()))
                    recordedMsgs.add(new Object[]{res, this.toString(), fut.hashCode(), System.currentTimeMillis(), null});
            }).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new RemotingException(e);
        } catch (TimeoutException e) {
            throw new InvokeTimeoutException();
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

        fut.orTimeout(timeoutMs, TimeUnit.MILLISECONDS).
            whenComplete((res, err) -> {
                if (err == null && recordPred != null && recordPred.test(res, this.toString()))
                    recordedMsgs.add(new Object[]{res, this.toString(), fut.hashCode(), System.currentTimeMillis(), null});

                if (err instanceof ExecutionException)
                    err = new RemotingException(err);
                else if (err instanceof TimeoutException) // Translate timeout exception.
                    err = new InvokeTimeoutException();

                Throwable finalErr = err;

                RpcUtils.runInThread(() -> callback.complete(res, finalErr)); // Avoid deadlocks if a closure has completed in the same thread.
            });

        // Future hashcode used as corellation id.
        if (recordPred != null && recordPred.test(request, endpoint.toString()))
            recordedMsgs.add(new Object[]{request, endpoint.toString(), fut.hashCode(), System.currentTimeMillis(), null});

        synchronized (this) {
            if (blockPred != null && blockPred.test(request, endpoint.toString())) {
                blockedMsgs.add(new Object[]{request, endpoint.toString(), fut.hashCode(), System.currentTimeMillis(), new Runnable() {
                    @Override public void run() {
                        locConn.send(request, fut);
                    }
                }});

                return;
            }
        }

        locConn.send(request, fut);
    }

    @Override public boolean init(RpcOptions opts) {
        return true;
    }

    @Override public void shutdown() {
        // Close all connection from this peer.
        for (LocalRpcServer value : LocalRpcServer.servers.values())
            LocalRpcServer.closeConnection(this, value.local);
    }

    @Override public void blockMessages(BiPredicate<Object, String> predicate) {
        this.blockPred = predicate;
    }

    @Override public void stopBlock() {
        ArrayList<Object[]> msgs = new ArrayList<>();

        synchronized (this) {
            blockedMsgs.drainTo(msgs);


            blockPred = null;
        }

        for (Object[] msg : msgs) {
            Runnable r = (Runnable) msg[4];

            r.run();
        }
    }

    @Override public void recordMessages(BiPredicate<Object, String> predicate) {
        this.recordPred = predicate;
    }

    @Override public void stopRecord() {
        this.recordPred = null;
    }

    @Override public Queue<Object[]> recordedMessages() {
        return recordedMsgs;
    }

    @Override public Queue<Object[]> blockedMessages() {
        return blockedMsgs;
    }
}
