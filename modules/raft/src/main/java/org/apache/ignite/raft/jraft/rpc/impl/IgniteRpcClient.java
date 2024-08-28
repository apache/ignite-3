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

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.PeerUnavailableException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.InvokeTimeoutException;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.InvokeCallback;
import org.apache.ignite.raft.jraft.rpc.InvokeContext;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcClientEx;
import org.apache.ignite.raft.jraft.util.Utils;

public class IgniteRpcClient implements RpcClientEx {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteRpcClient.class);

    private volatile BiPredicate<Object, String> recordPred;

    private BiPredicate<Object, String> blockPred;

    private LinkedBlockingQueue<Object[]> blockedMsgs = new LinkedBlockingQueue<>();

    private LinkedBlockingQueue<Object[]> recordedMsgs = new LinkedBlockingQueue<>();

    private final ClusterService service;

    /**
     * @param service The service.
     */
    public IgniteRpcClient(ClusterService service) {
        this.service = service;
    }

    public ClusterService clusterService() {
        return service;
    }

    /** {@inheritDoc} */
    @Override public boolean checkConnection(PeerId peerId) {
        return service.topologyService().getByConsistentId(peerId.getConsistentId()) != null;
    }

    /** {@inheritDoc} */
    @Override public void registerConnectEventListener(TopologyEventHandler handler) {
        service.topologyService().addEventHandler(handler);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Message> invokeAsync(
        PeerId peerId,
        Object request,
        InvokeContext ctx,
        InvokeCallback callback,
        long timeoutMs
    ) {
        CompletableFuture<Message> fut = new CompletableFuture<>();

        fut.whenComplete((res, err) -> {
            assert !(res == null && err == null) : res + " " + err;

            if (err == null && recordPred != null && recordPred.test(res, this.toString()))
                recordedMsgs.add(new Object[] {res, this.toString(), fut.hashCode(), System.currentTimeMillis(), null});

            if (err instanceof ExecutionException)
                err = new RemotingException(err);
            else if (err instanceof TimeoutException) // Translate timeout exception.
                err = new InvokeTimeoutException();

            Throwable finalErr = err;

            // Avoid deadlocks if a closure has completed in the same thread.
            Utils.runInThread(callback.executor(), () -> callback.complete(res, finalErr));
        });

        // Future hashcode used as corellation id.
        if (recordPred != null && recordPred.test(request, peerId.toString()))
            recordedMsgs.add(new Object[] {request, peerId.toString(), fut.hashCode(), System.currentTimeMillis(), null});

        synchronized (this) {
            if (blockPred != null && blockPred.test(request, peerId.toString())) {
                Object[] msgData = {
                        request,
                        peerId.toString(),
                        fut.hashCode(),
                        System.currentTimeMillis(),
                        (Runnable) () -> send(peerId, request, fut, timeoutMs)
                };

                blockedMsgs.add(msgData);

                if (timeoutMs > 0) {
                    fut.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
                }

                LOG.info("Blocked message to={} id={} msg={}", peerId.toString(), msgData[2], S.toString(request));

                return fut;
            }
        }

        send(peerId, request, fut, timeoutMs);

        return fut;
    }

    public void send(PeerId peerId, Object request, CompletableFuture<Message> fut, long timeout) {
        ClusterNode targetNode = service.topologyService().getByConsistentId(peerId.getConsistentId());

        if (targetNode == null) {
            // PeerUnavailableException will force a retry by the enclosing components.
            fut.completeExceptionally(new PeerUnavailableException(peerId.getConsistentId()));

            return;
        }

        service.messagingService()
            .invoke(targetNode, (NetworkMessage) request, timeout)
            .whenComplete((resp, err) -> {
                if (err != null)
                    fut.completeExceptionally(err);
                else
                    fut.complete((Message) resp);
            });
    }

    /** {@inheritDoc} */
    @Override public boolean init(RpcOptions opts) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
    }

    /** {@inheritDoc} */
    @Override public void blockMessages(BiPredicate<Object, String> predicate) {
        this.blockPred = predicate;
    }

    /** {@inheritDoc} */
    @Override public void stopBlock() {
        ArrayList<Object[]> msgs = new ArrayList<>();

        synchronized (this) {
            blockedMsgs.drainTo(msgs);

            blockPred = null;
        }

        for (Object[] msg : msgs) {
            Runnable r = (Runnable) msg[4];

            LOG.info("Unblocked message to={} id={} msg={}", msg[1], msg[2], S.toString(msg[0]));

            r.run();
        }
    }

    /** {@inheritDoc} */
    @Override public void stopBlock(int cnt) {
        ArrayList<Object[]> msgs = new ArrayList<>();

        synchronized (this) {
            while(cnt-- > 0) {
                Object[] tmp = blockedMsgs.poll();

                if (tmp == null)
                    break;

                msgs.add(tmp);
            }

            blockPred = null;
        }

        for (Object[] msg : msgs) {
            Runnable r = (Runnable) msg[4];

            r.run();
        }
    }

    /** {@inheritDoc} */
    @Override public void recordMessages(BiPredicate<Object, String> predicate) {
        this.recordPred = predicate;
    }

    /** {@inheritDoc} */
    @Override public void stopRecord() {
        this.recordPred = null;
    }

    /** {@inheritDoc} */
    @Override public Queue<Object[]> recordedMessages() {
        return recordedMsgs;
    }

    /** {@inheritDoc} */
    @Override public Queue<Object[]> blockedMessages() {
        return blockedMsgs;
    }
}
