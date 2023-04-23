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
package org.apache.ignite.raft.jraft.rpc.impl.core;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.InvokeContext;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.GetFileRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.GetFileResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.RequestVoteRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.RequestVoteResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.TimeoutNowRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.TimeoutNowResponse;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.rpc.impl.AbstractClientService;

/**
 * Raft rpc service.
 */
public class DefaultRaftClientService extends AbstractClientService implements RaftClientService {
    /** Stripes map */
    private final ConcurrentMap<PeerId, Executor> appendEntriesExecutorMap = new ConcurrentHashMap<>();

    // cached node options
    private NodeOptions nodeOptions;

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        final boolean ret = super.init(rpcOptions);
        if (ret) {
            this.nodeOptions = (NodeOptions) rpcOptions;
        }
        return ret;
    }

    @Override
    public Future<Message> preVote(final PeerId peerId, final RequestVoteRequest request,
        final RpcResponseClosure<RequestVoteResponse> done) {

        if (connect(peerId)) {
            return invokeWithDone(peerId, request, done, this.nodeOptions.getElectionTimeoutMs());
        }

        return onConnectionFail(rpcExecutor, request, done, peerId);
    }

    @Override
    public Future<Message> requestVote(final PeerId peerId, final RequestVoteRequest request,
        final RpcResponseClosure<RequestVoteResponse> done) {

        if (connect(peerId)) {
            return invokeWithDone(peerId, request, done, this.nodeOptions.getElectionTimeoutMs());
        }

        return onConnectionFail(rpcExecutor, request, done, peerId);
    }

    @Override
    public Future<Message> appendEntries(final PeerId peerId, final AppendEntriesRequest request,
        final int timeoutMs, final RpcResponseClosure<AppendEntriesResponse> done) {

        // Assign an executor in round-robin fasion.
        final Executor executor = this.appendEntriesExecutorMap.computeIfAbsent(peerId,
            k -> nodeOptions.getStripedExecutor().next());

        if (connect(peerId)) { // Replicator should be started asynchronously by node joined event.
            return invokeWithDone(peerId, request, done, timeoutMs, executor);
        }

        return onConnectionFail(executor, request, done, peerId);
    }

    @Override
    public Future<Message> getFile(final PeerId peerId, final GetFileRequest request, final int timeoutMs,
        final RpcResponseClosure<GetFileResponse> done) {
        // open checksum
        final InvokeContext ctx = new InvokeContext();

        return invokeWithDone(peerId, request, ctx, done, timeoutMs);
    }

    @Override
    public Future<Message> installSnapshot(final PeerId peerId, final InstallSnapshotRequest request,
        final RpcResponseClosure<InstallSnapshotResponse> done) {

        // Check connection before installing the snapshot to avoid waiting for undelivered message.
        if (connect(peerId)) {
            return invokeWithDone(peerId, request, done, this.rpcOptions.getRpcInstallSnapshotTimeout());
        }

        return onConnectionFail(rpcExecutor, request, done, peerId);
    }

    @Override
    public Future<Message> timeoutNow(final PeerId peerId, final TimeoutNowRequest request, final int timeoutMs,
        final RpcResponseClosure<TimeoutNowResponse> done) {
        return invokeWithDone(peerId, request, done, timeoutMs);
    }

    @Override
    public Future<Message> readIndex(final PeerId peerId, final ReadIndexRequest request, final int timeoutMs,
        final RpcResponseClosure<ReadIndexResponse> done) {
        return invokeWithDone(peerId, request, done, timeoutMs);
    }

    /**
     * @param executor The executor to run done closure.
     * @param request The request.
     * @param done The closure.
     * @param peerId The Peer ID.
     * @return The future.
     */
    private Future<Message> onConnectionFail(Executor executor, Message request, RpcResponseClosure<?> done, PeerId peerId) {
        // fail-fast when no connection
        final CompletableFuture<Message> future = new CompletableFuture<>();

        executor.execute(() -> {
            final String fmt = "Check connection[%s] fail and try to create new one";
            if (done != null) {
                try {
                    done.run(new Status(RaftError.EINTERNAL, fmt, peerId));
                }
                catch (final Throwable t) {
                    LOG.error("Fail to run RpcResponseClosure, the request is {}.", t, request);
                }
            }

            future.completeExceptionally(new RemotingException(String.format(fmt, peerId)));
        });

        return future;
    }
}
