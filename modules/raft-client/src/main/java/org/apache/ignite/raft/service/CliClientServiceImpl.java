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
package org.apache.ignite.raft.service;

import java.util.concurrent.Future;
import org.apache.ignite.raft.Endpoint;
import org.apache.ignite.raft.closure.RpcResponseClosure;
import org.apache.ignite.raft.rpc.CliRequests;
import org.apache.ignite.raft.rpc.CliRequests.AddLearnersRequest;
import org.apache.ignite.raft.rpc.CliRequests.AddPeerRequest;
import org.apache.ignite.raft.rpc.CliRequests.AddPeerResponse;
import org.apache.ignite.raft.rpc.CliRequests.ChangePeersRequest;
import org.apache.ignite.raft.rpc.CliRequests.ChangePeersResponse;
import org.apache.ignite.raft.rpc.CliRequests.GetLeaderRequest;
import org.apache.ignite.raft.rpc.CliRequests.GetLeaderResponse;
import org.apache.ignite.raft.rpc.CliRequests.GetPeersRequest;
import org.apache.ignite.raft.rpc.CliRequests.GetPeersResponse;
import org.apache.ignite.raft.rpc.CliRequests.LearnersOpResponse;
import org.apache.ignite.raft.rpc.CliRequests.RemoveLearnersRequest;
import org.apache.ignite.raft.rpc.CliRequests.RemovePeerRequest;
import org.apache.ignite.raft.rpc.CliRequests.RemovePeerResponse;
import org.apache.ignite.raft.rpc.CliRequests.ResetLearnersRequest;
import org.apache.ignite.raft.rpc.CliRequests.ResetPeerRequest;
import org.apache.ignite.raft.rpc.CliRequests.TransferLeaderRequest;
import org.apache.ignite.raft.rpc.Message;
import org.apache.ignite.raft.rpc.RpcRequests.ErrorResponse;

/**
 *
 */
public class CliClientServiceImpl extends AbstractClientService implements CliClientService {
    @Override
    public Future<Message> addPeer(final Endpoint endpoint, final AddPeerRequest request,
                                   final RpcResponseClosure<AddPeerResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> removePeer(final Endpoint endpoint, final RemovePeerRequest request,
                                      final RpcResponseClosure<RemovePeerResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> resetPeer(final Endpoint endpoint, final ResetPeerRequest request,
                                     final RpcResponseClosure<ErrorResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> snapshot(final Endpoint endpoint, final CliRequests.SnapshotRequest request,
                                    final RpcResponseClosure<ErrorResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> changePeers(final Endpoint endpoint, final ChangePeersRequest request,
                                       final RpcResponseClosure<ChangePeersResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> addLearners(final Endpoint endpoint, final AddLearnersRequest request,
                                       final RpcResponseClosure<LearnersOpResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> removeLearners(final Endpoint endpoint, final RemoveLearnersRequest request,
                                          final RpcResponseClosure<LearnersOpResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> resetLearners(final Endpoint endpoint, final ResetLearnersRequest request,
                                         final RpcResponseClosure<LearnersOpResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> getLeader(final Endpoint endpoint, final GetLeaderRequest request,
                                     final RpcResponseClosure<GetLeaderResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> transferLeader(final Endpoint endpoint, final TransferLeaderRequest request,
                                          final RpcResponseClosure<ErrorResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }

    @Override
    public Future<Message> getPeers(final Endpoint endpoint, final GetPeersRequest request,
                                    final RpcResponseClosure<GetPeersResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcDefaultTimeout());
    }
}
