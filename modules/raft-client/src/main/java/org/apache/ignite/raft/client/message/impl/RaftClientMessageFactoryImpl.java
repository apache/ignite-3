/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.client.message.impl;

import org.apache.ignite.raft.client.message.AddLearnersRequest;
import org.apache.ignite.raft.client.message.AddPeersRequest;
import org.apache.ignite.raft.client.message.ChangePeersResponse;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.GetPeersRequest;
import org.apache.ignite.raft.client.message.GetPeersResponse;
import org.apache.ignite.raft.client.message.RaftErrorResponse;
import org.apache.ignite.raft.client.message.RemoveLearnersRequest;
import org.apache.ignite.raft.client.message.RemovePeersRequest;
import org.apache.ignite.raft.client.message.SnapshotRequest;
import org.apache.ignite.raft.client.message.TransferLeaderRequest;
import org.apache.ignite.raft.client.message.UserRequest;
import org.apache.ignite.raft.client.message.UserResponse;

/**
 * Raft client message factory.
 */
public class RaftClientMessageFactoryImpl implements RaftClientMessageFactory {
    @Override public AddPeersRequest.Builder createAddPeersRequest() {
        return new AddPeersRequestImpl();
    }

    @Override public ChangePeersResponse.Builder createChangePeersResponse() {
        return new ChangePeersResponseImpl();
    }

    @Override public RemovePeersRequest.Builder createRemovePeerRequest() {
        return new RemovePeersRequestImpl();
    }

    @Override public SnapshotRequest.Builder createSnapshotRequest() {
        return new SnapshotRequestImpl();
    }

    @Override public TransferLeaderRequest.Builder createTransferLeaderRequest() {
        return new TransferLeaderRequestImpl();
    }

    @Override public GetLeaderRequest.Builder createGetLeaderRequest() {
        return new GetLeaderRequestImpl();
    }

    @Override public GetLeaderResponse.Builder createGetLeaderResponse() {
        return new GetLeaderResponseImpl();
    }

    @Override public GetPeersRequest.Builder createGetPeersRequest() {
        return new GetPeersRequestImpl();
    }

    @Override public GetPeersResponse.Builder createGetPeersResponse() {
        return new GetPeersResponseImpl();
    }

    @Override public AddLearnersRequest.Builder createAddLearnersRequest() {
        return new AddLearnersRequestImpl();
    }

    @Override public RemoveLearnersRequest.Builder createRemoveLearnersRequest() {
        return new RemoveLearnersRequestImpl();
    }

    @Override public UserRequest.Builder createUserRequest() {
        return new UserRequestImpl();
    }

    @Override public UserResponse.Builder createUserResponse() {
        return new UserResponseImpl();
    }

    @Override public RaftErrorResponse.Builder createRaftErrorResponse() {
        return new RaftErrorResponseImpl();
    }
}
