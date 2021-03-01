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

import org.apache.ignite.raft.client.message.RaftClientMessages;

/** */
public interface RaftClientMessageFactory {
    RaftClientMessages.AddPeerRequest.Builder createAddPeerRequest();

    RaftClientMessages.AddPeerResponse.Builder createAddPeerResponse();

    RaftClientMessages.RemovePeerRequest.Builder createRemovePeerRequest();

    RaftClientMessages.RemovePeerResponse.Builder createRemovePeerResponse();

    RaftClientMessages.ChangePeersRequest.Builder createChangePeerRequest();

    RaftClientMessages.ChangePeersResponse.Builder createChangePeerResponse();

    RaftClientMessages.SnapshotRequest.Builder createSnapshotRequest();

    RaftClientMessages.ResetPeerRequest.Builder createResetPeerRequest();

    RaftClientMessages.TransferLeaderRequest.Builder createTransferLeaderRequest();

    RaftClientMessages.GetLeaderRequest.Builder createGetLeaderRequest();

    RaftClientMessages.GetLeaderResponse.Builder createGetLeaderResponse();

    RaftClientMessages.GetPeersRequest.Builder createGetPeersRequest();

    RaftClientMessages.GetPeersResponse.Builder createGetPeersResponse();

    RaftClientMessages.AddLearnersRequest.Builder createAddLearnersRequest();

    RaftClientMessages.RemoveLearnersRequest.Builder createRemoveLearnersRequest();

    RaftClientMessages.ResetLearnersRequest.Builder createResetLearnersRequest();

    RaftClientMessages.LearnersOpResponse.Builder createLearnersOpResponse();

    RaftClientMessages.UserRequest.Builder createUserRequest();

    RaftClientMessages.UserResponse.Builder createUserResponse();
}
