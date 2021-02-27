package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientMessages;

/** */
public interface ClientMessageBuilderFactory {
    RaftClientMessages.PingRequest.Builder createPingRequest();

    RaftClientMessages.StatusResponse.Builder createStatusResponse();

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
