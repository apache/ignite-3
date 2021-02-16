package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientCommonMessages;

/** */
public interface ClientMessageBuilderFactory {
    public static ClientMessageBuilderFactory DEFAULT = new RaftClientCommonMessageBuilderFactory();

    RaftClientCommonMessages.PingRequest.Builder createPingRequest();

    RaftClientCommonMessages.StatusResponse.Builder createStatusResponse();

    RaftClientCommonMessages.AddPeerRequest.Builder createAddPeerRequest();

    RaftClientCommonMessages.AddPeerResponse.Builder createAddPeerResponse();

    RaftClientCommonMessages.RemovePeerRequest.Builder createRemovePeerRequest();

    RaftClientCommonMessages.RemovePeerResponse.Builder createRemovePeerResponse();

    RaftClientCommonMessages.ChangePeersRequest.Builder createChangePeerRequest();

    RaftClientCommonMessages.ChangePeersResponse.Builder createChangePeerResponse();

    RaftClientCommonMessages.SnapshotRequest.Builder createSnapshotRequest();

    RaftClientCommonMessages.ResetPeerRequest.Builder createResetPeerRequest();

    RaftClientCommonMessages.TransferLeaderRequest.Builder createTransferLeaderRequest();

    RaftClientCommonMessages.GetLeaderRequest.Builder createGetLeaderRequest();

    RaftClientCommonMessages.GetLeaderResponse.Builder createGetLeaderResponse();

    RaftClientCommonMessages.GetPeersRequest.Builder createGetPeersRequest();

    RaftClientCommonMessages.GetPeersResponse.Builder createGetPeersResponse();

    RaftClientCommonMessages.AddLearnersRequest.Builder createAddLearnersRequest();

    RaftClientCommonMessages.RemoveLearnersRequest.Builder createRemoveLearnersRequest();

    RaftClientCommonMessages.ResetLearnersRequest.Builder createResetLearnersRequest();

    RaftClientCommonMessages.LearnersOpResponse.Builder createLearnersOpResponse();
}
