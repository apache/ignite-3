package org.apache.ignite.raft.rpc;

import org.apache.ignite.raft.rpc.message.DefaultMessageBuilderFactory;

/** */
public interface MessageBuilderFactory {
    // TODO asch must be injected.
    public static MessageBuilderFactory DEFAULT = new DefaultMessageBuilderFactory();

    // Ping.
    RpcRequests.PingRequest.Builder createPingRequest();

    // Error.
    RpcRequests.ErrorResponse.Builder createErrorResponse();

    // CLI
    CliRequests.AddPeerRequest.Builder createAddPeerRequest();

    CliRequests.AddPeerResponse.Builder createAddPeerResponse();

    CliRequests.RemovePeerRequest.Builder createRemovePeerRequest();

    CliRequests.RemovePeerResponse.Builder createRemovePeerResponse();

    CliRequests.ChangePeersRequest.Builder createChangePeerRequest();

    CliRequests.ChangePeersResponse.Builder createChangePeerResponse();

    CliRequests.SnapshotRequest.Builder createSnapshotRequest();

    CliRequests.ResetPeerRequest.Builder createResetPeerRequest();

    CliRequests.TransferLeaderRequest.Builder createTransferLeaderRequest();

    CliRequests.GetLeaderRequest.Builder createGetLeaderRequest();

    CliRequests.GetLeaderResponse.Builder createGetLeaderResponse();

    CliRequests.GetPeersRequest.Builder createGetPeersRequest();

    CliRequests.GetPeersResponse.Builder createGetPeersResponse();

    CliRequests.AddLearnersRequest.Builder createAddLearnersRequest();

    CliRequests.RemoveLearnersRequest.Builder createRemoveLearnersRequest();

    CliRequests.ResetLearnersRequest.Builder createResetLearnersRequest();

    CliRequests.LearnersOpResponse.Builder createLearnersOpResponse();
}
