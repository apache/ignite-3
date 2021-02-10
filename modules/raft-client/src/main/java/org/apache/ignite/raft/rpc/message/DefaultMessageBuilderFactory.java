package org.apache.ignite.raft.rpc.message;

import org.apache.ignite.raft.rpc.CliRequests;
import org.apache.ignite.raft.rpc.MessageBuilderFactory;
import org.apache.ignite.raft.rpc.RpcRequests;

/**
 * Default message builders factory.
 */
public class DefaultMessageBuilderFactory implements MessageBuilderFactory {
    @Override public RpcRequests.PingRequest.Builder createPingRequest() {
        return new PingRequestImpl();
    }

    @Override public RpcRequests.ErrorResponse.Builder createErrorResponse() {
        return new ErrorResponseImpl();
    }

    @Override public CliRequests.AddPeerRequest.Builder createAddPeerRequest() {
        return new AddPeerRequestImpl();
    }

    @Override public CliRequests.AddPeerResponse.Builder createAddPeerResponse() {
        return new AddPeerResponseImpl();
    }

    @Override public CliRequests.RemovePeerRequest.Builder createRemovePeerRequest() {
        return new RemovePeerRequestImpl();
    }

    @Override public CliRequests.RemovePeerResponse.Builder createRemovePeerResponse() {
        return new RemovePeerResponseImpl();
    }

    @Override public CliRequests.ChangePeersRequest.Builder createChangePeerRequest() {
        return new ChangePeerRequestImpl();
    }

    @Override public CliRequests.ChangePeersResponse.Builder createChangePeerResponse() {
        return new ChangePeersResponseImpl();
    }

    @Override public CliRequests.SnapshotRequest.Builder createSnapshotRequest() {
        return new SnapshotRequestImpl();
    }

    @Override public CliRequests.ResetPeerRequest.Builder createResetPeerRequest() {
        return new ResetPeerRequestImpl();
    }

    @Override public CliRequests.TransferLeaderRequest.Builder createTransferLeaderRequest() {
        return new TransferLeaderRequestImpl();
    }

    @Override public CliRequests.GetLeaderRequest.Builder createGetLeaderRequest() {
        return new CreateGetLeaderRequestImpl();
    }

    @Override public CliRequests.GetLeaderResponse.Builder createGetLeaderResponse() {
        return new CreateGetLeaderResponseImpl();
    }

    @Override public CliRequests.GetPeersRequest.Builder createGetPeersRequest() {
        return new GetPeersRequestImpl();
    }

    @Override public CliRequests.GetPeersResponse.Builder createGetPeersResponse() {
        return new GetPeersResponseImpl();
    }

    @Override public CliRequests.AddLearnersRequest.Builder createAddLearnersRequest() {
        return new AddLearnersRequestImpl();
    }

    @Override public CliRequests.RemoveLearnersRequest.Builder createRemoveLearnersRequest() {
        return new RemoveLearnersRequestImpl();
    }

    @Override public CliRequests.ResetLearnersRequest.Builder createResetLearnersRequest() {
        return new ResetLearnersRequestImpl();
    }

    @Override public CliRequests.LearnersOpResponse.Builder createLearnersOpResponse() {
        return new LearnersOpResponseImpl();
    }
}
