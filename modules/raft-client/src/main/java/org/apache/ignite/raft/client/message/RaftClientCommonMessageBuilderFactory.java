package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientCommonMessages;

/**
 * Raft client message builders factory.
 */
public class RaftClientCommonMessageBuilderFactory implements ClientMessageBuilderFactory {
    public static RaftClientCommonMessageBuilderFactory DEFAULT = new RaftClientCommonMessageBuilderFactory();

    @Override public RaftClientCommonMessages.PingRequest.Builder createPingRequest() {
        return new PingRequestImpl();
    }

    @Override public RaftClientCommonMessages.StatusResponse.Builder createStatusResponse() {
        return new StatusResponseImpl();
    }

    @Override public RaftClientCommonMessages.AddPeerRequest.Builder createAddPeerRequest() {
        return new AddPeerRequestImpl();
    }

    @Override public RaftClientCommonMessages.AddPeerResponse.Builder createAddPeerResponse() {
        return new AddPeerResponseImpl();
    }

    @Override public RaftClientCommonMessages.RemovePeerRequest.Builder createRemovePeerRequest() {
        return new RemovePeerRequestImpl();
    }

    @Override public RaftClientCommonMessages.RemovePeerResponse.Builder createRemovePeerResponse() {
        return new RemovePeerResponseImpl();
    }

    @Override public RaftClientCommonMessages.ChangePeersRequest.Builder createChangePeerRequest() {
        return new ChangePeerRequestImpl();
    }

    @Override public RaftClientCommonMessages.ChangePeersResponse.Builder createChangePeerResponse() {
        return new ChangePeersResponseImpl();
    }

    @Override public RaftClientCommonMessages.SnapshotRequest.Builder createSnapshotRequest() {
        return new SnapshotRequestImpl();
    }

    @Override public RaftClientCommonMessages.ResetPeerRequest.Builder createResetPeerRequest() {
        return new ResetPeerRequestImpl();
    }

    @Override public RaftClientCommonMessages.TransferLeaderRequest.Builder createTransferLeaderRequest() {
        return new TransferLeaderRequestImpl();
    }

    @Override public RaftClientCommonMessages.GetLeaderRequest.Builder createGetLeaderRequest() {
        return new GetLeaderRequestImpl();
    }

    @Override public RaftClientCommonMessages.GetLeaderResponse.Builder createGetLeaderResponse() {
        return new GetLeaderResponseImpl();
    }

    @Override public RaftClientCommonMessages.GetPeersRequest.Builder createGetPeersRequest() {
        return new GetPeersRequestImpl();
    }

    @Override public RaftClientCommonMessages.GetPeersResponse.Builder createGetPeersResponse() {
        return new GetPeersResponseImpl();
    }

    @Override public RaftClientCommonMessages.AddLearnersRequest.Builder createAddLearnersRequest() {
        return new AddLearnersRequestImpl();
    }

    @Override public RaftClientCommonMessages.RemoveLearnersRequest.Builder createRemoveLearnersRequest() {
        return new RemoveLearnersRequestImpl();
    }

    @Override public RaftClientCommonMessages.ResetLearnersRequest.Builder createResetLearnersRequest() {
        return new ResetLearnersRequestImpl();
    }

    @Override public RaftClientCommonMessages.LearnersOpResponse.Builder createLearnersOpResponse() {
        return new LearnersOpResponseImpl();
    }
}
