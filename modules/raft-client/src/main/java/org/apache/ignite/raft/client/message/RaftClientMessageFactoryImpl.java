package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientMessages;

/**
 * Raft client message factory.
 */
public class RaftClientMessageFactoryImpl implements RaftClientMessageFactory {
    public static RaftClientMessageFactoryImpl INSTANCE = new RaftClientMessageFactoryImpl();

    @Override public RaftClientMessages.AddPeerRequest.Builder createAddPeerRequest() {
        return new AddPeerRequestImpl();
    }

    @Override public RaftClientMessages.AddPeerResponse.Builder createAddPeerResponse() {
        return new AddPeerResponseImpl();
    }

    @Override public RaftClientMessages.RemovePeerRequest.Builder createRemovePeerRequest() {
        return new RemovePeerRequestImpl();
    }

    @Override public RaftClientMessages.RemovePeerResponse.Builder createRemovePeerResponse() {
        return new RemovePeerResponseImpl();
    }

    @Override public RaftClientMessages.ChangePeersRequest.Builder createChangePeerRequest() {
        return new ChangePeerRequestImpl();
    }

    @Override public RaftClientMessages.ChangePeersResponse.Builder createChangePeerResponse() {
        return new ChangePeersResponseImpl();
    }

    @Override public RaftClientMessages.SnapshotRequest.Builder createSnapshotRequest() {
        return new SnapshotRequestImpl();
    }

    @Override public RaftClientMessages.ResetPeerRequest.Builder createResetPeerRequest() {
        return new ResetPeerRequestImpl();
    }

    @Override public RaftClientMessages.TransferLeaderRequest.Builder createTransferLeaderRequest() {
        return new TransferLeaderRequestImpl();
    }

    @Override public RaftClientMessages.GetLeaderRequest.Builder createGetLeaderRequest() {
        return new GetLeaderRequestImpl();
    }

    @Override public RaftClientMessages.GetLeaderResponse.Builder createGetLeaderResponse() {
        return new GetLeaderResponseImpl();
    }

    @Override public RaftClientMessages.GetPeersRequest.Builder createGetPeersRequest() {
        return new GetPeersRequestImpl();
    }

    @Override public RaftClientMessages.GetPeersResponse.Builder createGetPeersResponse() {
        return new GetPeersResponseImpl();
    }

    @Override public RaftClientMessages.AddLearnersRequest.Builder createAddLearnersRequest() {
        return new AddLearnersRequestImpl();
    }

    @Override public RaftClientMessages.RemoveLearnersRequest.Builder createRemoveLearnersRequest() {
        return new RemoveLearnersRequestImpl();
    }

    @Override public RaftClientMessages.ResetLearnersRequest.Builder createResetLearnersRequest() {
        return new ResetLearnersRequestImpl();
    }

    @Override public RaftClientMessages.LearnersOpResponse.Builder createLearnersOpResponse() {
        return new LearnersOpResponseImpl();
    }

    @Override public RaftClientMessages.UserRequest.Builder createUserRequest() {
        return new UserRequestImpl();
    }

    @Override public RaftClientMessages.UserResponse.Builder createUserResponse() {
        return new UserResponseImpl();
    }
}
