package org.apache.ignite.raft.client.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientCommonMessages;

class GetPeersResponseImpl implements RaftClientCommonMessages.GetPeersResponse, RaftClientCommonMessages.GetPeersResponse.Builder {
    private List<PeerId> peersList = new ArrayList<>();
    private List<PeerId> learnersList = new ArrayList<>();

    @Override public List<PeerId> getPeersList() {
        return peersList;
    }

    @Override public List<PeerId> getLearnersList() {
        return learnersList;
    }

    @Override public Builder addPeers(PeerId peerId) {
        peersList.add(peerId);

        return this;
    }

    @Override public Builder addLearners(PeerId learnerId) {
        learnersList.add(learnerId);

        return this;
    }

    @Override public RaftClientCommonMessages.GetPeersResponse build() {
        return this;
    }
}
