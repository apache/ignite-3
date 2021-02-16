package org.apache.ignite.raft.client.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientCommonMessages;

class RemovePeerResponseImpl implements RaftClientCommonMessages.RemovePeerResponse, RaftClientCommonMessages.RemovePeerResponse.Builder {
    private List<PeerId> oldPeersList = new ArrayList<>();
    private List<PeerId> newPeersList = new ArrayList<>();

    @Override public List<PeerId> getOldPeersList() {
        return oldPeersList;
    }

    @Override public List<PeerId> getNewPeersList() {
        return newPeersList;
    }

    @Override public Builder addOldPeers(PeerId oldPeerId) {
        oldPeersList.add(oldPeerId);

        return this;
    }

    @Override public Builder addNewPeers(PeerId newPeerId) {
        newPeersList.add(newPeerId);

        return this;
    }

    @Override public RaftClientCommonMessages.RemovePeerResponse build() {
        return this;
    }
}
