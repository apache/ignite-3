package org.apache.ignite.raft.client.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientMessages;

class LearnersOpResponseImpl implements RaftClientMessages.LearnersOpResponse, RaftClientMessages.LearnersOpResponse.Builder {
    private List<PeerId> oldLearnersList = new ArrayList<>();
    private List<PeerId> newLearnersList = new ArrayList<>();

    @Override public List<PeerId> getOldLearnersList() {
        return oldLearnersList;
    }

    @Override public List<PeerId> getNewLearnersList() {
        return newLearnersList;
    }

    @Override public Builder addOldLearners(PeerId oldLearnersId) {
        oldLearnersList.add(oldLearnersId);

        return this;
    }

    @Override public Builder addNewLearners(PeerId newLearnersId) {
        newLearnersList.add(newLearnersId);

        return this;
    }

    @Override public RaftClientMessages.LearnersOpResponse build() {
        return this;
    }
}
