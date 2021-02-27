package org.apache.ignite.raft.client.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientMessages;

class RemoveLearnersRequestImpl implements RaftClientMessages.RemoveLearnersRequest, RaftClientMessages.RemoveLearnersRequest.Builder {
    private String groupId;
    private List<PeerId> learnersList = new ArrayList<>();

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public List<PeerId> getLearnersList() {
        return learnersList;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder addLearners(PeerId learnerId) {
        learnersList.add(learnerId);

        return this;
    }

    @Override public RaftClientMessages.RemoveLearnersRequest build() {
        return this;
    }
}
