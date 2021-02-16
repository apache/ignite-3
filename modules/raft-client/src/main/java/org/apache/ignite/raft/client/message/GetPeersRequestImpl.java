package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientCommonMessages;

class GetPeersRequestImpl implements RaftClientCommonMessages.GetPeersRequest, RaftClientCommonMessages.GetPeersRequest.Builder {
    private String groupId;
    private boolean onlyAlive;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public boolean getOnlyAlive() {
        return onlyAlive;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setOnlyAlive(boolean onlyGetAlive) {
        this.onlyAlive = onlyGetAlive;

        return this;
    }

    @Override public RaftClientCommonMessages.GetPeersRequest build() {
        return this;
    }
}
