package org.apache.ignite.raft.jraft.rpc.message;

import org.apache.ignite.raft.jraft.rpc.CliRequests;

public class GetPeersRequestImpl implements CliRequests.GetPeersRequest, CliRequests.GetPeersRequest.Builder {
    private String groupId;
    private String leaderId;
    private boolean onlyAlive;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getLeaderId() {
        return leaderId;
    }

    @Override public boolean getOnlyAlive() {
        return onlyAlive;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setLeaderId(String leaderId) {
        this.leaderId = leaderId;

        return this;
    }

    @Override public Builder setOnlyAlive(boolean onlyGetAlive) {
        this.onlyAlive = onlyGetAlive;

        return this;
    }

    @Override public CliRequests.GetPeersRequest build() {
        return this;
    }
}
