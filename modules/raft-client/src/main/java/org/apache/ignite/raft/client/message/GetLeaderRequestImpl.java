package org.apache.ignite.raft.client.message;


import org.apache.ignite.raft.client.RaftClientMessages;

public class GetLeaderRequestImpl implements RaftClientMessages.GetLeaderRequest, RaftClientMessages.GetLeaderRequest.Builder {
    private String groupId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public RaftClientMessages.GetLeaderRequest build() {
        return this;
    }
}
