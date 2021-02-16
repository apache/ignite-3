package org.apache.ignite.raft.client.message;


import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientCommonMessages;

public class CreateGetLeaderRequestImpl implements RaftClientCommonMessages.GetLeaderRequest, RaftClientCommonMessages.GetLeaderRequest.Builder {
    private String groupId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public RaftClientCommonMessages.GetLeaderRequest build() {
        return this;
    }
}
