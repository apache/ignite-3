package org.apache.ignite.raft.client.message;


import org.apache.ignite.raft.client.RaftClientCommonMessages;

public class CreateGetLeaderResponseImpl implements RaftClientCommonMessages.GetLeaderResponse, RaftClientCommonMessages.GetLeaderResponse.Builder {
    private String leaderId;

    @Override public String getLeaderId() {
        return leaderId;
    }

    @Override public RaftClientCommonMessages.GetLeaderResponse build() {
        return this;
    }

    @Override public Builder setLeaderId(String leaderId) {
        this.leaderId = leaderId;

        return this;
    }
}
