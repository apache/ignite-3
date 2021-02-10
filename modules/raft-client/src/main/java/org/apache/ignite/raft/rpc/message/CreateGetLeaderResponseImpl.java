package org.apache.ignite.raft.rpc.message;


import org.apache.ignite.raft.rpc.CliRequests;

public class CreateGetLeaderResponseImpl implements CliRequests.GetLeaderResponse, CliRequests.GetLeaderResponse.Builder {
    private String leaderId;

    @Override public String getLeaderId() {
        return leaderId;
    }

    @Override public CliRequests.GetLeaderResponse build() {
        return this;
    }

    @Override public Builder setLeaderId(String leaderId) {
        this.leaderId = leaderId;

        return this;
    }
}
