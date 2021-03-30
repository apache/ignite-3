package org.apache.ignite.raft.jraft.rpc.message;

import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;

public class CreateGetLeaderResponseImpl implements CliRequests.GetLeaderResponse, CliRequests.GetLeaderResponse.Builder {
    private String leaderId;

    @Override public String getLeaderId() {
        return leaderId;
    }

    @Override public RpcRequests.ErrorResponse getErrorResponse() {
        return null;
    }

    @Override public CliRequests.GetLeaderResponse build() {
        return this;
    }

    @Override public Builder setLeaderId(String leaderId) {
        this.leaderId = leaderId;

        return this;
    }
}
