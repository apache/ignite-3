package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.CliRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests;

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
