package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.RpcRequests;

public class RequestVoteResponseImpl implements RpcRequests.RequestVoteResponse, RpcRequests.RequestVoteResponse.Builder {
    private long term;
    private boolean granted;

    @Override public long getTerm() {
        return term;
    }

    @Override public boolean getGranted() {
        return granted;
    }

    @Override public RpcRequests.ErrorResponse getErrorResponse() {
        return null;
    }

    @Override public RpcRequests.RequestVoteResponse build() {
        return this;
    }

    @Override public Builder setTerm(long currTerm) {
        this.term = currTerm;

        return this;
    }

    @Override public Builder setGranted(boolean granted) {
        this.granted = granted;

        return this;
    }
}
