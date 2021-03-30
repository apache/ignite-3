package org.apache.ignite.raft.jraft.rpc.message;

import org.apache.ignite.raft.jraft.rpc.RpcRequests;

class TimeoutNowResponseImpl implements RpcRequests.TimeoutNowResponse, RpcRequests.TimeoutNowResponse.Builder {
    private long term;
    private boolean success;

    @Override public long getTerm() {
        return term;
    }

    @Override public boolean getSuccess() {
        return success;
    }

    @Override public RpcRequests.ErrorResponse getErrorResponse() {
        return null;
    }

    @Override public RpcRequests.TimeoutNowResponse build() {
        return this;
    }

    @Override public Builder setTerm(long currTerm) {
        this.term = currTerm;

        return this;
    }

    @Override public Builder setSuccess(boolean success) {
        this.success = success;

        return this;
    }
}
