package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.RpcRequests;

public class AppendEntriesResponseImpl implements RpcRequests.AppendEntriesResponse, RpcRequests.AppendEntriesResponse.Builder {
    private long term;
    private boolean success;
    private long lastLogIndex;

    @Override public long getTerm() {
        return term;
    }

    @Override public boolean getSuccess() {
        return success;
    }

    @Override public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override public RpcRequests.ErrorResponse getErrorResponse() {
        return null;
    }

    @Override public RpcRequests.AppendEntriesResponse build() {
        return this;
    }

    @Override public Builder setSuccess(boolean success) {
        this.success = success;

        return this;
    }

    @Override public Builder setTerm(long currTerm) {
        this.term = currTerm;

        return this;
    }

    @Override public Builder setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;

        return this;
    }
}
