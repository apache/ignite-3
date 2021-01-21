package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.RpcRequests;

public class InstallSnapshotResponseImpl implements RpcRequests.InstallSnapshotResponse, RpcRequests.InstallSnapshotResponse.Builder {
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

    @Override public RpcRequests.InstallSnapshotResponse build() {
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
