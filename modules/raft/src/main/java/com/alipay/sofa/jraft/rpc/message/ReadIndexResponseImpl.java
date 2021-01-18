package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.RpcRequests;

class ReadIndexResponseImpl implements RpcRequests.ReadIndexResponse, RpcRequests.ReadIndexResponse.Builder {
    private long index;
    private boolean success;

    @Override public long getIndex() {
        return index;
    }

    @Override public boolean getSuccess() {
        return success;
    }

    @Override public RpcRequests.ErrorResponse getErrorResponse() {
        return null;
    }

    @Override public RpcRequests.ReadIndexResponse build() {
        return this;
    }

    @Override public Builder setSuccess(boolean success) {
        this.success = success;

        return this;
    }

    @Override public Builder setIndex(long lastCommittedIndex) {
        this.index = lastCommittedIndex;

        return this;
    }
}
