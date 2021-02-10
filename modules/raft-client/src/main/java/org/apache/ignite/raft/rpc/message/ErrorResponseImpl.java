package org.apache.ignite.raft.rpc.message;

import org.apache.ignite.raft.rpc.RpcRequests;

class ErrorResponseImpl implements RpcRequests.ErrorResponse, RpcRequests.ErrorResponse.Builder {
    private int errorCode;
    private String errorMsg = "";

    @Override public int getErrorCode() {
        return errorCode;
    }

    @Override public Builder setErrorCode(int errorCode) {
        this.errorCode = errorCode;

        return this;
    }

    @Override public String getErrorMsg() {
        return errorMsg;
    }

    @Override public Builder setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;

        return this;
    }

    @Override public RpcRequests.ErrorResponse build() {
        return this;
    }
}
