package org.apache.ignite.raft.rpc.message;

import org.apache.ignite.raft.rpc.RpcRequests;

class ErrorResponseImpl implements RpcRequests.ErrorResponse, RpcRequests.ErrorResponse.Builder {
    @Override public int getErrorCode() {
        return 0;
    }

    @Override public String getErrorMsg() {
        return null;
    }

    @Override public Builder setErrorCode(int code) {
        return null;
    }

    @Override public Builder setErrorMsg(String msg) {
        return null;
    }

    @Override public RpcRequests.ErrorResponse build() {
        return null;
    }
}
