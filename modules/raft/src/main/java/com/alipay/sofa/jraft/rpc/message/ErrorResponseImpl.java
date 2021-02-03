package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.RpcRequests;

public class ErrorResponseImpl implements RpcRequests.ErrorResponse, RpcRequests.ErrorResponse.Builder {
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
