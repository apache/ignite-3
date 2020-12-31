package com.alipay.sofa.jraft.rpc;

public interface HasErrorResponse extends Message {
    RpcRequests.ErrorResponse getErrorResponse(); // TODO asch can be removed.
}
