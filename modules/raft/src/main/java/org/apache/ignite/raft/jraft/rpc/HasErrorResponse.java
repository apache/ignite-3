package org.apache.ignite.raft.jraft.rpc;

public interface HasErrorResponse extends Message {
    RpcRequests.ErrorResponse getErrorResponse(); // TODO asch can be removed.
}
