package org.apache.ignite.raft.rpc.message;

import org.apache.ignite.raft.rpc.RpcRequests;

class PingRequestImpl implements RpcRequests.PingRequest, RpcRequests.PingRequest.Builder {
    private long sendTimestamp;

    @Override public long getSendTimestamp() {
        return sendTimestamp;
    }

    @Override public Builder setSendTimestamp(long timestamp) {
        this.sendTimestamp = timestamp;

        return this;
    }

    @Override public RpcRequests.PingRequest build() {
        return this;
    }
}
