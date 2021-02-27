package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientMessages;

class PingRequestImpl implements RaftClientMessages.PingRequest, RaftClientMessages.PingRequest.Builder {
    private long sendTimestamp;

    @Override public long getSendTimestamp() {
        return sendTimestamp;
    }

    @Override public Builder setSendTimestamp(long timestamp) {
        this.sendTimestamp = timestamp;

        return this;
    }

    @Override public RaftClientMessages.PingRequest build() {
        return this;
    }
}
