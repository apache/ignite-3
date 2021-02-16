package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientCommonMessages;

class PingRequestImpl implements RaftClientCommonMessages.PingRequest, RaftClientCommonMessages.PingRequest.Builder {
    private long sendTimestamp;

    @Override public long getSendTimestamp() {
        return sendTimestamp;
    }

    @Override public Builder setSendTimestamp(long timestamp) {
        this.sendTimestamp = timestamp;

        return this;
    }

    @Override public RaftClientCommonMessages.PingRequest build() {
        return this;
    }
}
