package org.apache.ignite.raft.client.message.impl;

import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.message.RaftErrorResponse;

public class RaftErrorResponseImpl implements RaftErrorResponse, RaftErrorResponse.Builder {
    /** */
    private RaftErrorCode errorCode;

    /** */
    private Peer newLeader;

    @Override public RaftErrorCode getErrorCode() {
        return errorCode;
    }

    @Override public Peer getNewLeader() {
        return newLeader;
    }

    @Override public Builder setErrorCode(RaftErrorCode errorCode) {
        this.errorCode = errorCode;

        return this;
    }

    @Override public Builder setNewLeader(Peer newLeader) {
        this.newLeader = newLeader;

        return this;
    }

    @Override public RaftErrorResponse build() {
        return this;
    }
}
