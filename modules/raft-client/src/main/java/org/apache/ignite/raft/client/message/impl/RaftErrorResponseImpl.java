package org.apache.ignite.raft.client.message.impl;

import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.message.RaftErrorResponse;

public class RaftErrorResponseImpl implements RaftErrorResponse, RaftErrorResponse.Builder {
    /** */
    private RaftErrorCode errorCode;

    /** */
    private Peer newLeader;

    /** {@inheritDoc} */
    @Override public RaftErrorCode errorCode() {
        return errorCode;
    }

    /** {@inheritDoc} */
    @Override public Peer newLeader() {
        return newLeader;
    }

    /** {@inheritDoc} */
    @Override public Builder errorCode(RaftErrorCode errorCode) {
        this.errorCode = errorCode;

        return this;
    }

    /** {@inheritDoc} */
    @Override public Builder newLeader(Peer newLeader) {
        this.newLeader = newLeader;

        return this;
    }

    /** {@inheritDoc} */
    @Override public RaftErrorResponse build() {
        return this;
    }
}
