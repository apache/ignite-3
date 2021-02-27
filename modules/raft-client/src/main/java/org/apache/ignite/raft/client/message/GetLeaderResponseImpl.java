package org.apache.ignite.raft.client.message;


import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientMessages;

public class GetLeaderResponseImpl implements RaftClientMessages.GetLeaderResponse, RaftClientMessages.GetLeaderResponse.Builder {
    private PeerId leaderId;

    @Override public PeerId getLeaderId() {
        return leaderId;
    }

    @Override public RaftClientMessages.GetLeaderResponse build() {
        return this;
    }

    @Override public Builder setLeaderId(PeerId leaderId) {
        this.leaderId = leaderId;

        return this;
    }
}
