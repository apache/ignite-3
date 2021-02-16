package org.apache.ignite.raft.client.message;


import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientCommonMessages;

public class GetLeaderResponseImpl implements RaftClientCommonMessages.GetLeaderResponse, RaftClientCommonMessages.GetLeaderResponse.Builder {
    private PeerId leaderId;

    @Override public PeerId getLeaderId() {
        return leaderId;
    }

    @Override public RaftClientCommonMessages.GetLeaderResponse build() {
        return this;
    }

    @Override public Builder setLeaderId(PeerId leaderId) {
        this.leaderId = leaderId;

        return this;
    }
}
