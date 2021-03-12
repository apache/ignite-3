package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.PeerId;

public interface GetLeaderResponse {
    PeerId getLeaderId();

    public interface Builder {
        GetLeaderResponse build();

        Builder setLeaderId(PeerId leaderId);
    }
}
