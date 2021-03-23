package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;

public interface GetLeaderResponse {
    Peer getLeaderId();

    public interface Builder {
        GetLeaderResponse build();

        Builder setLeaderId(Peer leaderId);
    }
}
