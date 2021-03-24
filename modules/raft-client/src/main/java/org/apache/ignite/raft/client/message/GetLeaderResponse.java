package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;
import org.jetbrains.annotations.Nullable;

public interface GetLeaderResponse {
    Peer leaderId();

    public interface Builder {
        GetLeaderResponse build();

        Builder setLeaderId(Peer leaderId);
    }
}
