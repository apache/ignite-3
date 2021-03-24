package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.jetbrains.annotations.Nullable;

public class RaftErrorResponse {
    private RaftErrorCode errorCode;

    private @Nullable Peer newLeader;

    public RaftErrorCode getErrorCode() {
        return errorCode;
    }

    public Peer getNewLeader() {
        return newLeader;
    }
}
