package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;

public interface RaftErrorResponse {
    public RaftErrorCode getErrorCode();

    public Peer getNewLeader();

    public interface Builder {
        Builder setErrorCode(RaftErrorCode errorCode);

        Builder setNewLeader(Peer newLeader);

        RaftErrorResponse build();
    }
}
