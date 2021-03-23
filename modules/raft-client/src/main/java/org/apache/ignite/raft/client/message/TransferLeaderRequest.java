package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;

public interface TransferLeaderRequest {
    String getGroupId();

    Peer getPeer();

    public interface Builder {
        Builder setGroupId(String groupId);

        TransferLeaderRequest build();
    }
}
