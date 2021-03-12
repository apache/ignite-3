package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.PeerId;

public interface TransferLeaderRequest {
    String getGroupId();

    PeerId getPeerId();

    public interface Builder {
        Builder setGroupId(String groupId);

        TransferLeaderRequest build();
    }
}
