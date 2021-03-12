package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.PeerId;

public interface RemovePeerRequest {
    PeerId getPeerId();

    interface Builder {
        Builder setGroupId(String groupId);

        Builder setPeerId(PeerId peerId);

        RemovePeerRequest build();
    }
}
