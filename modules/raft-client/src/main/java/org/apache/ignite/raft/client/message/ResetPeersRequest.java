package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

public interface ResetPeersRequest {
    String getGroupId();

    List<PeerId> getNewPeersList();

    public interface Builder {
        Builder setGroupId(String groupId);

        Builder addPeer(PeerId peerId);

        ResetPeersRequest build();
    }
}
