package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

public interface RemovePeersRequest {
    String getGroupId();

    List<PeerId> getPeersList();

    interface Builder {
        Builder setGroupId(String groupId);

        Builder addPeer(PeerId peerId);

        RemovePeersRequest build();
    }
}
