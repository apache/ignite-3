package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

public interface ChangePeersRequest {
    List<PeerId> getNewPeersList();

    public interface Builder {
        Builder setGroupId(String groupId);

        Builder addNewPeers(PeerId peerId);

        ChangePeersRequest build();
    }
}
