package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

public interface ChangePeersResponse {
    List<PeerId> getOldPeersList();

    List<PeerId> getNewPeersList();

    public interface Builder {
        Builder addOldPeers(PeerId oldPeersId);

        Builder addNewPeers(PeerId newPeersId);

        ChangePeersResponse build();
    }
}
