package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

public interface RemovePeerResponse {
    List<PeerId> getOldPeersList();

    List<PeerId> getNewPeersList();

    public interface Builder {
        Builder addOldPeers(PeerId oldPeerId);

        Builder addNewPeers(PeerId newPeerId);

        RemovePeerResponse build();
    }
}
