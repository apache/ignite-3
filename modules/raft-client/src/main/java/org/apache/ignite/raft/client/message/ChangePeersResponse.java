package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.Peer;

public interface ChangePeersResponse {
    List<Peer> getOldPeersList();

    List<Peer> getNewPeersList();

    public interface Builder {
        Builder addOldPeers(Peer oldPeersId);

        Builder addNewPeers(Peer newPeersId);

        ChangePeersResponse build();
    }
}
