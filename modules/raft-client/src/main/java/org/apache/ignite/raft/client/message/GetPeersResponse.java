package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.Peer;

public interface GetPeersResponse {
    List<Peer> getPeersList();

    List<Peer> getLearnersList();

    public interface Builder {
        Builder addPeers(Peer peer);

        Builder addLearners(Peer learnerId);

        GetPeersResponse build();
    }
}
