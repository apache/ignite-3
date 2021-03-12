package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

public interface GetPeersResponse {
    List<PeerId> getPeersList();

    List<PeerId> getLearnersList();

    public interface Builder {
        Builder addPeers(PeerId peerId);

        Builder addLearners(PeerId learnerId);

        GetPeersResponse build();
    }
}
