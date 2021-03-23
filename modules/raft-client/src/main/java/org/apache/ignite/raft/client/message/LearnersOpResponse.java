package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.Peer;

public interface LearnersOpResponse {
    List<Peer> getOldLearnersList();

    List<Peer> getNewLearnersList();

    public interface Builder {
        Builder addOldLearners(Peer oldLearnersId);

        Builder addNewLearners(Peer newLearnersId);

        LearnersOpResponse build();
    }
}
