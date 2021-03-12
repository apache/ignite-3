package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

public interface LearnersOpResponse {
    List<PeerId> getOldLearnersList();

    List<PeerId> getNewLearnersList();

    public interface Builder {
        Builder addOldLearners(PeerId oldLearnersId);

        Builder addNewLearners(PeerId newLearnersId);

        LearnersOpResponse build();
    }
}
