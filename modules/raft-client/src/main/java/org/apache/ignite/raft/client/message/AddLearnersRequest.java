package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.Peer;

public interface AddLearnersRequest {
    String getGroupId();

    List<Peer> getLearnersList();

    public interface Builder {
        Builder setGroupId(String groupId);

        Builder addLearner(Peer learner);

        AddLearnersRequest build();
    }
}
