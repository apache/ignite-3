package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

public interface AddLearnersRequest {
    String getGroupId();

    List<PeerId> getLearnersList();

    public interface Builder {
        Builder setGroupId(String groupId);

        Builder addLearner(PeerId learnerId);

        AddLearnersRequest build();
    }
}
