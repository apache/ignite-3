package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

public interface AddLearnersRequest {
    List<PeerId> getLearnersList();

    public interface Builder {
        Builder setGroupId(String groupId);

        Builder addLearners(PeerId learnerId);

        AddLearnersRequest build();
    }
}
