package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.Peer;

/**
 * Add learners.
 */
public interface AddLearnersRequest {
    /**
     * @return Group id.
     */
    String groupId();

    /**
     * @return List of learners.
     */
    List<Peer> learners();

    /** */
    public interface Builder {
        /**
         * @param groupId Group id.
         * @return The builder.
         */
        Builder groupId(String groupId);

        /**
         * @param learners Learners.
         * @return The builder.
         */
        Builder learners(List<Peer> learner);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        AddLearnersRequest build();
    }
}
