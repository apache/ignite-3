package org.apache.ignite.raft.client.message;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.raft.client.Peer;

/**
 * Remove learners.
 */
public interface RemoveLearnersRequest {
    /**
     * @return Group id.
     */
    String groupId();

    /**
     * @return Learners to remove.
     */
    List<Peer> learners();

    public interface Builder {
        /**
         * @param groupId Group id.
         * @return The builder.
         */
        Builder groupId(String groupId);

        /**
         * @param learners Learners to remove.
         * @return The builder.
         */
        Builder learners(List<Peer> learners);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        RemoveLearnersRequest build();
    }
}
