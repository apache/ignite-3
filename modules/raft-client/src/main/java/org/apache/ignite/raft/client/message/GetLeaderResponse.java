package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;

/**
 * A current leader.
 */
public interface GetLeaderResponse {
    /**
     * @return The leader.
     */
    Peer leader();

    public interface Builder {
        /**
         * @param leader Leader
         * @return The builder.
         */
        Builder leader(Peer leaderId);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        GetLeaderResponse build();
    }
}
