package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;

/**
 * Transfer a leadership to receiving peer.
 */
public interface TransferLeadershipRequest {
    /**
     * @return Group id.
     */
    String groupId();

    /**
     * @return New leader.
     */
    Peer newLeader();

    /** */
    public interface Builder {
        /**
         * @param groupId Group id.
         * @return The builder.
         */
        Builder groupId(String groupId);

        /**
         * @param newLeader New leader.
         * @return The builder.
         */
        Builder peer(Peer newLeader);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        TransferLeadershipRequest build();
    }
}
