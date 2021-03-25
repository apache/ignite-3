package org.apache.ignite.raft.client.message;

/**
 * Take a local snapshot on the peer.
 */
public interface SnapshotRequest {
    /**
     * @return Group id.
     */
    String groupId();

    public interface Builder {
        /**
         * @param groupId Group id.
         * @return The builder.
         */
        Builder groupId(String groupId);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        SnapshotRequest build();
    }
}
