package org.apache.ignite.raft.client.message;

/**
 * Transfer a leadership to receiving peer.
 */
public interface TransferLeadershipRequest {
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

        TransferLeadershipRequest build();
    }
}
