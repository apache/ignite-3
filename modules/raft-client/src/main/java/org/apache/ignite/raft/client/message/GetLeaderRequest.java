package org.apache.ignite.raft.client.message;

/**
 * Get leader.
 */
public interface GetLeaderRequest {
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

        GetLeaderRequest build();
    }
}
