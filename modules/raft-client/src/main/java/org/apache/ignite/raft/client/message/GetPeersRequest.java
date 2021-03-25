package org.apache.ignite.raft.client.message;

/** Get peers. */
public interface GetPeersRequest {
    /**
     * @return Group id.
     */
    String groupId();

    /**
     * @return {@code True} to list only alive nodes.
     */
    boolean onlyAlive();

    /** */
    public interface Builder {
        /**
         * @param groupId Group id.
         * @return The builder.
         */
        Builder groupId(String groupId);

        /**
         * @param onlyGetAlive {@code True} to list only alive nodes.
         * @return The builder.
         */
        Builder onlyAlive(boolean onlyGetAlive);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        GetPeersRequest build();
    }
}
