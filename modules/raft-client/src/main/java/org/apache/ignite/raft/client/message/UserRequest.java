package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Command;

/**
 * Submit user request to a replication group.
 */
public interface UserRequest {
    /**
     * @return Group id.
     */
    String groupId();

    /**
     * @return State machine command.
     */
    Command command();

    /** */
    public interface Builder {
        /**
         * @param cmd State machine command.
         * @return The builder.
         */
        Builder command(Command cmd);

        /**
         * @param groupId Group id.
         * @return The builder.
         */
        Builder groupId(String groupId);

        UserRequest build();
    }
}
