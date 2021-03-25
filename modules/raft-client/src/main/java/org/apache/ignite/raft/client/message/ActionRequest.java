package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Command;

/**
 * Submit an action to a replication group.
 */
public interface ActionRequest {
    /**
     * @return Group id.
     */
    String groupId();

    /**
     * @return Action's command.
     */
    Command command();

    /** */
    public interface Builder {
        /**
         * @param cmd Action's command.
         * @return The builder.
         */
        Builder command(Command cmd);

        /**
         * @param groupId Group id.
         * @return The builder.
         */
        Builder groupId(String groupId);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        ActionRequest build();
    }
}
