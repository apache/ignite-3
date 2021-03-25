package org.apache.ignite.raft.client.message;

/**
 * The result of an action.
 */
public interface ActionResponse {
    /**
     * @return A result for this request, can be of any type.
     */
    Object result();

    public interface Builder {
        /**
         * @param result A result for this request.
         * @return The builder.
         */
        Builder result(Object result);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        ActionResponse build();
    }
}
