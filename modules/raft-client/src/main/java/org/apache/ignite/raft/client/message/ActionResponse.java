package org.apache.ignite.raft.client.message;

/**
 * The result of an action.
 */
public interface ActionResponse<T> {
    /**
     * @return A result for this request, can be of any type.
     */
    T result();

    /** */
    public interface Builder<T> {
        /**
         * @param result A result for this request.
         * @return The builder.
         */
        Builder result(T result);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        ActionResponse build();
    }
}
