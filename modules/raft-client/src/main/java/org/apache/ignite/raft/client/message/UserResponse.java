package org.apache.ignite.raft.client.message;

/**
 * The result of user request.
 * @param <T> Result type.
 */
public interface UserResponse<T> {
    /**
     * @return A result for this request.
     */
    T result();

    public interface Builder<T> {
        /**
         * @param result A result for this request.
         * @return The builder.
         */
        Builder result(T result);

        UserResponse<T> build();
    }
}
