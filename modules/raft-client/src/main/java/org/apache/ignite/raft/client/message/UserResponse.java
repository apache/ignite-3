package org.apache.ignite.raft.client.message;

public interface UserResponse<T> {
    T response();

    public interface Builder<T> {
        Builder setResponse(T response);

        UserResponse<T> build();
    }
}
