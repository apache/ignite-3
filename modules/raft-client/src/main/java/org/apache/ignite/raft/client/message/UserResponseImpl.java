package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientMessages;

public class UserResponseImpl<T> implements RaftClientMessages.UserResponse<T>, RaftClientMessages.UserResponse.Builder<T> {
    private T response;

    @Override public T response() {
        return response;
    }

    @Override public Builder setResponse(T response) {
        this.response = response;

        return this;
    }

    @Override public RaftClientMessages.UserResponse build() {
        return this;
    }
}
