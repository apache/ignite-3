package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientMessages;

public class UserRequestImpl<T> implements RaftClientMessages.UserRequest<T>, RaftClientMessages.UserRequest.Builder<T> {
    private T request;
    private String groupId;

    @Override public T request() {
        return request;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setRequest(T request) {
        this.request = request;

        return this;
    }

    @Override public RaftClientMessages.UserRequest<T> build() {
        return this;
    }

    @Override public String getGroupId() {
        return groupId;
    }
}
