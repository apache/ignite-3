package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientMessages;

public class UserRequestImpl<T> implements RaftClientMessages.UserRequest, RaftClientMessages.UserRequest.Builder {
    private Object request;
    private String groupId;

    @Override public Object request() {
        return request;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setRequest(Object request) {
        this.request = request;

        return this;
    }

    @Override public RaftClientMessages.UserRequest build() {
        return this;
    }

    @Override public String getGroupId() {
        return groupId;
    }
}
