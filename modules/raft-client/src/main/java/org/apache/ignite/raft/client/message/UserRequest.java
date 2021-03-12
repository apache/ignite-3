package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Command;

public interface UserRequest {
    Command request();

    String getGroupId();

    public interface Builder {
        Builder setRequest(Command request);

        Builder setGroupId(String groupId);

        UserRequest build();
    }
}
