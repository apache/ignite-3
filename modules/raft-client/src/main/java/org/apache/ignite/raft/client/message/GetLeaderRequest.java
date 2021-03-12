package org.apache.ignite.raft.client.message;

public interface GetLeaderRequest {
    String getGroupId();

    public interface Builder {
        Builder setGroupId(String groupId);

        GetLeaderRequest build();
    }
}
