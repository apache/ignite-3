package org.apache.ignite.raft.client.message;

public interface GetPeersRequest {
    boolean getOnlyAlive();

    public interface Builder {
        Builder setGroupId(String groupId);

        Builder setOnlyAlive(boolean onlyGetAlive);

        GetPeersRequest build();
    }
}
