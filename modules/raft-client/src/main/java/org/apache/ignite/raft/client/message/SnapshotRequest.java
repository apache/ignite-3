package org.apache.ignite.raft.client.message;

public interface SnapshotRequest {
    String getGroupId();

    public interface Builder {
        Builder setGroupId(String groupId);

        SnapshotRequest build();
    }
}
