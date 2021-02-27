package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientMessages;

class SnapshotRequestImpl implements RaftClientMessages.SnapshotRequest, RaftClientMessages.SnapshotRequest.Builder {
    private String groupId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public RaftClientMessages.SnapshotRequest build() {
        return this;
    }
}
